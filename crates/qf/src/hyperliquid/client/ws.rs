use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Context;
use chrono::{TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::core::{Decimal, OrderId, Side, Timestamp};
use crate::hyperliquid::client::rest::{parse_ws_clearinghouse_state, parse_ws_open_orders};
use crate::hyperliquid::types::{
    HlAccountState, HlCancelResponse, HlCancelStatus, HlOpenOrder, HlOrderOutcome,
};

const POST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<anyhow::Result<Value>>>>>;

#[derive(Clone, Debug)]
pub enum HyperliquidWsEvent {
    Connected,
    Disconnected,
    Message(Value),
}

#[derive(Clone, Debug)]
pub struct HlOrderUpdate {
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub status: Option<String>,
    pub raw: Value,
}

#[derive(Clone, Debug)]
pub struct HlUserFill {
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub coin: Option<String>,
    pub size: Option<Decimal>,
    pub price: Option<Decimal>,
    pub fee: Option<Decimal>,
    pub side: Option<Side>,
    pub timestamp: Option<Timestamp>,
    pub trade_id: Option<String>,
    pub raw: Value,
}

/// 解析账户仓位 WebSocket 快照，供只读账户监控策略复用。
pub fn ws_clearinghouse_state(message: &Value) -> anyhow::Result<HlAccountState> {
    parse_ws_clearinghouse_state(message)
}

pub(crate) fn ws_open_orders(message: &Value) -> anyhow::Result<Vec<HlOpenOrder>> {
    parse_ws_open_orders(message)
}

pub(crate) fn order_updates(message: &Value) -> Vec<HlOrderUpdate> {
    event_values(message)
        .into_iter()
        .map(|value| parse_order_update(message, value))
        .collect()
}

pub(crate) fn user_fills(message: &Value) -> Vec<HlUserFill> {
    nested_event_values(message, "fills")
        .into_iter()
        .map(|value| parse_user_fill(message, value))
        .collect()
}

pub(crate) fn funding_events(message: &Value) -> Vec<Value> {
    nested_event_values(message, "fundings")
        .into_iter()
        .cloned()
        .collect()
}

pub(crate) fn non_funding_ledger_events(message: &Value) -> Vec<Value> {
    let values = nested_event_values(message, "nonFundingLedgerUpdates");
    if values.is_empty() {
        event_values(message).into_iter().cloned().collect()
    } else {
        values.into_iter().cloned().collect()
    }
}

pub(crate) fn parse_order_outcome(raw: &Value) -> Result<HlOrderOutcome, String> {
    let payload = raw
        .pointer("/data/response/payload")
        .ok_or("missing websocket order response payload")?;
    if payload.get("type").and_then(Value::as_str) == Some("error") {
        return Err(payload
            .get("payload")
            .and_then(Value::as_str)
            .unwrap_or("websocket action error")
            .to_string());
    }
    if payload.get("status").and_then(Value::as_str) == Some("err") {
        return Err(payload
            .get("response")
            .and_then(Value::as_str)
            .unwrap_or("websocket action error")
            .to_string());
    }
    let data = payload
        .pointer("/response/data")
        .or_else(|| payload.get("data"))
        .or_else(|| {
            payload
                .get("response")
                .filter(|response| response.get("statuses").is_some())
        })
        .filter(|data| data.get("statuses").is_some())
        .ok_or("missing websocket order response data")?;
    let status = data
        .get("statuses")
        .and_then(Value::as_array)
        .and_then(|statuses| statuses.first())
        .ok_or("missing websocket order status")?;
    if let Some(resting) = status.get("resting") {
        return Ok(HlOrderOutcome::Resting {
            order_id: OrderId::new(
                resting
                    .get("oid")
                    .and_then(Value::as_u64)
                    .ok_or("resting response is missing oid")?
                    .to_string(),
            ),
        });
    }
    if let Some(filled) = status.get("filled") {
        return Ok(HlOrderOutcome::Filled {
            order_id: OrderId::new(
                filled
                    .get("oid")
                    .and_then(Value::as_u64)
                    .ok_or("filled response is missing oid")?
                    .to_string(),
            ),
            total_size: filled
                .get("totalSz")
                .and_then(Value::as_str)
                .ok_or("filled response is missing totalSz")?
                .parse()
                .map_err(|_| "invalid filled totalSz")?,
            avg_price: filled
                .get("avgPx")
                .and_then(Value::as_str)
                .ok_or("filled response is missing avgPx")?
                .parse()
                .map_err(|_| "invalid filled avgPx")?,
        });
    }
    status
        .get("error")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| "unsupported websocket order status".to_string())
        .and_then(Err)
}

pub(crate) fn parse_default_action_response(raw: &Value) -> Result<(), String> {
    let payload = raw.pointer("/data/response/payload").unwrap_or(raw);
    if payload.get("status").and_then(Value::as_str) == Some("ok")
        && payload.pointer("/response/type").and_then(Value::as_str) == Some("default")
    {
        return Ok(());
    }
    Err(payload
        .get("payload")
        .or_else(|| payload.get("response"))
        .and_then(Value::as_str)
        .unwrap_or("unexpected update leverage response")
        .to_string())
}

pub(crate) fn parse_cancel_response(raw: Value) -> Result<HlCancelResponse, String> {
    let statuses = raw
        .pointer("/data/response/payload/response/data/statuses")
        .and_then(Value::as_array)
        .ok_or("missing websocket cancel response statuses")?;
    let statuses = statuses
        .iter()
        .map(|status| {
            if status.as_str() == Some("success") {
                HlCancelStatus::Success
            } else {
                HlCancelStatus::Error {
                    message: status
                        .get("error")
                        .and_then(Value::as_str)
                        .unwrap_or("unsupported websocket cancel status")
                        .to_string(),
                }
            }
        })
        .collect::<Vec<_>>();
    let success = statuses
        .iter()
        .all(|status| matches!(status, HlCancelStatus::Success));
    Ok(HlCancelResponse {
        success,
        statuses,
        raw,
    })
}

pub(crate) fn event_data(message: &Value) -> &Value {
    message.get("data").unwrap_or(message)
}

pub(crate) fn event_values(message: &Value) -> Vec<&Value> {
    match event_data(message) {
        Value::Array(values) => values.iter().collect(),
        value => vec![value],
    }
}

pub(crate) fn nested_event_values<'a>(message: &'a Value, field: &str) -> Vec<&'a Value> {
    event_data(message)
        .get(field)
        .and_then(Value::as_array)
        .map(|values| values.iter().collect())
        .unwrap_or_default()
}

pub(crate) fn string_field(value: &Value, names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        value.get(*name).and_then(|field| {
            field
                .as_str()
                .map(str::to_string)
                .or_else(|| field.as_u64().map(|value| value.to_string()))
        })
    })
}

pub(crate) fn decimal_field(value: &Value, names: &[&str]) -> Option<Decimal> {
    names.iter().find_map(|name| {
        value
            .get(*name)
            .and_then(|field| {
                field
                    .as_str()
                    .map(str::to_string)
                    .or_else(|| field.is_number().then(|| field.to_string()))
            })
            .and_then(|value| value.parse().ok())
    })
}

pub(crate) fn parse_order_update(message: &Value, value: &Value) -> HlOrderUpdate {
    let order = value.get("order").unwrap_or(value);
    HlOrderUpdate {
        order_id: string_field(order, &["oid", "orderId"]),
        client_order_id: string_field(order, &["cloid", "clientOrderId"]),
        status: string_field(value, &["status", "orderStatus"]),
        raw: message.clone(),
    }
}

pub(crate) fn parse_user_fill(message: &Value, value: &Value) -> HlUserFill {
    HlUserFill {
        order_id: string_field(value, &["oid", "orderId"]),
        client_order_id: string_field(value, &["cloid", "clientOrderId"]),
        coin: string_field(value, &["coin"]),
        size: decimal_field(value, &["sz", "size"]),
        price: decimal_field(value, &["px", "price"]),
        fee: decimal_field(value, &["fee"]),
        side: match value.get("side").and_then(Value::as_str) {
            Some("B") | Some("Buy") => Some(Side::Buy),
            Some("A") | Some("Sell") => Some(Side::Sell),
            _ => None,
        },
        timestamp: value
            .get("time")
            .and_then(|value| {
                value
                    .as_i64()
                    .or_else(|| value.as_u64().map(|value| value as i64))
            })
            .and_then(|value| Utc.timestamp_millis_opt(value).single()),
        trade_id: string_field(value, &["tid"]),
        raw: message.clone(),
    }
}

#[derive(Clone)]
pub struct HyperliquidWsClient {
    pub base_url: String,
    writer: mpsc::Sender<Message>,
    pending: Pending,
    next_id: Arc<AtomicU64>,
}

impl HyperliquidWsClient {
    pub async fn connect(
        base_url: impl Into<String>,
    ) -> anyhow::Result<(Self, mpsc::Receiver<HyperliquidWsEvent>)> {
        let base_url = base_url.into();
        let (writer, mut outbound) = mpsc::channel::<Message>(128);
        let (events, event_rx) = mpsc::channel(128);
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let pending_reader = Arc::clone(&pending);
        let reconnect_url = base_url.clone();

        tokio::spawn(async move {
            let mut subscriptions: Vec<Message> = Vec::new();
            loop {
                let stream = match connect_async(&reconnect_url).await {
                    Ok((stream, _)) => stream,
                    Err(_) => {
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        continue;
                    }
                };
                if events.send(HyperliquidWsEvent::Connected).await.is_err() {
                    return;
                }
                let (mut sink, mut source) = stream.split();
                for subscription in &subscriptions {
                    let _ = sink.send(subscription.clone()).await;
                }

                loop {
                    tokio::select! {
                        Some(message) = outbound.recv() => {
                            if let Message::Text(text) = &message
                                && let Ok(value) = serde_json::from_str::<Value>(text)
                                && value.get("method").and_then(Value::as_str) == Some("subscribe")
                                && !subscriptions.iter().any(|item| item == &message)
                            {
                                subscriptions.push(message.clone());
                            }
                            if sink.send(message).await.is_err() {
                                break;
                            }
                        }
                        incoming = source.next() => {
                            let Some(incoming) = incoming else { break };
                            let value = match incoming {
                                Ok(Message::Text(text)) => serde_json::from_str::<Value>(&text),
                                Ok(Message::Binary(bytes)) => serde_json::from_slice(&bytes),
                                Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => continue,
                                Ok(Message::Close(_)) | Err(_) => break,
                                _ => continue,
                            };
                            let Ok(value) = value else { continue };
                            if let Some(id) = value.pointer("/data/id").and_then(Value::as_u64)
                                && let Some(sender) = pending_reader.lock().ok().and_then(|mut map| map.remove(&id))
                            {
                                let _ = sender.send(Ok(value));
                                continue;
                            }
                            if events.send(HyperliquidWsEvent::Message(value)).await.is_err() {
                                return;
                            }
                        }
                        else => break,
                    }
                }
                if let Ok(mut map) = pending_reader.lock() {
                    for (_, sender) in map.drain() {
                        let _ = sender.send(Err(anyhow::anyhow!(
                            "websocket connection closed before response",
                        )));
                    }
                }
                if events.send(HyperliquidWsEvent::Disconnected).await.is_err() {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });

        Ok((
            Self {
                base_url,
                writer,
                pending,
                next_id: Arc::new(AtomicU64::new(1)),
            },
            event_rx,
        ))
    }

    pub async fn subscribe(&self, subscription: Value) -> anyhow::Result<()> {
        self.writer
            .send(Message::Text(
                serde_json::json!({
                    "method": "subscribe",
                    "subscription": subscription,
                })
                .to_string()
                .into(),
            ))
            .await
            .map_err(|_| anyhow::anyhow!("websocket writer is closed"))
    }

    pub async fn subscribe_all_mids(&self) -> anyhow::Result<()> {
        self.subscribe(serde_json::json!({ "type": "allMids" }))
            .await
    }

    pub async fn subscribe_account_channels(&self, user: &str) -> anyhow::Result<()> {
        for subscription_type in [
            "clearinghouseState",
            "openOrders",
            "orderUpdates",
            "userFills",
            "userFundings",
            "userNonFundingLedgerUpdates",
        ] {
            self.subscribe(serde_json::json!({
                "type": subscription_type,
                "user": user,
            }))
            .await?;
        }
        Ok(())
    }

    pub async fn post(&self, request: Value) -> anyhow::Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        self.pending
            .lock()
            .map_err(|_| anyhow::anyhow!("pending request lock is poisoned"))?
            .insert(id, sender);
        let message = serde_json::json!({
            "method": "post",
            "id": id,
            "request": request,
        });
        if self
            .writer
            .send(Message::Text(message.to_string().into()))
            .await
            .is_err()
        {
            self.pending.lock().ok().and_then(|mut map| map.remove(&id));
            return Err(anyhow::anyhow!("websocket writer is closed"));
        }
        match tokio::time::timeout(POST_TIMEOUT, receiver).await {
            Ok(result) => result.context("receive websocket post response")?,
            Err(_) => {
                self.pending.lock().ok().and_then(|mut map| map.remove(&id));
                Err(anyhow::anyhow!("websocket post response timeout"))
            }
        }
    }
}
