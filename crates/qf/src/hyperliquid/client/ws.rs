use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const POST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<anyhow::Result<Value>>>>>;

#[derive(Clone, Debug)]
pub enum HyperliquidWsEvent {
    Connected,
    Disconnected,
    Message(Value),
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
