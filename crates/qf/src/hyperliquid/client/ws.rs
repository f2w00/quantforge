use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<anyhow::Result<Value>>>>>;

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
    ) -> anyhow::Result<(Self, mpsc::Receiver<Value>)> {
        let base_url = base_url.into();
        let (stream, _) = connect_async(&base_url)
            .await
            .context("connect Hyperliquid websocket")?;
        let (mut sink, mut source) = stream.split();
        let (writer, mut outbound) = mpsc::channel::<Message>(128);
        let (events, event_rx) = mpsc::channel(128);
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let pending_reader = Arc::clone(&pending);

        tokio::spawn(async move {
            while let Some(message) = outbound.recv().await {
                if sink.send(message).await.is_err() {
                    break;
                }
            }
        });

        tokio::spawn(async move {
            while let Some(message) = source.next().await {
                let value = match message {
                    Ok(Message::Text(text)) => serde_json::from_str::<Value>(&text),
                    Ok(Message::Binary(bytes)) => serde_json::from_slice(&bytes),
                    Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => continue,
                    Ok(Message::Close(_)) | Err(_) => break,
                    _ => continue,
                };
                let Ok(value) = value else { continue };
                if let Some(id) = value.pointer("/data/id").and_then(Value::as_u64) {
                    if let Some(sender) = pending_reader
                        .lock()
                        .ok()
                        .and_then(|mut map| map.remove(&id))
                    {
                        let _ = sender.send(Ok(value));
                        continue;
                    }
                }
                let _ = events.send(value).await;
            }

            if let Ok(mut map) = pending_reader.lock() {
                for (_, sender) in map.drain() {
                    let _ = sender.send(Err(anyhow::anyhow!("websocket connection closed")));
                }
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
        receiver.await.context("receive websocket post response")?
    }
}
