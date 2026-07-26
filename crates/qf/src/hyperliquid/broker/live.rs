use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use alloy::primitives::Address;
use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::sync::mpsc;

use crate::core::StrategyId;
use crate::hyperliquid::broker::HlBrokerError;
use crate::hyperliquid::broker::risk_adapter::order_risk_input_at_price;
use crate::hyperliquid::broker::state::HlBrokerState;
use crate::hyperliquid::broker::traits::HyperliquidBroker;
use crate::hyperliquid::client::rest::{parse_ws_clearinghouse_state, parse_ws_open_orders};
use crate::hyperliquid::client::{HyperliquidRestClient, HyperliquidSigner, HyperliquidWsClient};
use crate::hyperliquid::types::{
    HlAccountState, HlCancelRequest, HlCancelResponse, HlCancelStatus, HlClientOrderId,
    HlCloseRequest, HlCloseSize, HlCoin, HlExchangeAction, HlMetadataSnapshot, HlMidSnapshot,
    HlOpenOrder, HlOrderOutcome, HlOrderRequest, HlOrderResult, HlOrderType, HlSubmittedOrder,
};
use crate::risk::{RiskDecision, RiskGuard};

const ACCOUNT_EVENT_HISTORY_LIMIT: usize = 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HlNetwork {
    Mainnet,
    Testnet,
}

impl HlNetwork {
    fn rest_url(self) -> &'static str {
        match self {
            Self::Mainnet => "https://api.hyperliquid.xyz",
            Self::Testnet => "https://api.hyperliquid-testnet.xyz",
        }
    }

    fn ws_url(self) -> &'static str {
        match self {
            Self::Mainnet => "wss://api.hyperliquid.xyz/ws",
            Self::Testnet => "wss://api.hyperliquid-testnet.xyz/ws",
        }
    }
}

#[derive(Clone, Debug)]
pub struct HlLiveBrokerConfig {
    pub strategy_id: StrategyId,
    pub network: HlNetwork,
    pub account_address: Address,
    pub metadata_refresh_interval: std::time::Duration,
    pub connect_timeout: std::time::Duration,
    pub freshness_max_age: std::time::Duration,
    pub reconciliation_interval: std::time::Duration,
}

pub struct HyperliquidLiveBroker {
    strategy_id: StrategyId,
    state: RwLock<HlBrokerState>,
    risk_guard: RiskGuard,
    client: HyperliquidRestClient,
    metadata: RwLock<HlMetadataSnapshot>,
    mids: RwLock<HlMidSnapshot>,
    next_client_order_id: AtomicU64,
    signer: std::sync::Arc<HyperliquidSigner>,
    ws: HyperliquidWsClient,
    network: HlNetwork,
    order_updates: RwLock<Vec<HlOrderUpdate>>,
    fills: RwLock<Vec<HlUserFill>>,
    account_address: String,
    freshness_max_age: std::time::Duration,
    freshness: RwLock<HlFreshness>,
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
    pub size: Option<crate::core::Decimal>,
    pub price: Option<crate::core::Decimal>,
    pub fee: Option<crate::core::Decimal>,
    pub raw: Value,
}

#[derive(Clone, Debug, Default)]
struct HlFreshness {
    metadata: Option<DateTime<Utc>>,
    mids: Option<DateTime<Utc>>,
    account: Option<DateTime<Utc>>,
    open_orders: Option<DateTime<Utc>>,
}

impl HyperliquidLiveBroker {
    pub async fn connect(
        config: HlLiveBrokerConfig,
        signer: std::sync::Arc<HyperliquidSigner>,
        risk_guard: RiskGuard,
    ) -> Result<std::sync::Arc<Self>, HlBrokerError> {
        let client = HyperliquidRestClient::new(config.network.rest_url());
        let user = format!("{:#x}", config.account_address);
        let metadata = client.meta().await.map_err(transport_error)?;
        let mids = client.all_mids().await.map_err(transport_error)?;
        let account = client
            .clearinghouse_state(&user)
            .await
            .map_err(transport_error)?;
        let open_orders = client.open_orders(&user).await.map_err(transport_error)?;
        let (ws, events) = HyperliquidWsClient::connect(config.network.ws_url())
            .await
            .map_err(transport_error)?;
        let broker = std::sync::Arc::new(Self::from_parts(
            config.strategy_id,
            HlBrokerState {
                account,
                open_orders,
            },
            risk_guard,
            client,
            metadata,
            mids,
            signer,
            ws,
            config.network,
            user.clone(),
            config.freshness_max_age,
        ));

        broker
            .ws
            .subscribe_all_mids()
            .await
            .map_err(transport_error)?;
        broker
            .ws
            .subscribe_account_channels(&user)
            .await
            .map_err(transport_error)?;
        let events = broker
            .wait_for_subscriptions(events, config.connect_timeout)
            .await?;
        broker.spawn_event_consumer(events);
        std::sync::Arc::clone(&broker).spawn_background_tasks(
            config.metadata_refresh_interval,
            config.reconciliation_interval,
        );
        Ok(broker)
    }

    #[allow(clippy::too_many_arguments)]
    fn from_parts(
        strategy_id: StrategyId,
        state: HlBrokerState,
        risk_guard: RiskGuard,
        client: HyperliquidRestClient,
        metadata: HlMetadataSnapshot,
        mids: HlMidSnapshot,
        signer: std::sync::Arc<HyperliquidSigner>,
        ws: HyperliquidWsClient,
        network: HlNetwork,
        account_address: String,
        freshness_max_age: std::time::Duration,
    ) -> Self {
        let metadata_updated_at = metadata.updated_at;
        let mids_updated_at = mids.updated_at;
        Self {
            strategy_id,
            state: RwLock::new(state),
            risk_guard,
            client,
            metadata: RwLock::new(metadata),
            mids: RwLock::new(mids),
            next_client_order_id: AtomicU64::new(current_millis()),
            signer,
            ws,
            network,
            order_updates: RwLock::new(Vec::new()),
            fills: RwLock::new(Vec::new()),
            account_address,
            freshness_max_age,
            freshness: RwLock::new(HlFreshness {
                metadata: metadata_updated_at,
                mids: mids_updated_at,
                account: Some(Utc::now()),
                open_orders: Some(Utc::now()),
            }),
        }
    }

    async fn wait_for_subscriptions(
        &self,
        mut events: mpsc::Receiver<Value>,
        timeout: std::time::Duration,
    ) -> Result<mpsc::Receiver<Value>, HlBrokerError> {
        let expected = [
            "allMids",
            "clearinghouseState",
            "openOrders",
            "orderUpdates",
            "userFills",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<std::collections::HashSet<_>>();
        let mut acknowledged = std::collections::HashSet::new();
        tokio::time::timeout(timeout, async {
            while acknowledged != expected {
                let message = events
                    .recv()
                    .await
                    .ok_or_else(|| HlBrokerError::Transport {
                        message: "websocket closed before subscription confirmation".to_string(),
                    })?;
                match message.get("channel").and_then(Value::as_str) {
                    Some("subscriptionResponse") => {
                        let subscription_type = message
                            .pointer("/data/subscription/type")
                            .and_then(Value::as_str)
                            .ok_or_else(|| HlBrokerError::Transport {
                                message: "invalid websocket subscription response".to_string(),
                            })?;
                        if !expected.contains(subscription_type) {
                            return Err(HlBrokerError::Transport {
                                message: format!(
                                    "unexpected websocket subscription: {subscription_type}"
                                ),
                            });
                        }
                        acknowledged.insert(subscription_type.to_string());
                    }
                    Some("error") => {
                        return Err(HlBrokerError::Transport {
                            message: format!("websocket subscription error: {message}"),
                        });
                    }
                    _ => self.apply_ws_event(message)?,
                }
            }
            Ok::<(), HlBrokerError>(())
        })
        .await
        .map_err(|_| HlBrokerError::Transport {
            message: "timed out waiting for websocket subscriptions".to_string(),
        })??;
        Ok(events)
    }

    fn spawn_event_consumer(self: &std::sync::Arc<Self>, mut events: mpsc::Receiver<Value>) {
        let broker = std::sync::Arc::clone(self);
        tokio::spawn(async move {
            while let Some(message) = events.recv().await {
                let _ = broker.apply_ws_event(message);
            }
        });
    }

    fn apply_ws_event(&self, message: Value) -> Result<(), HlBrokerError> {
        match message.get("channel").and_then(Value::as_str) {
            Some("allMids") => self.apply_mids_event(&message),
            Some("clearinghouseState") => {
                let account = parse_ws_clearinghouse_state(&message).map_err(transport_error)?;
                self.state
                    .write()
                    .map_err(|_| HlBrokerError::StateUnavailable)?
                    .account = account;
                self.mark_fresh(|freshness| freshness.account = Some(Utc::now()))?;
                Ok(())
            }
            Some("openOrders") => {
                let open_orders = parse_ws_open_orders(&message).map_err(transport_error)?;
                self.state
                    .write()
                    .map_err(|_| HlBrokerError::StateUnavailable)?
                    .open_orders = open_orders;
                self.mark_fresh(|freshness| freshness.open_orders = Some(Utc::now()))?;
                Ok(())
            }
            Some("orderUpdates") => {
                for value in event_values(&message) {
                    push_bounded(&self.order_updates, parse_order_update(&message, value))?;
                }
                Ok(())
            }
            Some("userFills") => {
                for value in event_values(&message) {
                    push_bounded(&self.fills, parse_user_fill(&message, value))?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn apply_mids_event(&self, message: &Value) -> Result<(), HlBrokerError> {
        self.mids
            .write()
            .map_err(|_| HlBrokerError::StateUnavailable)?
            .apply_ws_message(message)
            .map_err(transport_error)?;
        self.mark_fresh(|freshness| freshness.mids = Some(Utc::now()))
    }

    fn mark_fresh(&self, update: impl FnOnce(&mut HlFreshness)) -> Result<(), HlBrokerError> {
        let mut freshness = self
            .freshness
            .write()
            .map_err(|_| HlBrokerError::StateUnavailable)?;
        update(&mut freshness);
        Ok(())
    }

    fn is_fresh(&self, field: Option<DateTime<Utc>>) -> bool {
        field
            .map(|updated_at| {
                Utc::now()
                    .signed_duration_since(updated_at)
                    .to_std()
                    .map(|age| age <= self.freshness_max_age)
                    .unwrap_or(false)
            })
            .unwrap_or(false)
    }

    fn ensure_trading_state_fresh(&self) -> Result<(), HlBrokerError> {
        let freshness = self
            .freshness
            .read()
            .map_err(|_| HlBrokerError::StateUnavailable)?
            .clone();
        if !self.is_fresh(freshness.metadata)
            || !self.is_fresh(freshness.mids)
            || !self.is_fresh(freshness.account)
            || !self.is_fresh(freshness.open_orders)
        {
            return Err(HlBrokerError::StateUnavailable);
        }
        Ok(())
    }

    pub async fn refresh_metadata(&self) -> Result<(), HlBrokerError> {
        let snapshot = self
            .client
            .meta()
            .await
            .map_err(|error| HlBrokerError::Transport {
                message: error.to_string(),
            })?;
        *self
            .metadata
            .write()
            .map_err(|_| HlBrokerError::StateUnavailable)? = snapshot;
        self.freshness
            .write()
            .map_err(|_| HlBrokerError::StateUnavailable)?
            .metadata = Some(Utc::now());
        Ok(())
    }

    pub async fn refresh_mids(&self) -> Result<(), HlBrokerError> {
        let snapshot = self
            .client
            .all_mids()
            .await
            .map_err(|error| HlBrokerError::Transport {
                message: error.to_string(),
            })?;
        *self
            .mids
            .write()
            .map_err(|_| HlBrokerError::StateUnavailable)? = snapshot;
        self.freshness
            .write()
            .map_err(|_| HlBrokerError::StateUnavailable)?
            .mids = Some(Utc::now());
        Ok(())
    }

    pub fn metadata(&self) -> HlMetadataSnapshot {
        self.metadata
            .read()
            .map(|value| value.clone())
            .unwrap_or_default()
    }

    pub fn mid_price(&self, coin: &HlCoin) -> Option<crate::core::Decimal> {
        self.mids.read().ok()?.mids.get(coin).copied()
    }

    pub fn spawn_background_tasks(
        self: std::sync::Arc<Self>,
        metadata_interval: std::time::Duration,
        reconciliation_interval: std::time::Duration,
    ) -> tokio::task::JoinHandle<()> {
        let metadata_broker = std::sync::Arc::clone(&self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(metadata_interval);
            loop {
                ticker.tick().await;
                let _ = metadata_broker.refresh_metadata().await;
            }
        });

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(reconciliation_interval);
            loop {
                ticker.tick().await;
                if let Ok(account) = self.client.clearinghouse_state(&self.account_address).await {
                    if let Ok(mut state) = self.state.write() {
                        state.account = account;
                    }
                    let _ = self.mark_fresh(|freshness| freshness.account = Some(Utc::now()));
                } else {
                    let _ = self.mark_fresh(|freshness| freshness.account = None);
                }
                if let Ok(open_orders) = self.client.open_orders(&self.account_address).await {
                    if let Ok(mut state) = self.state.write() {
                        state.open_orders = open_orders;
                    }
                    let _ = self.mark_fresh(|freshness| freshness.open_orders = Some(Utc::now()));
                } else {
                    let _ = self.mark_fresh(|freshness| freshness.open_orders = None);
                }
            }
        })
    }

    fn next_client_order_id(&self) -> HlClientOrderId {
        let now = current_millis();
        let value = self
            .next_client_order_id
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                Some(current.saturating_add(1).max(now))
            })
            .unwrap_or(now);
        HlClientOrderId::new(format!("0x{value:032x}"))
            .expect("generated client order id must be valid")
    }

    pub async fn order_status(&self, order_id_or_cloid: &str) -> Result<Value, HlBrokerError> {
        self.client
            .order_status(&self.account_address, order_id_or_cloid)
            .await
            .map_err(transport_error)
    }
}

fn current_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn transport_error(error: impl std::fmt::Display) -> HlBrokerError {
    HlBrokerError::Transport {
        message: error.to_string(),
    }
}

fn protected_price(
    reference: crate::core::Decimal,
    side: crate::core::Side,
    max_slippage_bps: u32,
) -> crate::core::Decimal {
    let slippage =
        crate::core::Decimal::from(max_slippage_bps) / crate::core::Decimal::from(10_000u32);
    if side == crate::core::Side::Buy {
        reference * (crate::core::Decimal::ONE + slippage)
    } else {
        reference * (crate::core::Decimal::ONE - slippage)
    }
}

fn normalize_order_precision(
    size: crate::core::Decimal,
    price: crate::core::Decimal,
    size_decimals: u32,
) -> Result<(crate::core::Decimal, crate::core::Decimal), String> {
    let normalized_size = size.round_dp(size_decimals);
    let price_decimals = 6u32.saturating_sub(size_decimals);
    let normalized_price = price
        .round_sf(5)
        .ok_or_else(|| "price cannot be normalized to five significant digits".to_string())?
        .round_dp(price_decimals);
    if normalized_size != size {
        return Err(format!(
            "order size must have at most {size_decimals} decimals"
        ));
    }
    if normalized_price <= crate::core::Decimal::ZERO {
        return Err("normalized order price must be positive".to_string());
    }
    Ok((normalized_size, normalized_price))
}

fn push_bounded<T>(lock: &RwLock<Vec<T>>, value: T) -> Result<(), HlBrokerError> {
    let mut values = lock.write().map_err(|_| HlBrokerError::StateUnavailable)?;
    if values.len() == ACCOUNT_EVENT_HISTORY_LIMIT {
        values.remove(0);
    }
    values.push(value);
    Ok(())
}

fn event_data(message: &Value) -> &Value {
    message.get("data").unwrap_or(message)
}

fn event_values(message: &Value) -> Vec<&Value> {
    match event_data(message) {
        Value::Array(values) => values.iter().collect(),
        value => vec![value],
    }
}

fn string_field(value: &Value, names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        value.get(*name).and_then(|field| {
            field
                .as_str()
                .map(str::to_string)
                .or_else(|| field.as_u64().map(|number| number.to_string()))
        })
    })
}

fn decimal_field(value: &Value, names: &[&str]) -> Option<crate::core::Decimal> {
    string_field(value, names)?.parse().ok()
}

fn parse_order_update(message: &Value, value: &Value) -> HlOrderUpdate {
    HlOrderUpdate {
        order_id: string_field(value, &["oid", "orderId"]),
        client_order_id: string_field(value, &["cloid", "clientOrderId"]),
        status: string_field(value, &["status", "orderStatus"]),
        raw: message.clone(),
    }
}

fn parse_user_fill(message: &Value, value: &Value) -> HlUserFill {
    HlUserFill {
        order_id: string_field(value, &["oid", "orderId"]),
        client_order_id: string_field(value, &["cloid", "clientOrderId"]),
        coin: string_field(value, &["coin"]),
        size: decimal_field(value, &["sz", "size"]),
        price: decimal_field(value, &["px", "price"]),
        fee: decimal_field(value, &["fee"]),
        raw: message.clone(),
    }
}

#[async_trait::async_trait]
impl HyperliquidBroker for HyperliquidLiveBroker {
    fn account_state(&self) -> HlAccountState {
        self.state
            .read()
            .map(|state| state.account.clone())
            .unwrap_or_default()
    }

    fn open_orders(&self) -> Vec<HlOpenOrder> {
        self.state
            .read()
            .map(|state| state.open_orders.clone())
            .unwrap_or_default()
    }

    async fn place_order(
        &self,
        mut request: HlOrderRequest,
    ) -> Result<HlOrderResult, HlBrokerError> {
        request
            .validate()
            .map_err(|message| HlBrokerError::InvalidRequest { message })?;
        self.ensure_trading_state_fresh()?;
        if request.client_order_id.is_none() {
            request.client_order_id = Some(self.next_client_order_id());
        }
        let price = match &request.order_type {
            HlOrderType::Limit { limit_price, .. } => *limit_price,
            HlOrderType::Market { max_slippage_bps } => protected_price(
                self.mid_price(&request.coin)
                    .ok_or(HlBrokerError::StateUnavailable)?,
                request.side,
                *max_slippage_bps,
            ),
            HlOrderType::Trigger {
                trigger_price,
                execution,
                ..
            } => match execution {
                crate::hyperliquid::types::HlTriggerExecution::Market { max_slippage_bps } => {
                    protected_price(*trigger_price, request.side, *max_slippage_bps)
                }
                crate::hyperliquid::types::HlTriggerExecution::Limit { limit_price } => {
                    *limit_price
                }
            },
        };
        let asset = {
            let metadata = self
                .metadata
                .read()
                .map_err(|_| HlBrokerError::StateUnavailable)?;
            metadata
                .asset(&request.coin)
                .map(|asset| (asset.asset_id, asset.size_decimals))
        };
        let (asset, size_decimals) = asset.ok_or_else(|| HlBrokerError::InvalidRequest {
            message: format!("unknown Hyperliquid coin {}", request.coin.0),
        })?;
        let (normalized_size, price) =
            normalize_order_precision(request.size, price, size_decimals)
                .map_err(|message| HlBrokerError::InvalidRequest { message })?;
        request.size = normalized_size;
        let (account, open_order_count) = {
            let state = self
                .state
                .read()
                .map_err(|_| HlBrokerError::StateUnavailable)?;
            (state.account.clone(), state.open_orders.len())
        };
        let input = order_risk_input_at_price(
            self.strategy_id.clone(),
            &account,
            &request,
            price,
            open_order_count,
        );

        if let RiskDecision::Rejected { violations } = self.risk_guard.check(&input) {
            return Err(HlBrokerError::RiskRejected { violations });
        }
        let signer = &self.signer;
        let ws = &self.ws;
        let action = request.to_order_action(asset, price);
        let nonce = signer.next_nonce();
        let expires_after = request
            .expires_after
            .map(|value| value.timestamp_millis() as u64);
        let signed = signer
            .sign_action(
                &action,
                nonce,
                None,
                expires_after,
                self.network == HlNetwork::Mainnet,
            )
            .map_err(|error| HlBrokerError::Transport {
                message: error.to_string(),
            })?;
        let raw = ws
            .post(serde_json::json!({
                "type": "action",
                "payload": signed.to_exchange_payload(),
            }))
            .await
            .map_err(|_error| HlBrokerError::OutcomeUnknown {
                client_order_id: request
                    .client_order_id
                    .clone()
                    .expect("cloid generated above"),
            })?;
        parse_order_result(
            raw,
            HlSubmittedOrder {
                coin: request.coin,
                side: request.side,
                size: request.size,
                limit_price: price,
                reduce_only: request.reduce_only,
                client_order_id: request.client_order_id.expect("cloid generated above"),
            },
        )
    }

    async fn cancel_order(
        &self,
        _request: HlCancelRequest,
    ) -> Result<HlCancelResponse, HlBrokerError> {
        let signer = &self.signer;
        let ws = &self.ws;
        let asset = self
            .metadata
            .read()
            .map_err(|_| HlBrokerError::StateUnavailable)?
            .asset(&_request.coin)
            .ok_or(HlBrokerError::StateUnavailable)?
            .asset_id;
        let action = match _request.target {
            Some(crate::hyperliquid::types::HlCancelTarget::ClientOrderId(cloid)) => {
                HlExchangeAction::CancelByCloid(crate::hyperliquid::types::HlCancelByCloidAction {
                    cancels: vec![crate::hyperliquid::types::HlWireCancelByCloid {
                        asset,
                        client_order_id: cloid.as_str().to_string(),
                    }],
                    fast: _request.fast,
                })
            }
            _ => {
                let oid = _request.order_id.0.parse::<u64>().map_err(|_| {
                    HlBrokerError::InvalidRequest {
                        message: "Hyperliquid order id must be numeric".to_string(),
                    }
                })?;
                HlExchangeAction::Cancel(crate::hyperliquid::types::HlCancelAction {
                    cancels: vec![crate::hyperliquid::types::HlWireCancel {
                        asset,
                        order_id: oid,
                    }],
                    fast: _request.fast,
                })
            }
        };
        let nonce = signer.next_nonce();
        let expires_after = _request
            .expires_after
            .map(|value| value.timestamp_millis() as u64);
        let signed = signer
            .sign_action(
                &action,
                nonce,
                None,
                expires_after,
                self.network == HlNetwork::Mainnet,
            )
            .map_err(|error| HlBrokerError::Transport {
                message: error.to_string(),
            })?;
        let raw = ws
            .post(serde_json::json!({
                "type": "action",
                "payload": signed.to_exchange_payload(),
            }))
            .await
            .map_err(|error| HlBrokerError::Transport {
                message: error.to_string(),
            })?;
        parse_cancel_response(raw)
    }

    async fn close_position(
        &self,
        request: HlCloseRequest,
    ) -> Result<HlOrderResult, HlBrokerError> {
        let position = self
            .position(&request.coin)
            .filter(|position| position.size != crate::core::Decimal::ZERO)
            .ok_or_else(|| HlBrokerError::PositionUnavailable {
                coin: request.coin.clone(),
            })?;
        let size = match request.size {
            HlCloseSize::Full => position.size.abs(),
            HlCloseSize::Exact(size) if size > crate::core::Decimal::ZERO => size,
            HlCloseSize::Exact(_) => {
                return Err(HlBrokerError::InvalidRequest {
                    message: "close size must be positive".to_string(),
                });
            }
        };

        self.place_order(HlOrderRequest {
            coin: request.coin,
            side: if position.size.is_sign_positive() {
                crate::core::Side::Sell
            } else {
                crate::core::Side::Buy
            },
            size,
            reduce_only: true,
            order_type: HlOrderType::Market {
                max_slippage_bps: request.max_slippage_bps,
            },
            client_order_id: request.client_order_id,
            expires_after: request.expires_after,
        })
        .await
    }
}

fn parse_order_result(
    raw: Value,
    submitted: HlSubmittedOrder,
) -> Result<HlOrderResult, HlBrokerError> {
    let payload =
        raw.pointer("/data/response/payload")
            .ok_or_else(|| HlBrokerError::ExchangeRejected {
                message: "missing websocket order response payload".to_string(),
                raw: raw.clone(),
            })?;
    if payload.get("type").and_then(Value::as_str) == Some("error") {
        return Err(HlBrokerError::ExchangeRejected {
            message: payload
                .get("payload")
                .and_then(Value::as_str)
                .unwrap_or("websocket action error")
                .to_string(),
            raw,
        });
    }
    let data =
        payload
            .pointer("/response/data")
            .ok_or_else(|| HlBrokerError::ExchangeRejected {
                message: "missing websocket order response data".to_string(),
                raw: raw.clone(),
            })?;
    let status = data
        .get("statuses")
        .and_then(Value::as_array)
        .and_then(|statuses| statuses.first())
        .ok_or_else(|| HlBrokerError::ExchangeRejected {
            message: "missing websocket order status".to_string(),
            raw: raw.clone(),
        })?;
    let outcome = if let Some(resting) = status.get("resting") {
        HlOrderOutcome::Resting {
            order_id: crate::core::OrderId::new(
                resting
                    .get("oid")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| HlBrokerError::ExchangeRejected {
                        message: "resting response is missing oid".to_string(),
                        raw: raw.clone(),
                    })?
                    .to_string(),
            ),
        }
    } else if let Some(filled) = status.get("filled") {
        HlOrderOutcome::Filled {
            order_id: crate::core::OrderId::new(
                filled
                    .get("oid")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| HlBrokerError::ExchangeRejected {
                        message: "filled response is missing oid".to_string(),
                        raw: raw.clone(),
                    })?
                    .to_string(),
            ),
            total_size: filled
                .get("totalSz")
                .and_then(Value::as_str)
                .ok_or_else(|| HlBrokerError::ExchangeRejected {
                    message: "filled response is missing totalSz".to_string(),
                    raw: raw.clone(),
                })?
                .parse()
                .map_err(|_| HlBrokerError::ExchangeRejected {
                    message: "invalid filled totalSz".to_string(),
                    raw: raw.clone(),
                })?,
            avg_price: filled
                .get("avgPx")
                .and_then(Value::as_str)
                .ok_or_else(|| HlBrokerError::ExchangeRejected {
                    message: "filled response is missing avgPx".to_string(),
                    raw: raw.clone(),
                })?
                .parse()
                .map_err(|_| HlBrokerError::ExchangeRejected {
                    message: "invalid filled avgPx".to_string(),
                    raw: raw.clone(),
                })?,
        }
    } else if let Some(message) = status.get("error").and_then(Value::as_str) {
        return Err(HlBrokerError::ExchangeRejected {
            message: message.to_string(),
            raw,
        });
    } else {
        return Err(HlBrokerError::ExchangeRejected {
            message: "unsupported websocket order status".to_string(),
            raw,
        });
    };
    Ok(HlOrderResult {
        submitted,
        outcome,
        raw,
    })
}

fn parse_cancel_response(raw: Value) -> Result<HlCancelResponse, HlBrokerError> {
    let statuses = raw
        .pointer("/data/response/payload/response/data/statuses")
        .and_then(Value::as_array)
        .ok_or_else(|| HlBrokerError::ExchangeRejected {
            message: "missing websocket cancel response statuses".to_string(),
            raw: raw.clone(),
        })?;
    let statuses = statuses
        .iter()
        .map(|status| {
            if status.as_str() == Some("success") {
                HlCancelStatus::Success
            } else if let Some(message) = status.get("error").and_then(Value::as_str) {
                HlCancelStatus::Error {
                    message: message.to_string(),
                }
            } else {
                HlCancelStatus::Error {
                    message: "unsupported websocket cancel status".to_string(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_filled_order_post_response() {
        let raw = serde_json::json!({
            "channel": "post",
            "data": {
                "id": 1,
                "response": {
                    "type": "action",
                    "payload": {
                        "status": "ok",
                        "response": {
                            "type": "order",
                            "data": {
                                "statuses": [{
                                    "filled": {
                                        "totalSz": "0.02",
                                        "avgPx": "1891.4",
                                        "oid": 77747314
                                    }
                                }]
                            }
                        }
                    }
                }
            }
        });
        let result = parse_order_result(
            raw,
            HlSubmittedOrder {
                coin: HlCoin::new("ETH"),
                side: crate::core::Side::Buy,
                size: "0.02".parse().unwrap(),
                limit_price: "1900".parse().unwrap(),
                reduce_only: false,
                client_order_id: HlClientOrderId::new("0x0123456789abcdef0123456789abcdef")
                    .unwrap(),
            },
        )
        .unwrap();
        assert!(matches!(
            result.outcome,
            HlOrderOutcome::Filled { total_size, .. }
                if total_size == "0.02".parse().unwrap()
        ));
    }

    #[test]
    fn parses_cancel_success_post_response() {
        let raw = serde_json::json!({
            "data": {
                "response": {
                    "payload": {
                        "response": {
                            "data": {"statuses": ["success"]}
                        }
                    }
                }
            }
        });
        let result = parse_cancel_response(raw).unwrap();
        assert!(result.success);
        assert!(matches!(result.statuses[0], HlCancelStatus::Success));
    }

    #[test]
    fn normalizes_hyperliquid_price_precision() {
        let (_, price) =
            normalize_order_precision("1.25".parse().unwrap(), "123.456789".parse().unwrap(), 3)
                .unwrap();
        assert_eq!(price, "123.46".parse().unwrap());
    }

    #[test]
    fn rejects_size_beyond_metadata_precision() {
        let result = normalize_order_precision("1.001".parse().unwrap(), "100".parse().unwrap(), 2);
        assert!(result.is_err());
    }

    #[test]
    fn structures_fill_event_and_keeps_raw_payload() {
        let message = serde_json::json!({
            "channel": "userFills",
            "data": {
                "oid": 42,
                "cloid": "0xabc",
                "coin": "BTC",
                "sz": "0.1",
                "px": "100.5",
                "fee": "0.01"
            }
        });
        let fill = parse_user_fill(&message, event_data(&message));
        assert_eq!(fill.order_id.as_deref(), Some("42"));
        assert_eq!(fill.coin.as_deref(), Some("BTC"));
        assert_eq!(fill.size, Some("0.1".parse().unwrap()));
        assert_eq!(fill.raw, message);
    }
}
