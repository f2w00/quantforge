use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use alloy::primitives::Address;
use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::sync::{mpsc, watch};

use crate::core::{Decimal, StrategyId};
use crate::hyperliquid::broker::HlBrokerError;
use crate::hyperliquid::broker::risk_adapter::order_risk_input_at_price;
use crate::hyperliquid::broker::state::HlBrokerState;
use crate::hyperliquid::broker::traits::HyperliquidBroker;
use crate::hyperliquid::client::rest::{parse_ws_clearinghouse_state, parse_ws_open_orders};
use crate::hyperliquid::client::ws::HyperliquidWsEvent;
use crate::hyperliquid::client::{HyperliquidRestClient, HyperliquidSigner, HyperliquidWsClient};
use crate::hyperliquid::types::{
    HlAccountState, HlCancelRequest, HlCancelResponse, HlCancelStatus, HlClientOrderId,
    HlCloseRequest, HlCloseSize, HlCoin, HlExchangeAction, HlMetadataSnapshot, HlMidSnapshot,
    HlOpenOrder, HlOrderOutcome, HlOrderRequest, HlOrderResult, HlOrderSize, HlOrderType,
    HlSubmittedOrder, HlUpdateLeverageAction,
};
use crate::risk::{RiskDecision, RiskGuard};

const ACCOUNT_EVENT_HISTORY_LIMIT: usize = 1_000;
const RECOVERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
const REQUIRED_SUBSCRIPTIONS: [&str; 5] = [
    "allMids",
    "clearinghouseState",
    "openOrders",
    "orderUpdates",
    "userFills",
];

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
    pub markets: Vec<HlMarketConfig>,
    pub default_market_slippage_bps: u32,
    pub default_close_slippage_bps: u32,
}

impl HlLiveBrokerConfig {
    pub fn new(strategy_id: StrategyId, account_address: Address) -> Self {
        Self {
            strategy_id,
            network: HlNetwork::Testnet,
            account_address,
            metadata_refresh_interval: std::time::Duration::from_secs(60 * 60),
            connect_timeout: std::time::Duration::from_secs(10),
            freshness_max_age: std::time::Duration::from_secs(30),
            reconciliation_interval: std::time::Duration::from_secs(10),
            markets: Vec::new(),
            default_market_slippage_bps: 100,
            default_close_slippage_bps: 100,
        }
    }
}

#[derive(Clone, Debug)]
pub struct HlMarketConfig {
    pub coin: HlCoin,
    pub leverage: u32,
    pub is_cross: bool,
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
    ws_ready: watch::Sender<bool>,
    subscriptions: Mutex<HashSet<String>>,
    pending_notional: Mutex<HashMap<HlClientOrderId, Decimal>>,
    pending_cancels: Mutex<HashSet<String>>,
    orders: Mutex<HashMap<HlClientOrderId, HlTrackedOrder>>,
    order_notifiers: Mutex<HashMap<HlClientOrderId, watch::Sender<HlTrackedOrder>>>,
    markets: HashMap<HlCoin, HlMarketConfig>,
    default_market_slippage_bps: u32,
    default_close_slippage_bps: u32,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HlTrackedOrderState {
    PendingSubmit,
    Open,
    Filled,
    Canceled,
    Rejected,
    Expired,
    OutcomeUnknown,
}

impl HlTrackedOrderState {
    fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Canceled | Self::Rejected | Self::Expired
        )
    }
}

#[derive(Clone, Debug)]
pub struct HlTrackedOrder {
    pub submitted: HlSubmittedOrder,
    pub order_id: Option<String>,
    pub filled_size: Decimal,
    pub state: HlTrackedOrderState,
}

#[derive(Clone, Debug, Default)]
struct HlFreshness {
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
        if config.default_market_slippage_bps >= 10_000
            || config.default_close_slippage_bps >= 10_000
        {
            return Err(HlBrokerError::InvalidRequest {
                message: "default slippage must be less than 10000 bps".to_string(),
            });
        }
        let client = HyperliquidRestClient::new(config.network.rest_url());
        let user = format!("{:#x}", config.account_address);
        let signer_address = format!("{:#x}", signer.wallet_address());
        let agent_owner = client
            .agent_owner(&signer_address)
            .await
            .map_err(transport_error)?;
        if agent_owner != config.account_address {
            return Err(HlBrokerError::InvalidRequest {
                message: format!(
                    "API wallet {signer_address} is authorized for {agent_owner:#x}, not \
                     configured account {user}"
                ),
            });
        }
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
            config
                .markets
                .iter()
                .cloned()
                .map(|market| (market.coin.clone(), market))
                .collect(),
            config.default_market_slippage_bps,
            config.default_close_slippage_bps,
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
        for market in &config.markets {
            broker.set_leverage(market).await?;
        }
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
        markets: HashMap<HlCoin, HlMarketConfig>,
        default_market_slippage_bps: u32,
        default_close_slippage_bps: u32,
    ) -> Self {
        let mids_updated_at = mids.updated_at;
        let (ws_ready, _) = watch::channel(false);
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
                mids: mids_updated_at,
                account: Some(Utc::now()),
                open_orders: Some(Utc::now()),
            }),
            ws_ready,
            subscriptions: Mutex::new(HashSet::new()),
            pending_notional: Mutex::new(HashMap::new()),
            pending_cancels: Mutex::new(HashSet::new()),
            orders: Mutex::new(HashMap::new()),
            order_notifiers: Mutex::new(HashMap::new()),
            markets,
            default_market_slippage_bps,
            default_close_slippage_bps,
        }
    }

    async fn wait_for_subscriptions(
        &self,
        mut events: mpsc::Receiver<HyperliquidWsEvent>,
        timeout: std::time::Duration,
    ) -> Result<mpsc::Receiver<HyperliquidWsEvent>, HlBrokerError> {
        let expected = REQUIRED_SUBSCRIPTIONS
            .into_iter()
            .map(str::to_string)
            .collect::<std::collections::HashSet<_>>();
        let mut acknowledged = std::collections::HashSet::new();
        tokio::time::timeout(timeout, async {
            while acknowledged != expected {
                let event = events
                    .recv()
                    .await
                    .ok_or_else(|| HlBrokerError::Transport {
                        message: "websocket closed before subscription confirmation".to_string(),
                    })?;
                let message = match event {
                    HyperliquidWsEvent::Connected => continue,
                    HyperliquidWsEvent::Disconnected => {
                        return Err(HlBrokerError::Transport {
                            message: "websocket disconnected before subscription confirmation"
                                .to_string(),
                        });
                    }
                    HyperliquidWsEvent::Message(message) => message,
                };
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
        if let Ok(mut subscriptions) = self.subscriptions.lock() {
            *subscriptions = expected;
        }
        let _ = self.ws_ready.send(true);
        Ok(events)
    }

    fn spawn_event_consumer(
        self: &std::sync::Arc<Self>,
        mut events: mpsc::Receiver<HyperliquidWsEvent>,
    ) {
        let broker = std::sync::Arc::clone(self);
        tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                match event {
                    HyperliquidWsEvent::Connected => {
                        let _ = broker.ws_ready.send(false);
                        if let Ok(mut subscriptions) = broker.subscriptions.lock() {
                            subscriptions.clear();
                        }
                    }
                    HyperliquidWsEvent::Disconnected => broker.mark_ws_unavailable(),
                    HyperliquidWsEvent::Message(message) => {
                        if broker.apply_ws_event(message.clone()).is_err() {
                            let _ = broker.invalidate_event_freshness(&message);
                        }
                        broker.confirm_ws_recovery(&message);
                    }
                }
            }
            broker.mark_ws_unavailable();
        });
    }

    fn mark_ws_unavailable(&self) {
        let _ = self.ws_ready.send(false);
        if let Ok(mut subscriptions) = self.subscriptions.lock() {
            subscriptions.clear();
        }
        let _ = self.mark_fresh(|freshness| {
            freshness.mids = None;
            freshness.account = None;
            freshness.open_orders = None;
        });
    }

    fn confirm_ws_recovery(&self, message: &Value) {
        if message.get("channel").and_then(Value::as_str) != Some("subscriptionResponse") {
            return;
        }
        let Some(subscription) = message
            .pointer("/data/subscription/type")
            .and_then(Value::as_str)
        else {
            return;
        };
        let Ok(mut subscriptions) = self.subscriptions.lock() else {
            return;
        };
        subscriptions.insert(subscription.to_string());
        if REQUIRED_SUBSCRIPTIONS
            .iter()
            .all(|required| subscriptions.contains(*required))
        {
            let _ = self.ws_ready.send(true);
        }
    }

    fn invalidate_event_freshness(&self, message: &Value) -> Result<(), HlBrokerError> {
        match message.get("channel").and_then(Value::as_str) {
            Some("clearinghouseState") => self.mark_fresh(|freshness| freshness.account = None),
            Some("openOrders") => self.mark_fresh(|freshness| freshness.open_orders = None),
            Some("allMids") => self.mark_fresh(|freshness| freshness.mids = None),
            _ => Ok(()),
        }
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
                self.confirm_pending_open_orders()?;
                self.mark_fresh(|freshness| freshness.open_orders = Some(Utc::now()))?;
                Ok(())
            }
            Some("orderUpdates") => {
                for value in event_values(&message) {
                    let update = parse_order_update(&message, value);
                    self.apply_order_update(&update)?;
                    push_bounded(&self.order_updates, update)?;
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
        self.ws_ready.send_modify(|_| {});
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

    fn trading_state_is_fresh(&self) -> Result<bool, HlBrokerError> {
        let freshness = self
            .freshness
            .read()
            .map_err(|_| HlBrokerError::StateUnavailable)?
            .clone();
        Ok(*self.ws_ready.borrow()
            && self.is_fresh(freshness.mids)
            && self.is_fresh(freshness.account)
            && self.is_fresh(freshness.open_orders))
    }

    async fn ensure_trading_state_fresh(&self) -> Result<(), HlBrokerError> {
        if self.trading_state_is_fresh()? {
            return Ok(());
        }
        let mut ws_ready = self.ws_ready.subscribe();
        tokio::time::timeout(RECOVERY_TIMEOUT, async {
            loop {
                if self.trading_state_is_fresh()? {
                    return Ok(());
                }
                ws_ready
                    .changed()
                    .await
                    .map_err(|_| HlBrokerError::StateUnavailable)?;
            }
        })
        .await
        .map_err(|_| HlBrokerError::StateUnavailable)?
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
                if let Err(error) = metadata_broker.refresh_metadata().await {
                    eprintln!("Hyperliquid metadata refresh failed: {error}");
                }
            }
        });

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(reconciliation_interval);
            loop {
                ticker.tick().await;
                match self.client.clearinghouse_state(&self.account_address).await {
                    Ok(account) => {
                        if let Ok(mut state) = self.state.write() {
                            state.account = account;
                        }
                        let _ = self.mark_fresh(|freshness| freshness.account = Some(Utc::now()));
                    }
                    Err(error) => {
                        eprintln!("Hyperliquid account reconciliation failed: {error}");
                        let _ = self.mark_fresh(|freshness| freshness.account = None);
                    }
                }
                match self.client.open_orders(&self.account_address).await {
                    Ok(open_orders) => {
                        if let Ok(mut state) = self.state.write() {
                            state.open_orders = open_orders;
                        }
                        let _ = self.confirm_pending_open_orders();
                        let _ = self.reconcile_unknown_orders().await;
                        let _ = self.reconcile_pending_cancels().await;
                        let _ =
                            self.mark_fresh(|freshness| freshness.open_orders = Some(Utc::now()));
                    }
                    Err(error) => {
                        eprintln!("Hyperliquid open-order reconciliation failed: {error}");
                        let _ = self.mark_fresh(|freshness| freshness.open_orders = None);
                    }
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

    pub async fn reconcile_order(
        &self,
        client_order_id: &HlClientOrderId,
    ) -> Result<Value, HlBrokerError> {
        let status = self.order_status(client_order_id.as_str()).await?;
        Ok(status)
    }

    pub fn order(&self, client_order_id: &HlClientOrderId) -> Option<HlTrackedOrder> {
        self.orders.lock().ok()?.get(client_order_id).cloned()
    }

    pub async fn wait_order_terminal(
        &self,
        client_order_id: &HlClientOrderId,
        timeout: std::time::Duration,
    ) -> Result<HlTrackedOrder, HlBrokerError> {
        let mut receiver = self
            .order_notifiers
            .lock()
            .map_err(|_| HlBrokerError::StateUnavailable)?
            .get(client_order_id)
            .map(watch::Sender::subscribe)
            .ok_or_else(|| HlBrokerError::InvalidRequest {
                message: format!("unknown client order id {}", client_order_id.as_str()),
            })?;
        tokio::time::timeout(timeout, async {
            loop {
                let order = receiver.borrow().clone();
                if order.state.is_terminal() {
                    return Ok(order);
                }
                receiver
                    .changed()
                    .await
                    .map_err(|_| HlBrokerError::StateUnavailable)?;
            }
        })
        .await
        .map_err(|_| HlBrokerError::OrderWaitTimeout {
            client_order_id: client_order_id.clone(),
        })?
    }

    pub async fn set_leverage(&self, market: &HlMarketConfig) -> Result<(), HlBrokerError> {
        if market.leverage == 0 {
            return Err(HlBrokerError::InvalidRequest {
                message: "leverage must be positive".to_string(),
            });
        }
        if !market.is_cross {
            return Err(HlBrokerError::InvalidRequest {
                message: "isolated leverage is not supported yet".to_string(),
            });
        }
        let asset = self
            .metadata
            .read()
            .map_err(|_| HlBrokerError::StateUnavailable)?
            .asset(&market.coin)
            .cloned()
            .ok_or_else(|| HlBrokerError::InvalidRequest {
                message: format!("unknown Hyperliquid coin {}", market.coin.0),
            })?;
        if let Some(max_leverage) = asset.max_leverage {
            if market.leverage > max_leverage {
                return Err(HlBrokerError::InvalidRequest {
                    message: format!(
                        "leverage {} exceeds {} maximum of {}",
                        market.leverage, market.coin.0, max_leverage
                    ),
                });
            }
        }
        let action = HlExchangeAction::UpdateLeverage(HlUpdateLeverageAction {
            asset: asset.asset_id,
            is_cross: market.is_cross,
            leverage: market.leverage,
        });
        let signed = self
            .signer
            .sign_action(
                &action,
                self.signer.next_nonce(),
                None,
                None,
                self.network == HlNetwork::Mainnet,
            )
            .map_err(transport_error)?;
        let raw = self
            .ws
            .post(serde_json::json!({
                "type": "action",
                "payload": signed.to_exchange_payload(),
            }))
            .await
            .map_err(transport_error)?;
        parse_default_action_response(raw)
    }

    fn release_pending_notional(&self, client_order_id: &HlClientOrderId) {
        if let Ok(mut pending) = self.pending_notional.lock() {
            pending.remove(client_order_id);
        }
    }

    fn register_order(&self, submitted: HlSubmittedOrder) -> Result<(), HlBrokerError> {
        let client_order_id = submitted.client_order_id.clone();
        let order = HlTrackedOrder {
            submitted,
            order_id: None,
            filled_size: Decimal::ZERO,
            state: HlTrackedOrderState::PendingSubmit,
        };
        self.orders
            .lock()
            .map_err(|_| HlBrokerError::StateUnavailable)?
            .insert(client_order_id.clone(), order.clone());
        self.order_notifiers
            .lock()
            .map_err(|_| HlBrokerError::StateUnavailable)?
            .insert(client_order_id, watch::channel(order).0);
        Ok(())
    }

    fn update_order_state(
        &self,
        client_order_id: &HlClientOrderId,
        state: HlTrackedOrderState,
        order_id: Option<String>,
        filled_size: Option<Decimal>,
    ) {
        let order = self.orders.lock().ok().and_then(|mut orders| {
            let order = orders.get_mut(client_order_id)?;
            order.state = state;
            if order_id.is_some() {
                order.order_id = order_id;
            }
            if let Some(filled_size) = filled_size {
                order.filled_size = filled_size;
            }
            Some(order.clone())
        });
        if let Some(order) = order {
            if order.state.is_terminal() {
                self.release_pending_notional(client_order_id);
            }
            if let Some(sender) = self
                .order_notifiers
                .lock()
                .ok()
                .and_then(|notifiers| notifiers.get(client_order_id).cloned())
            {
                let _ = sender.send(order);
            }
        }
    }

    fn apply_order_update(&self, update: &HlOrderUpdate) -> Result<(), HlBrokerError> {
        let Some(client_order_id) = update
            .client_order_id
            .as_deref()
            .map(HlClientOrderId::new)
            .transpose()
            .map_err(|message| HlBrokerError::Transport { message })?
        else {
            return Ok(());
        };
        if let Some(state) = update.status.as_deref().and_then(order_update_state) {
            self.update_order_state(&client_order_id, state, update.order_id.clone(), None);
        }
        Ok(())
    }

    fn confirm_pending_open_orders(&self) -> Result<(), HlBrokerError> {
        let client_order_ids = self
            .state
            .read()
            .map_err(|_| HlBrokerError::StateUnavailable)?
            .open_orders
            .iter()
            .filter_map(|order| order.client_order_id.clone())
            .collect::<Vec<_>>();
        for client_order_id in client_order_ids {
            self.release_pending_notional(&client_order_id);
            self.update_order_state(&client_order_id, HlTrackedOrderState::Open, None, None);
        }
        Ok(())
    }

    async fn reconcile_unknown_orders(&self) -> Result<(), HlBrokerError> {
        let open_orders = self
            .state
            .read()
            .map_err(|_| HlBrokerError::StateUnavailable)?
            .open_orders
            .clone();
        let unknown_orders = self
            .orders
            .lock()
            .map_err(|_| HlBrokerError::StateUnavailable)?
            .iter()
            .filter(|(_, order)| order.state == HlTrackedOrderState::OutcomeUnknown)
            .map(|(client_order_id, _)| client_order_id.clone())
            .collect::<Vec<_>>();
        for client_order_id in unknown_orders {
            if let Some(order) = open_orders
                .iter()
                .find(|order| order.client_order_id.as_ref() == Some(&client_order_id))
            {
                self.update_order_state(
                    &client_order_id,
                    HlTrackedOrderState::Open,
                    Some(order.order_id.0.clone()),
                    None,
                );
                continue;
            }
            if let Ok(raw) = self.reconcile_order(&client_order_id).await
                && let Some(status) = order_status_from_response(&raw)
            {
                self.update_order_state(&client_order_id, status, None, None);
            }
        }
        Ok(())
    }

    async fn reconcile_pending_cancels(&self) -> Result<(), HlBrokerError> {
        let pending_cancels = self
            .pending_cancels
            .lock()
            .map_err(|_| HlBrokerError::StateUnavailable)?
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        for target in pending_cancels {
            let Ok(raw) = self.order_status(&target).await else {
                continue;
            };
            let Some(state) = order_status_from_response(&raw) else {
                continue;
            };
            if let Some(client_order_id) = self.client_order_id_for_target(&target) {
                self.update_order_state(&client_order_id, state.clone(), None, None);
            }
            if state.is_terminal()
                && let Ok(mut pending_cancels) = self.pending_cancels.lock()
            {
                pending_cancels.remove(&target);
            }
        }
        Ok(())
    }

    fn client_order_id_for_target(&self, target: &str) -> Option<HlClientOrderId> {
        HlClientOrderId::new(target).ok().or_else(|| {
            self.orders.lock().ok().and_then(|orders| {
                orders.iter().find_map(|(client_order_id, order)| {
                    (order.order_id.as_deref() == Some(target)).then(|| client_order_id.clone())
                })
            })
        })
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

fn resolve_margin_fraction_size(
    account: &HlAccountState,
    reference_price: Decimal,
    size_decimals: u32,
    leverage: u32,
    margin_fraction: Decimal,
    reserve_fraction: Decimal,
) -> Result<Decimal, HlBrokerError> {
    if margin_fraction <= Decimal::ZERO || margin_fraction > Decimal::ONE {
        return Err(HlBrokerError::InvalidRequest {
            message: "margin fraction must be in (0, 1]".to_string(),
        });
    }
    if leverage == 0 {
        return Err(HlBrokerError::InvalidRequest {
            message: "leverage must be positive".to_string(),
        });
    }
    if reserve_fraction < Decimal::ZERO || reserve_fraction >= Decimal::ONE {
        return Err(HlBrokerError::InvalidRequest {
            message: "reserve fraction must be in [0, 1)".to_string(),
        });
    }
    if reference_price <= Decimal::ZERO {
        return Err(HlBrokerError::InvalidRequest {
            message: "reference price must be positive".to_string(),
        });
    }
    let reserve_margin = account.equity * reserve_fraction;
    let available_margin =
        (account.equity - account.margin_used - reserve_margin).max(Decimal::ZERO);
    let margin = available_margin * margin_fraction;
    let notional = margin * Decimal::from(leverage);
    let size = (notional / reference_price)
        .round_dp_with_strategy(size_decimals, rust_decimal::RoundingStrategy::ToZero);
    if size <= Decimal::ZERO {
        return Err(HlBrokerError::InvalidRequest {
            message: "sizing result is below the minimum quantity increment".to_string(),
        });
    }
    Ok(size)
}

pub(crate) fn calculate_close_size(
    position_size: Decimal,
    fraction: Decimal,
    size_decimals: u32,
) -> Result<Decimal, HlBrokerError> {
    if fraction <= Decimal::ZERO || fraction > Decimal::ONE {
        return Err(HlBrokerError::InvalidRequest {
            message: "close fraction must be in (0, 1]".to_string(),
        });
    }
    let current_position_size = position_size.abs();
    let close_size = (current_position_size * fraction)
        .round_dp_with_strategy(size_decimals, rust_decimal::RoundingStrategy::ToZero);
    if close_size <= Decimal::ZERO {
        return Err(HlBrokerError::InvalidRequest {
            message: "close sizing result is below the minimum quantity increment".to_string(),
        });
    }
    Ok(close_size)
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

fn order_update_state(status: &str) -> Option<HlTrackedOrderState> {
    match status.to_ascii_lowercase().as_str() {
        "open" | "resting" => Some(HlTrackedOrderState::Open),
        "filled" => Some(HlTrackedOrderState::Filled),
        "canceled" | "margincanceled" | "selftradecanceled" | "delistedcanceled" => {
            Some(HlTrackedOrderState::Canceled)
        }
        "rejected" => Some(HlTrackedOrderState::Rejected),
        "expired" => Some(HlTrackedOrderState::Expired),
        _ => None,
    }
}

fn order_status_from_response(raw: &Value) -> Option<HlTrackedOrderState> {
    match raw {
        Value::Object(values) => values.iter().find_map(|(key, value)| {
            (key == "status" || key == "orderStatus")
                .then(|| value.as_str().and_then(order_update_state))
                .flatten()
                .or_else(|| order_status_from_response(value))
        }),
        Value::Array(values) => values.iter().find_map(order_status_from_response),
        _ => None,
    }
}

#[async_trait::async_trait]
impl HyperliquidBroker for HyperliquidLiveBroker {
    fn account_state(&self) -> Result<HlAccountState, HlBrokerError> {
        self.state
            .read()
            .map(|state| state.account.clone())
            .map_err(|_| HlBrokerError::StateUnavailable)
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
        self.ensure_trading_state_fresh().await?;
        if request
            .expires_after
            .is_some_and(|expires_after| expires_after <= Utc::now())
        {
            return Err(HlBrokerError::InvalidRequest {
                message: "order expiration must be in the future".to_string(),
            });
        }
        if request.client_order_id.is_none() {
            request.client_order_id = Some(self.next_client_order_id());
        }
        let price = match &request.order_type {
            HlOrderType::Limit { limit_price, .. } => *limit_price,
            HlOrderType::Market { max_slippage_bps } => protected_price(
                self.mid_price(&request.coin)
                    .ok_or(HlBrokerError::StateUnavailable)?,
                request.side,
                max_slippage_bps.unwrap_or(self.default_market_slippage_bps),
            ),
            HlOrderType::Trigger {
                trigger_price,
                execution,
                ..
            } => match execution {
                crate::hyperliquid::types::HlTriggerExecution::Market { max_slippage_bps } => {
                    protected_price(
                        *trigger_price,
                        request.side,
                        max_slippage_bps.unwrap_or(self.default_market_slippage_bps),
                    )
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
        let size = match request.size {
            HlOrderSize::Exact(size) if size > Decimal::ZERO => size,
            HlOrderSize::Exact(_) => {
                return Err(HlBrokerError::InvalidRequest {
                    message: "order size must be positive".to_string(),
                });
            }
            HlOrderSize::MarginFraction {
                margin_fraction,
                reserve_fraction,
            } => {
                let market = self.markets.get(&request.coin).ok_or_else(|| {
                    HlBrokerError::InvalidRequest {
                        message: format!(
                            "margin-fraction sizing requires configured leverage for {}",
                            request.coin.0
                        ),
                    }
                })?;
                resolve_margin_fraction_size(
                    &self.account_state()?,
                    price,
                    size_decimals,
                    market.leverage,
                    margin_fraction,
                    reserve_fraction,
                )?
            }
        };
        let (normalized_size, price) = normalize_order_precision(size, price, size_decimals)
            .map_err(|message| HlBrokerError::InvalidRequest { message })?;
        let (account, state_open_orders) = {
            let state = self
                .state
                .read()
                .map_err(|_| HlBrokerError::StateUnavailable)?;
            (state.account.clone(), state.open_orders.clone())
        };
        let client_order_id = request
            .client_order_id
            .clone()
            .expect("cloid generated above");
        let submitted = HlSubmittedOrder {
            coin: request.coin.clone(),
            side: request.side,
            size: normalized_size,
            limit_price: price,
            reduce_only: request.reduce_only,
            client_order_id: client_order_id.clone(),
        };
        if !request.reduce_only {
            let mut pending = self
                .pending_notional
                .lock()
                .map_err(|_| HlBrokerError::StateUnavailable)?;
            let input = order_risk_input_at_price(
                self.strategy_id.clone(),
                &account,
                &request,
                normalized_size,
                price,
                &state_open_orders,
                pending.values().copied().sum(),
            );
            if let RiskDecision::Rejected { violations } = self.risk_guard.check(&input) {
                return Err(HlBrokerError::RiskRejected { violations });
            }
            pending.insert(client_order_id.clone(), input.order_notional);
        } else {
            let input = order_risk_input_at_price(
                self.strategy_id.clone(),
                &account,
                &request,
                normalized_size,
                price,
                &state_open_orders,
                Decimal::ZERO,
            );
            if let RiskDecision::Rejected { violations } = self.risk_guard.check(&input) {
                return Err(HlBrokerError::RiskRejected { violations });
            }
        }
        self.register_order(submitted.clone())?;
        let signer = &self.signer;
        let ws = &self.ws;
        let action = request.to_order_action(asset, price, normalized_size);
        let nonce = signer.next_nonce();
        let expires_after = request
            .expires_after
            .map(|value| value.timestamp_millis() as u64);
        let signed = match signer.sign_action(
            &action,
            nonce,
            None,
            expires_after,
            self.network == HlNetwork::Mainnet,
        ) {
            Ok(signed) => signed,
            Err(error) => {
                self.release_pending_notional(&client_order_id);
                self.update_order_state(
                    &client_order_id,
                    HlTrackedOrderState::Rejected,
                    None,
                    None,
                );
                return Err(HlBrokerError::Transport {
                    message: error.to_string(),
                });
            }
        };
        let raw = ws
            .post(serde_json::json!({
                "type": "action",
                "payload": signed.to_exchange_payload(),
            }))
            .await
            .map_err(|_error| {
                self.update_order_state(
                    &client_order_id,
                    HlTrackedOrderState::OutcomeUnknown,
                    None,
                    None,
                );
                HlBrokerError::OutcomeUnknown {
                    client_order_id: client_order_id.clone(),
                }
            })?;
        let result = match parse_order_result(raw, submitted) {
            Ok(result) => result,
            Err(error) => {
                self.update_order_state(
                    &client_order_id,
                    HlTrackedOrderState::Rejected,
                    None,
                    None,
                );
                return Err(error);
            }
        };
        match &result.outcome {
            HlOrderOutcome::Resting { order_id } => self.update_order_state(
                &client_order_id,
                HlTrackedOrderState::Open,
                Some(order_id.0.clone()),
                None,
            ),
            HlOrderOutcome::Filled {
                order_id,
                total_size,
                ..
            } => self.update_order_state(
                &client_order_id,
                HlTrackedOrderState::Filled,
                Some(order_id.0.clone()),
                Some(*total_size),
            ),
        }
        Ok(result)
    }

    async fn cancel_order(
        &self,
        request: HlCancelRequest,
    ) -> Result<HlCancelResponse, HlBrokerError> {
        let signer = &self.signer;
        let ws = &self.ws;
        let target = match &request.target {
            Some(crate::hyperliquid::types::HlCancelTarget::ClientOrderId(client_order_id)) => {
                client_order_id.as_str().to_string()
            }
            _ => request.order_id.0.clone(),
        };
        let asset = self
            .metadata
            .read()
            .map_err(|_| HlBrokerError::StateUnavailable)?
            .asset(&request.coin)
            .ok_or(HlBrokerError::StateUnavailable)?
            .asset_id;
        let action = match request.target {
            Some(crate::hyperliquid::types::HlCancelTarget::ClientOrderId(cloid)) => {
                HlExchangeAction::CancelByCloid(crate::hyperliquid::types::HlCancelByCloidAction {
                    cancels: vec![crate::hyperliquid::types::HlWireCancelByCloid {
                        asset,
                        client_order_id: cloid.as_str().to_string(),
                    }],
                    fast: request.fast,
                })
            }
            _ => {
                let oid = request.order_id.0.parse::<u64>().map_err(|_| {
                    HlBrokerError::InvalidRequest {
                        message: "Hyperliquid order id must be numeric".to_string(),
                    }
                })?;
                HlExchangeAction::Cancel(crate::hyperliquid::types::HlCancelAction {
                    cancels: vec![crate::hyperliquid::types::HlWireCancel {
                        asset,
                        order_id: oid,
                    }],
                    fast: request.fast,
                })
            }
        };
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
        {
            let mut pending_cancels = self
                .pending_cancels
                .lock()
                .map_err(|_| HlBrokerError::StateUnavailable)?;
            if !pending_cancels.insert(target.clone()) {
                return Err(HlBrokerError::InvalidRequest {
                    message: format!("cancel outcome is pending for {target}"),
                });
            }
        }
        let raw = ws
            .post(serde_json::json!({
                "type": "action",
                "payload": signed.to_exchange_payload(),
            }))
            .await
            .map_err(|_| HlBrokerError::CancelOutcomeUnknown {
                target: target.clone(),
            })?;
        let result = parse_cancel_response(raw);
        if let Ok(mut pending_cancels) = self.pending_cancels.lock() {
            pending_cancels.remove(&target);
        }
        result
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
            HlCloseSize::Exact(size) if size > Decimal::ZERO => size,
            HlCloseSize::Exact(_) => {
                return Err(HlBrokerError::InvalidRequest {
                    message: "close size must be positive".to_string(),
                });
            }
            HlCloseSize::Fraction(fraction) => {
                let size_decimals = self
                    .metadata
                    .read()
                    .map_err(|_| HlBrokerError::StateUnavailable)?
                    .asset(&request.coin)
                    .ok_or(HlBrokerError::StateUnavailable)?
                    .size_decimals;
                calculate_close_size(position.size, fraction, size_decimals)?
            }
        };

        self.place_order(HlOrderRequest {
            coin: request.coin,
            side: if position.size.is_sign_positive() {
                crate::core::Side::Sell
            } else {
                crate::core::Side::Buy
            },
            size: HlOrderSize::Exact(size),
            reduce_only: true,
            order_type: HlOrderType::Market {
                max_slippage_bps: request
                    .max_slippage_bps
                    .or(Some(self.default_close_slippage_bps)),
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

fn parse_default_action_response(raw: Value) -> Result<(), HlBrokerError> {
    let payload = raw.pointer("/data/response/payload").unwrap_or(&raw);
    if payload.get("status").and_then(Value::as_str) == Some("ok")
        && payload.pointer("/response/type").and_then(Value::as_str) == Some("default")
    {
        return Ok(());
    }
    if payload.get("type").and_then(Value::as_str) == Some("error") {
        return Err(HlBrokerError::ExchangeRejected {
            message: payload
                .get("payload")
                .and_then(Value::as_str)
                .unwrap_or("exchange action error")
                .to_string(),
            raw,
        });
    }
    Err(HlBrokerError::ExchangeRejected {
        message: "unexpected update leverage response".to_string(),
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
    fn live_broker_config_uses_safe_defaults() {
        let config = HlLiveBrokerConfig::new(StrategyId::new("test"), Address::ZERO);

        assert_eq!(config.network, HlNetwork::Testnet);
        assert_eq!(
            config.metadata_refresh_interval,
            std::time::Duration::from_secs(60 * 60)
        );
        assert_eq!(config.connect_timeout, std::time::Duration::from_secs(10));
        assert_eq!(config.freshness_max_age, std::time::Duration::from_secs(30));
        assert_eq!(
            config.reconciliation_interval,
            std::time::Duration::from_secs(10)
        );
        assert!(config.markets.is_empty());
        assert_eq!(config.default_market_slippage_bps, 100);
        assert_eq!(config.default_close_slippage_bps, 100);
    }

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

    #[test]
    fn resolves_margin_fraction_size_down() {
        let account = HlAccountState {
            equity: "1000".parse().unwrap(),
            margin_used: "100".parse().unwrap(),
            positions: HashMap::new(),
            updated_at: Utc::now(),
        };
        let size = resolve_margin_fraction_size(
            &account,
            "12345".parse().unwrap(),
            3,
            5,
            "0.5".parse().unwrap(),
            "0.1".parse().unwrap(),
        )
        .unwrap();

        assert_eq!(size, "0.162".parse().unwrap());
    }

    #[test]
    fn sizes_fractional_close_down_to_asset_increment() {
        let result =
            calculate_close_size("0.1019".parse().unwrap(), "0.5".parse().unwrap(), 3).unwrap();

        assert_eq!(result, "0.050".parse().unwrap());
    }

    #[test]
    fn parses_update_leverage_response() {
        parse_default_action_response(serde_json::json!({
            "status": "ok",
            "response": {"type": "default"}
        }))
        .unwrap();
    }

    #[test]
    fn parses_websocket_update_leverage_response() {
        parse_default_action_response(serde_json::json!({
            "channel": "post",
            "data": {
                "response": {
                    "payload": {
                        "status": "ok",
                        "response": {"type": "default"}
                    }
                }
            }
        }))
        .unwrap();
    }

    #[test]
    fn parses_nested_order_status_for_reconciliation() {
        let raw = serde_json::json!({
            "status": "ok",
            "response": {"data": {"orderStatus": "filled"}}
        });

        assert_eq!(
            order_status_from_response(&raw),
            Some(HlTrackedOrderState::Filled)
        );
    }
}
