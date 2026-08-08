use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use alloy::primitives::Address;
use chrono::{DateTime, TimeZone, Utc};
use serde_json::Value;
use tokio::sync::{mpsc, watch};

use crate::audit::{AuditAction, AuditRecord, LedgerEvent, LedgerEventKind, RunJournal};
use crate::core::{Decimal, RunMode, StrategyId, Timestamp};
use crate::hyperliquid::broker::HlBrokerError;
use crate::hyperliquid::broker::risk_adapter::order_risk_input_at_price;
use crate::hyperliquid::broker::state::HlBrokerState;
use crate::hyperliquid::broker::traits::HyperliquidBroker;
use crate::hyperliquid::client::rest::{
    HlSpotUsdcState, HlUserAbstraction, order_status_from_response,
};
use crate::hyperliquid::client::ws::{
    HyperliquidWsEvent, funding_events, non_funding_ledger_events, order_updates,
    parse_cancel_response, parse_default_action_response, parse_order_outcome, user_fills,
    ws_clearinghouse_state, ws_open_orders,
};
use crate::hyperliquid::client::{
    HlOrderUpdate, HlUserFill, HyperliquidRestClient, HyperliquidSigner, HyperliquidWsClient,
};
use crate::hyperliquid::types::{
    HlAccountState, HlCancelRequest, HlCancelResponse, HlClientOrderId, HlCloseRequest,
    HlCloseSize, HlCoin, HlExchangeAction, HlMetadataSnapshot, HlMidSnapshot, HlOpenOrder,
    HlOrderOutcome, HlOrderRequest, HlOrderResult, HlOrderSize, HlOrderType, HlSubmittedOrder,
    HlUpdateLeverageAction,
};
use crate::risk::{RiskDecision, RiskGuard};

const ACCOUNT_EVENT_HISTORY_LIMIT: usize = 1_000;
const RECOVERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
const RECOVERY_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);
const REQUIRED_SUBSCRIPTIONS: [&str; 7] = [
    "allMids",
    "clearinghouseState",
    "openOrders",
    "orderUpdates",
    "userFills",
    "userFundings",
    "userNonFundingLedgerUpdates",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HlNetwork {
    Mainnet,
    Testnet,
}

impl HlNetwork {
    pub fn rest_url(self) -> &'static str {
        match self {
            Self::Mainnet => "https://api.hyperliquid.xyz",
            Self::Testnet => "https://api.hyperliquid-testnet.xyz",
        }
    }

    pub fn ws_url(self) -> &'static str {
        match self {
            Self::Mainnet => "wss://api.hyperliquid.xyz/ws",
            Self::Testnet => "wss://api.hyperliquid-testnet.xyz/ws",
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Mainnet => "mainnet",
            Self::Testnet => "testnet",
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
    pub default_margin_mode: HlMarginMode,
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
            default_margin_mode: HlMarginMode::Auto,
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
    pub margin_mode: Option<HlMarginMode>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HlMarginMode {
    Auto,
    Cross,
    Isolated,
}

impl HlMarginMode {
    fn is_cross(self) -> bool {
        matches!(self, Self::Cross)
    }

    fn name(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Cross => "cross",
            Self::Isolated => "isolated",
        }
    }
}

pub struct HyperliquidLiveBroker {
    strategy_id: StrategyId,
    journal: std::sync::Arc<RunJournal>,
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
    collateral: RwLock<HlCollateralState>,
    freshness_max_age: std::time::Duration,
    freshness: RwLock<HlFreshness>,
    ws_ready: watch::Sender<bool>,
    subscriptions: Mutex<HashSet<String>>,
    pending_notional: Mutex<HashMap<HlClientOrderId, Decimal>>,
    pending_cancels: Mutex<HashSet<String>>,
    orders: Mutex<HashMap<HlClientOrderId, HlTrackedOrder>>,
    order_notifiers: Mutex<HashMap<HlClientOrderId, watch::Sender<HlTrackedOrder>>>,
    markets: Mutex<HashMap<HlCoin, HlMarketConfig>>,
    market_locks: Mutex<HashMap<HlCoin, std::sync::Arc<tokio::sync::Mutex<()>>>>,
    default_margin_mode: HlMarginMode,
    default_market_slippage_bps: u32,
    default_close_slippage_bps: u32,
}

#[derive(Clone, Debug)]
struct HlCollateralState {
    abstraction: HlUserAbstraction,
    spot_usdc: Option<HlSpotUsdcState>,
}

impl HlCollateralState {
    fn total_collateral(&self, perp_account: &HlAccountState) -> Decimal {
        self.spot_usdc
            .as_ref()
            .map(|spot| spot.total)
            .unwrap_or(perp_account.equity)
    }

    fn apply_to(&self, account: &mut HlAccountState) {
        account.equity = self.total_collateral(account);
    }

    fn available_collateral(&self, perp_account: &HlAccountState) -> Decimal {
        self.spot_usdc
            .as_ref()
            .map(|spot| spot.available_after_maintenance)
            .unwrap_or_else(|| (perp_account.equity - perp_account.margin_used).max(Decimal::ZERO))
    }
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
        journal: std::sync::Arc<RunJournal>,
    ) -> Result<std::sync::Arc<Self>, HlBrokerError> {
        if config.default_market_slippage_bps >= 10_000
            || config.default_close_slippage_bps >= 10_000
        {
            let error = HlBrokerError::InvalidRequest {
                message: "default slippage must be less than 10000 bps".to_string(),
            };
            Self::record_connection_error(&journal, &config.strategy_id, "validate_config", &error);
            return Err(error);
        }
        let client = HyperliquidRestClient::new(config.network.rest_url());
        let user = format!("{:#x}", config.account_address);
        let signer_address = format!("{:#x}", signer.wallet_address());
        let agent_owner = client.agent_owner(&signer_address).await.map_err(|error| {
            Self::connection_error(&journal, &config.strategy_id, "agent_owner", error)
        })?;
        if agent_owner != config.account_address {
            let error = HlBrokerError::InvalidRequest {
                message: format!(
                    "API wallet {signer_address} is authorized for {agent_owner:#x}, not \
                     configured account {user}"
                ),
            };
            Self::record_connection_error(&journal, &config.strategy_id, "agent_owner", &error);
            return Err(error);
        }
        let metadata = client.meta().await.map_err(|error| {
            Self::connection_error(&journal, &config.strategy_id, "metadata", error)
        })?;
        let mids = client.all_mids().await.map_err(|error| {
            Self::connection_error(&journal, &config.strategy_id, "mids", error)
        })?;
        let abstraction = client.user_abstraction(&user).await.map_err(|error| {
            Self::connection_error(&journal, &config.strategy_id, "user_abstraction", error)
        })?;
        if abstraction == HlUserAbstraction::DexAbstraction {
            let error = HlBrokerError::InvalidRequest {
                message: "Hyperliquid dexAbstraction account funding is unsupported".to_string(),
            };
            Self::record_connection_error(
                &journal,
                &config.strategy_id,
                "user_abstraction",
                &error,
            );
            return Err(error);
        }
        let mut account = client.clearinghouse_state(&user).await.map_err(|error| {
            Self::connection_error(&journal, &config.strategy_id, "account", error)
        })?;
        let perp_account_value = account.equity;
        let spot_usdc = if abstraction.uses_spot_collateral() {
            Some(
                client
                    .spot_clearinghouse_state(&user)
                    .await
                    .map_err(|error| {
                        Self::connection_error(&journal, &config.strategy_id, "spot_account", error)
                    })?,
            )
        } else {
            None
        };
        let collateral = HlCollateralState {
            abstraction,
            spot_usdc,
        };
        collateral.apply_to(&mut account);
        let open_orders = client.open_orders(&user).await.map_err(|error| {
            Self::connection_error(&journal, &config.strategy_id, "open_orders", error)
        })?;
        let (ws, events) = HyperliquidWsClient::connect(config.network.ws_url())
            .await
            .map_err(|error| {
                Self::connection_error(&journal, &config.strategy_id, "websocket", error)
            })?;
        let markets = config
            .markets
            .iter()
            .map(|market| resolve_market_config(market, &metadata, config.default_margin_mode))
            .collect::<Result<Vec<_>, _>>()?;
        let broker = std::sync::Arc::new(Self::from_parts(
            config.strategy_id,
            journal,
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
            collateral,
            config.freshness_max_age,
            markets
                .iter()
                .cloned()
                .map(|market| (market.coin.clone(), market))
                .collect(),
            config.default_margin_mode,
            config.default_market_slippage_bps,
            config.default_close_slippage_bps,
        ));
        broker.record_audit(
            AuditAction::Connect,
            None,
            broker.collateral_audit_data(perp_account_value),
        );
        broker.record_equity_snapshot();

        broker.ws.subscribe_all_mids().await.map_err(|error| {
            Self::connection_error(
                &broker.journal,
                &broker.strategy_id,
                "subscribe_all_mids",
                error,
            )
        })?;
        broker
            .ws
            .subscribe_account_channels(&user)
            .await
            .map_err(|error| {
                Self::connection_error(
                    &broker.journal,
                    &broker.strategy_id,
                    "subscribe_account",
                    error,
                )
            })?;
        let events = broker
            .wait_for_subscriptions(events, config.connect_timeout)
            .await
            .map_err(|error| {
                broker.record_audit(
                    AuditAction::Connect,
                    None,
                    serde_json::json!({"stage": "subscription_confirmation", "error": Self::audit_error(&error)}),
                );
                error
        })?;
        for market in &markets {
            broker.set_leverage(market).await.map_err(|error| {
                broker.record_audit(
                    AuditAction::Connect,
                    Some(market.coin.0.clone()),
                    serde_json::json!({"stage": "set_leverage", "error": Self::audit_error(&error)}),
                );
                error
            })?;
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
        journal: std::sync::Arc<RunJournal>,
        state: HlBrokerState,
        risk_guard: RiskGuard,
        client: HyperliquidRestClient,
        metadata: HlMetadataSnapshot,
        mids: HlMidSnapshot,
        signer: std::sync::Arc<HyperliquidSigner>,
        ws: HyperliquidWsClient,
        network: HlNetwork,
        account_address: String,
        collateral: HlCollateralState,
        freshness_max_age: std::time::Duration,
        markets: HashMap<HlCoin, HlMarketConfig>,
        default_margin_mode: HlMarginMode,
        default_market_slippage_bps: u32,
        default_close_slippage_bps: u32,
    ) -> Self {
        let mids_updated_at = mids.updated_at;
        let (ws_ready, _) = watch::channel(false);
        Self {
            strategy_id,
            journal,
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
            collateral: RwLock::new(collateral),
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
            markets: Mutex::new(markets),
            market_locks: Mutex::new(HashMap::new()),
            default_margin_mode,
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
                            message: "websocket subscription error".to_string(),
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
        self.ws_ready.send_replace(true);
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
                        broker.mark_ws_unavailable();
                    }
                    HyperliquidWsEvent::Disconnected => {
                        broker.record_audit(
                            AuditAction::WebSocket,
                            None,
                            serde_json::json!({
                                "target": "connection",
                                "error": {"outcome": "failed", "error": "websocket disconnected"},
                            }),
                        );
                        broker.mark_ws_unavailable();
                    }
                    HyperliquidWsEvent::Message(message) => {
                        if let Err(error) = broker.apply_ws_event(message.clone()) {
                            let channel = message
                                .get("channel")
                                .and_then(Value::as_str)
                                .unwrap_or("unknown");
                            broker.record_audit(
                                AuditAction::WebSocket,
                                None,
                                serde_json::json!({
                                    "target": "event_processing",
                                    "channel": channel,
                                    "error": Self::audit_error(&error),
                                }),
                            );
                            let _ = broker.invalidate_event_freshness(&message);
                        }
                        broker.confirm_ws_recovery(&message);
                    }
                }
            }
            broker.record_audit(
                AuditAction::WebSocket,
                None,
                serde_json::json!({
                    "target": "event_channel",
                    "error": {"outcome": "failed", "error": "websocket event channel closed"},
                }),
            );
            broker.mark_ws_unavailable();
        });
    }

    fn mark_ws_unavailable(&self) {
        self.ws_ready.send_replace(false);
        if let Ok(mut subscriptions) = self.subscriptions.lock() {
            subscriptions.clear();
        }
        let _ = self.mark_fresh(|freshness| {
            freshness.mids = None;
            freshness.account = None;
            freshness.open_orders = None;
        });
    }

    fn record_audit(&self, action: AuditAction, symbol: Option<String>, data: serde_json::Value) {
        Self::record_audit_to_journal(&self.journal, &self.strategy_id, action, symbol, data);
    }

    fn order_audit_context(
        &self,
        request: &HlOrderRequest,
        client_order_id: &HlClientOrderId,
        submitted: Option<&HlSubmittedOrder>,
    ) -> Value {
        order_audit_context(
            self.network,
            format!("{:#x}", self.signer.wallet_address()),
            &self.account_address,
            request,
            client_order_id,
            submitted,
        )
    }

    fn record_ledger(&self, event_id: String, timestamp: Timestamp, event: LedgerEventKind) {
        self.journal.record_ledger(&LedgerEvent {
            event_id,
            strategy_id: self.strategy_id.clone(),
            mode: RunMode::Live,
            exchange: "hyperliquid".to_string(),
            timestamp,
            event,
        });
    }

    fn record_equity_snapshot(&self) {
        let Ok(account) = self.state.read().map(|state| state.account.clone()) else {
            self.record_invalid_ledger_event("equity_snapshot", "account state is unavailable");
            return;
        };
        self.record_ledger(
            format!("hl-equity-{}", account.updated_at.timestamp_millis()),
            account.updated_at,
            LedgerEventKind::EquitySnapshot {
                equity: account.equity,
                margin_used: account.margin_used,
                realized_pnl: None,
                unrealized_pnl: None,
                trading_fees: None,
                funding_pnl: None,
            },
        );
    }

    fn record_fill(&self, fill: &HlUserFill) {
        let order = self.tracked_order_for_fill(fill);
        let client_order_id = fill.client_order_id.clone().or_else(|| {
            order
                .as_ref()
                .map(|(client_order_id, _)| client_order_id.as_str().to_string())
        });
        let reduce_only = order.map(|(_, order)| order.submitted.reduce_only);
        match fill_ledger_event(fill, client_order_id, reduce_only) {
            Ok((event_id, timestamp, event)) => self.record_ledger(event_id, timestamp, event),
            Err(error) => self.record_invalid_ledger_event("fill", error),
        }
    }

    fn tracked_order_for_fill(
        &self,
        fill: &HlUserFill,
    ) -> Option<(HlClientOrderId, HlTrackedOrder)> {
        let orders = self.orders.lock().ok()?;
        if let Some(client_order_id) = fill.client_order_id.as_deref() {
            let client_order_id = HlClientOrderId::new(client_order_id).ok()?;
            return orders
                .get(&client_order_id)
                .map(|order| (client_order_id, order.clone()));
        }
        let order_id = fill.order_id.as_deref()?;
        orders
            .iter()
            .find(|(_, order)| order.order_id.as_deref() == Some(order_id))
            .map(|(client_order_id, order)| (client_order_id.clone(), order.clone()))
    }

    fn record_funding(&self, value: &Value) {
        let Some(symbol) = string_field(value, &["coin"]) else {
            self.record_invalid_ledger_event("funding", "missing coin");
            return;
        };
        let (Some(cashflow), Some(timestamp)) = (
            decimal_field(value, &["usdc"]),
            timestamp_field(value, &["time"]),
        ) else {
            self.record_invalid_ledger_event("funding", "missing cashflow or timestamp");
            return;
        };
        self.record_ledger(
            format!("hl-funding-{}-{symbol}", timestamp.timestamp_millis()),
            timestamp,
            LedgerEventKind::Funding {
                symbol,
                funding_rate: decimal_field(value, &["fundingRate"]),
                settlement_price: None,
                cashflow,
            },
        );
    }

    fn record_liquidations(&self, value: &Value) {
        let Some(delta) = value.get("delta") else {
            return;
        };
        if delta.get("type").and_then(Value::as_str) != Some("liquidation") {
            return;
        }
        let Some(timestamp) = timestamp_field(value, &["time"]) else {
            self.record_invalid_ledger_event("liquidation", "missing timestamp");
            return;
        };
        let Some(source) = string_field(value, &["hash"]) else {
            self.record_invalid_ledger_event("liquidation", "missing source hash");
            return;
        };
        let Some(positions) = delta.get("liquidatedPositions").and_then(Value::as_array) else {
            self.record_invalid_ledger_event("liquidation", "missing liquidated positions");
            return;
        };
        if positions.is_empty() {
            self.record_invalid_ledger_event("liquidation", "missing liquidated positions");
            return;
        }
        for (index, position) in positions.iter().enumerate() {
            self.record_ledger(
                format!(
                    "hl-liquidation-{}-{source}-{index}",
                    timestamp.timestamp_millis()
                ),
                timestamp,
                LedgerEventKind::Liquidation {
                    symbol: string_field(position, &["coin"]),
                    size: decimal_field(position, &["szi"]).map(|size| size.abs()),
                    price: None,
                    realized_pnl: None,
                    fee: None,
                    reason: Some("exchange_liquidation".to_string()),
                },
            );
        }
    }

    fn record_invalid_ledger_event(&self, target: &str, error: &str) {
        self.record_audit(
            AuditAction::WebSocket,
            None,
            serde_json::json!({
                "target": "ledger_event",
                "ledger_event": target,
                "error": {"outcome": "failed", "error": error},
            }),
        );
    }

    fn record_audit_to_journal(
        journal: &RunJournal,
        strategy_id: &StrategyId,
        action: AuditAction,
        symbol: Option<String>,
        data: serde_json::Value,
    ) {
        journal.record_audit(AuditRecord {
            strategy_id: strategy_id.clone(),
            mode: RunMode::Live,
            exchange: "hyperliquid".to_string(),
            symbol,
            action,
            data,
        });
    }

    fn record_connection_error(
        journal: &RunJournal,
        strategy_id: &StrategyId,
        stage: &str,
        error: &HlBrokerError,
    ) {
        Self::record_audit_to_journal(
            journal,
            strategy_id,
            AuditAction::Connect,
            None,
            serde_json::json!({"stage": stage, "error": Self::audit_error(error)}),
        );
    }

    fn connection_error(
        journal: &RunJournal,
        strategy_id: &StrategyId,
        stage: &str,
        error: impl std::fmt::Display,
    ) -> HlBrokerError {
        let error = transport_error(error);
        Self::record_connection_error(journal, strategy_id, stage, &error);
        error
    }

    fn audit_error(error: &HlBrokerError) -> serde_json::Value {
        let outcome = match error {
            HlBrokerError::RiskRejected { .. }
            | HlBrokerError::InvalidRequest { .. }
            | HlBrokerError::ExchangeRejected { .. } => "rejected",
            HlBrokerError::OutcomeUnknown { .. }
            | HlBrokerError::CancelOutcomeUnknown { .. }
            | HlBrokerError::OrderWaitTimeout { .. } => "outcome_unknown",
            _ => "failed",
        };
        let mut value = serde_json::json!({
            "outcome": outcome,
            "error": error.to_string(),
        });
        if let HlBrokerError::ExchangeRejected { raw, .. } = error {
            value["response"] = raw.clone();
        }
        if let HlBrokerError::RiskRejected { violations } = error {
            value["risk_decision"] = serde_json::json!({
                "Rejected": { "violations": violations },
            });
        }
        value
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
    }

    fn confirm_data_recovery(&self) -> Result<(), HlBrokerError> {
        if !self.trading_data_is_fresh()? || *self.ws_ready.borrow() {
            return Ok(());
        }
        self.ws_ready.send_replace(true);
        Ok(())
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
                let mut account = ws_clearinghouse_state(&message).map_err(transport_error)?;
                self.apply_collateral(&mut account)?;
                self.state
                    .write()
                    .map_err(|_| HlBrokerError::StateUnavailable)?
                    .account = account;
                self.record_equity_snapshot();
                self.mark_fresh(|freshness| freshness.account = Some(Utc::now()))?;
                self.confirm_data_recovery()?;
                Ok(())
            }
            Some("openOrders") => {
                let open_orders = ws_open_orders(&message).map_err(transport_error)?;
                self.state
                    .write()
                    .map_err(|_| HlBrokerError::StateUnavailable)?
                    .open_orders = open_orders;
                self.confirm_pending_open_orders()?;
                self.mark_fresh(|freshness| freshness.open_orders = Some(Utc::now()))?;
                self.confirm_data_recovery()?;
                Ok(())
            }
            Some("orderUpdates") => {
                for update in order_updates(&message) {
                    self.apply_order_update(&update)?;
                    push_bounded(&self.order_updates, update)?;
                }
                Ok(())
            }
            Some("userFills") => {
                for fill in user_fills(&message) {
                    self.record_fill(&fill);
                    push_bounded(&self.fills, fill)?;
                }
                Ok(())
            }
            Some("userFundings") => {
                for value in funding_events(&message) {
                    self.record_funding(&value);
                }
                Ok(())
            }
            Some("userNonFundingLedgerUpdates") => {
                for value in non_funding_ledger_events(&message) {
                    self.record_liquidations(&value);
                }
                Ok(())
            }
            Some("error") => {
                self.record_audit(
                    AuditAction::WebSocket,
                    None,
                    serde_json::json!({
                        "target": "exchange_message",
                        "error": {
                            "outcome": "failed",
                            "error": "received websocket error message",
                        },
                    }),
                );
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
            .and_then(|()| self.confirm_data_recovery())
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
        Ok(self.trading_state_stale_message()?.is_none())
    }

    fn trading_data_is_fresh(&self) -> Result<bool, HlBrokerError> {
        let freshness = self
            .freshness
            .read()
            .map_err(|_| HlBrokerError::StateUnavailable)?
            .clone();
        Ok(trading_data_is_fresh(
            self.is_fresh(freshness.mids),
            self.is_fresh(freshness.account),
            self.is_fresh(freshness.open_orders),
        ))
    }

    fn trading_state_stale_message(&self) -> Result<Option<String>, HlBrokerError> {
        let freshness = self
            .freshness
            .read()
            .map_err(|_| HlBrokerError::StateUnavailable)?
            .clone();
        Ok(stale_state_message(
            *self.ws_ready.borrow(),
            self.is_fresh(freshness.mids),
            self.is_fresh(freshness.account),
            self.is_fresh(freshness.open_orders),
        ))
    }

    pub async fn wait_until_trading_ready(&self) -> Result<(), HlBrokerError> {
        if self.trading_state_is_fresh()? {
            return Ok(());
        }
        tokio::time::timeout(RECOVERY_TIMEOUT, async {
            loop {
                if self.trading_state_is_fresh()? {
                    return Ok(());
                }
                tokio::time::sleep(RECOVERY_POLL_INTERVAL).await;
            }
        })
        .await
        .map_err(|_| HlBrokerError::StateStale {
            message: self
                .trading_state_stale_message()
                .ok()
                .flatten()
                .unwrap_or_else(|| "state could not be inspected".to_string()),
        })?
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
                    metadata_broker.record_audit(
                        AuditAction::ReconcileState,
                        None,
                        serde_json::json!({
                            "target": "metadata",
                            "error": Self::audit_error(&error),
                        }),
                    );
                }
            }
        });

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(reconciliation_interval);
            loop {
                ticker.tick().await;
                match self.refresh_account().await {
                    Ok((account, perp_account_value)) => {
                        if let Ok(mut state) = self.state.write() {
                            state.account = account;
                        }
                        self.record_audit(
                            AuditAction::ReconcileState,
                            None,
                            self.collateral_audit_data(perp_account_value),
                        );
                        self.record_equity_snapshot();
                        let _ = self.mark_fresh(|freshness| freshness.account = Some(Utc::now()));
                    }
                    Err(error) => {
                        self.record_audit(
                            AuditAction::ReconcileState,
                            None,
                            serde_json::json!({
                                "target": "account",
                                "error": Self::audit_error(&transport_error(error)),
                            }),
                        );
                        let _ = self.mark_fresh(|freshness| freshness.account = None);
                    }
                }
                match self.client.open_orders(&self.account_address).await {
                    Ok(open_orders) => {
                        if let Ok(mut state) = self.state.write() {
                            state.open_orders = open_orders;
                        }
                        let _ = self.confirm_pending_open_orders();
                        if let Err(error) = self.reconcile_unknown_orders().await {
                            self.record_audit(
                                AuditAction::ReconcileState,
                                None,
                                serde_json::json!({
                                    "target": "unknown_orders",
                                    "error": Self::audit_error(&error),
                                }),
                            );
                        }
                        if let Err(error) = self.reconcile_pending_cancels().await {
                            self.record_audit(
                                AuditAction::ReconcileState,
                                None,
                                serde_json::json!({
                                    "target": "pending_cancels",
                                    "error": Self::audit_error(&error),
                                }),
                            );
                        }
                        let _ =
                            self.mark_fresh(|freshness| freshness.open_orders = Some(Utc::now()));
                    }
                    Err(error) => {
                        self.record_audit(
                            AuditAction::ReconcileState,
                            None,
                            serde_json::json!({
                                "target": "open_orders",
                                "error": Self::audit_error(&transport_error(error)),
                            }),
                        );
                        let _ = self.mark_fresh(|freshness| freshness.open_orders = None);
                    }
                }
            }
        })
    }

    async fn refresh_account(&self) -> Result<(HlAccountState, Decimal), HlBrokerError> {
        let mut account = self
            .client
            .clearinghouse_state(&self.account_address)
            .await
            .map_err(transport_error)?;
        let perp_account_value = account.equity;
        let uses_spot_collateral = self
            .collateral
            .read()
            .map_err(|_| HlBrokerError::StateUnavailable)?
            .abstraction
            .uses_spot_collateral();
        if uses_spot_collateral {
            let spot_usdc = self
                .client
                .spot_clearinghouse_state(&self.account_address)
                .await
                .map_err(transport_error)?;
            self.collateral
                .write()
                .map_err(|_| HlBrokerError::StateUnavailable)?
                .spot_usdc = Some(spot_usdc);
        }
        self.apply_collateral(&mut account)?;
        Ok((account, perp_account_value))
    }

    fn apply_collateral(&self, account: &mut HlAccountState) -> Result<(), HlBrokerError> {
        self.collateral
            .read()
            .map_err(|_| HlBrokerError::StateUnavailable)?
            .apply_to(account);
        Ok(())
    }

    fn collateral_audit_data(&self, perp_account_value: Decimal) -> Value {
        let collateral = match self.collateral.read() {
            Ok(collateral) => collateral,
            Err(_) => return serde_json::json!({"collateral": "state_unavailable"}),
        };
        let spot_usdc = collateral.spot_usdc.as_ref();
        let margin_used = self
            .state
            .read()
            .map(|state| state.account.margin_used)
            .unwrap_or(Decimal::ZERO);
        let total_collateral = spot_usdc
            .map(|spot| spot.total)
            .unwrap_or(perp_account_value);
        let available_collateral = spot_usdc
            .map(|spot| spot.available_after_maintenance)
            .unwrap_or_else(|| (perp_account_value - margin_used).max(Decimal::ZERO));
        serde_json::json!({
            "account_abstraction": collateral.abstraction.name(),
            "equity_source": if collateral.abstraction.uses_spot_collateral() {
                "spot_usdc_total"
            } else {
                "perp_account_value"
            },
            "spot_usdc_total": spot_usdc.map(|spot| spot.total.to_string()),
            "spot_usdc_hold": spot_usdc.map(|spot| spot.hold.to_string()),
            "spot_usdc_available_after_maintenance": spot_usdc
                .map(|spot| spot.available_after_maintenance.to_string()),
            "perp_account_value": perp_account_value.to_string(),
            "total_collateral": total_collateral.to_string(),
            "available_collateral": available_collateral.to_string(),
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
        let result = tokio::time::timeout(timeout, async {
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
        });
        if let Err(error @ HlBrokerError::OrderWaitTimeout { .. }) = &result {
            let symbol = self
                .order(client_order_id)
                .map(|order| order.submitted.coin.0);
            self.record_audit(
                AuditAction::PlaceOrder,
                symbol,
                serde_json::json!({
                    "client_order_id": client_order_id.as_str(),
                    "wait_timeout_ms": timeout.as_millis(),
                    "error": Self::audit_error(error),
                }),
            );
        }
        result?
    }

    pub async fn set_leverage(&self, market: &HlMarketConfig) -> Result<(), HlBrokerError> {
        let resolved_market = self
            .metadata
            .read()
            .map_err(|_| HlBrokerError::StateUnavailable)
            .and_then(|metadata| {
                resolve_market_config(market, &metadata, self.default_margin_mode)
            });
        let result = match resolved_market {
            Ok(market) => self.set_leverage_inner(&market).await.map(|()| market),
            Err(error) => Err(error),
        };
        if let Ok(resolved_market) = &result {
            if let Ok(mut markets) = self.markets.lock() {
                markets.insert(market.coin.clone(), resolved_market.clone());
            }
        }
        let audit_market = result.as_ref().ok().unwrap_or(market);
        let margin_mode = audit_market
            .margin_mode
            .unwrap_or(self.default_margin_mode)
            .name();
        let data = match &result {
            Ok(_) => serde_json::json!({
                "outcome": "accepted",
                "leverage": audit_market.leverage,
                "margin_mode": margin_mode,
                "is_cross": audit_market
                    .margin_mode
                    .unwrap_or(self.default_margin_mode)
                    .is_cross(),
            }),
            Err(error) => serde_json::json!({
                "leverage": market.leverage,
                "margin_mode": margin_mode,
                "error": Self::audit_error(error),
            }),
        };
        self.record_audit(AuditAction::SetLeverage, Some(market.coin.0.clone()), data);
        result.map(|_| ())
    }

    async fn set_leverage_inner(&self, market: &HlMarketConfig) -> Result<(), HlBrokerError> {
        if market.leverage == 0 {
            return Err(HlBrokerError::InvalidRequest {
                message: "leverage must be positive".to_string(),
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
        let margin_mode = resolve_margin_mode(
            market.margin_mode.unwrap_or(self.default_margin_mode),
            asset.only_isolated,
            &market.coin,
        )?;
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
            is_cross: margin_mode.is_cross(),
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
        parse_default_action_response(&raw)
            .map_err(|message| HlBrokerError::ExchangeRejected { message, raw })
    }

    fn market_lock(
        &self,
        coin: &HlCoin,
    ) -> Result<std::sync::Arc<tokio::sync::Mutex<()>>, HlBrokerError> {
        let mut locks = self
            .market_locks
            .lock()
            .map_err(|_| HlBrokerError::StateUnavailable)?;
        Ok(locks
            .entry(coin.clone())
            .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(())))
            .clone())
    }

    async fn ensure_leverage(&self, coin: &HlCoin, leverage: u32) -> Result<(), HlBrokerError> {
        let configured = self
            .markets
            .lock()
            .map_err(|_| HlBrokerError::StateUnavailable)?
            .get(coin)
            .cloned();
        if configured
            .as_ref()
            .is_some_and(|market| market.leverage == leverage)
        {
            return Ok(());
        }
        let market = HlMarketConfig {
            coin: coin.clone(),
            leverage,
            margin_mode: Some(
                configured
                    .and_then(|market| market.margin_mode)
                    .unwrap_or(self.default_margin_mode),
            ),
        };
        self.set_leverage(&market).await?;
        Ok(())
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
                // 订单可能在调用方开始等待前已终态，必须保留最新状态。
                sender.send_replace(order);
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
            if self.order(&client_order_id).is_some() {
                self.update_order_state(&client_order_id, state, update.order_id.clone(), None);
            } else {
                self.record_audit(
                    AuditAction::WebSocket,
                    None,
                    serde_json::json!({
                        "target": "order_update_association",
                        "client_order_id": client_order_id.as_str(),
                        "order_id": update.order_id,
                        "status": update.status,
                        "error": {
                            "outcome": "failed",
                            "error": "order update did not match a locally tracked order",
                        },
                    }),
                );
            }
        } else if update.status.is_some() {
            self.record_audit(
                AuditAction::WebSocket,
                None,
                serde_json::json!({
                    "target": "order_update_state",
                    "client_order_id": client_order_id.as_str(),
                    "order_id": update.order_id,
                    "status": update.status,
                    "error": {
                        "outcome": "failed",
                        "error": "unrecognized order update status",
                    },
                }),
            );
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
            match self.reconcile_order(&client_order_id).await {
                Ok(raw) => {
                    if let Some(status) = order_status_from_response(&raw)
                        .as_deref()
                        .and_then(order_update_state)
                    {
                        self.update_order_state(&client_order_id, status, None, None);
                    } else {
                        self.record_audit(
                            AuditAction::ReconcileState,
                            None,
                            serde_json::json!({
                                "target": "unknown_order",
                                "client_order_id": client_order_id.as_str(),
                                "error": {
                                    "outcome": "failed",
                                    "error": "order status response did not contain a recognized state",
                                },
                            }),
                        );
                    }
                }
                Err(error) => {
                    self.record_audit(
                        AuditAction::ReconcileState,
                        None,
                        serde_json::json!({
                            "target": "unknown_order",
                            "client_order_id": client_order_id.as_str(),
                            "error": Self::audit_error(&error),
                        }),
                    );
                }
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
            let raw = match self.order_status(&target).await {
                Ok(raw) => raw,
                Err(error) => {
                    self.record_audit(
                        AuditAction::ReconcileState,
                        None,
                        serde_json::json!({
                            "target": "pending_cancel",
                            "order_target": target,
                            "error": Self::audit_error(&error),
                        }),
                    );
                    continue;
                }
            };
            let Some(state) = order_status_from_response(&raw)
                .as_deref()
                .and_then(order_update_state)
            else {
                self.record_audit(
                    AuditAction::ReconcileState,
                    None,
                    serde_json::json!({
                        "target": "pending_cancel",
                        "order_target": target,
                        "error": {
                            "outcome": "failed",
                            "error": "order status response did not contain a recognized state",
                        },
                    }),
                );
                continue;
            };
            if let Some(client_order_id) = self.client_order_id_for_target(&target) {
                self.update_order_state(&client_order_id, state.clone(), None, None);
            } else {
                self.record_audit(
                    AuditAction::ReconcileState,
                    None,
                    serde_json::json!({
                        "target": "pending_cancel_association",
                        "order_target": target,
                        "error": {
                            "outcome": "failed",
                            "error": "order status did not match a locally tracked order",
                        },
                    }),
                );
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

fn ensure_minimum_order_notional(
    size: crate::core::Decimal,
    price: crate::core::Decimal,
) -> Result<(), HlBrokerError> {
    let minimum = crate::core::Decimal::from(10);
    let notional = size * price;
    if notional < minimum {
        return Err(HlBrokerError::InvalidRequest {
            message: format!("order notional {notional} is below Hyperliquid minimum {minimum}"),
        });
    }
    Ok(())
}

fn resolve_margin_fraction_size(
    total_collateral: Decimal,
    available_collateral: Decimal,
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
    let reserve_margin = total_collateral * reserve_fraction;
    let available_margin = (available_collateral - reserve_margin).max(Decimal::ZERO);
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

fn decimal_field(value: &Value, names: &[&str]) -> Option<Decimal> {
    names.iter().find_map(|name| {
        value.get(*name).and_then(|field| {
            field
                .as_str()
                .map(str::to_string)
                .or_else(|| field.is_number().then(|| field.to_string()))
                .and_then(|value| value.parse().ok())
        })
    })
}

fn timestamp_field(value: &Value, names: &[&str]) -> Option<Timestamp> {
    names.iter().find_map(|name| {
        value
            .get(*name)
            .and_then(|field| {
                field
                    .as_i64()
                    .or_else(|| field.as_u64().map(|value| value as i64))
            })
            .and_then(|millis| Utc.timestamp_millis_opt(millis).single())
    })
}

fn fill_ledger_event(
    fill: &HlUserFill,
    client_order_id: Option<String>,
    reduce_only: Option<bool>,
) -> Result<(String, Timestamp, LedgerEventKind), &'static str> {
    let symbol = fill.coin.clone().ok_or("missing coin")?;
    let size = fill.size.ok_or("missing required fill fields")?;
    let price = fill.price.ok_or("missing required fill fields")?;
    let side = fill.side.ok_or("missing required fill fields")?;
    let timestamp = fill.timestamp.ok_or("missing required fill fields")?;
    let trade_id = fill
        .trade_id
        .as_ref()
        .ok_or("missing required fill fields")?;
    Ok((
        format!(
            "hl-fill-{}-{symbol}-{trade_id}",
            timestamp.timestamp_millis()
        ),
        timestamp,
        LedgerEventKind::Fill {
            order_id: fill.order_id.clone(),
            client_order_id,
            symbol,
            side,
            size,
            price,
            fee: fill.fee,
            reduce_only,
        },
    ))
}

fn order_update_state(status: &str) -> Option<HlTrackedOrderState> {
    match status.to_ascii_lowercase().as_str() {
        "open" | "resting" => Some(HlTrackedOrderState::Open),
        "filled" => Some(HlTrackedOrderState::Filled),
        "canceled" | "margincanceled" | "selftradecanceled" | "delistedcanceled" => {
            Some(HlTrackedOrderState::Canceled)
        }
        "rejected"
        | "mintradentlrejected"
        | "perpmarginrejected"
        | "reduceonlyrejected"
        | "badalopxrejected"
        | "ioccancelrejected"
        | "marketordernoliquidityrejected"
        | "oraclerejected" => Some(HlTrackedOrderState::Rejected),
        "expired" => Some(HlTrackedOrderState::Expired),
        _ => None,
    }
}

impl HyperliquidLiveBroker {
    async fn place_order_inner(
        &self,
        mut request: HlOrderRequest,
    ) -> Result<HlOrderResult, HlBrokerError> {
        request
            .validate()
            .map_err(|message| HlBrokerError::InvalidRequest { message })?;
        self.wait_until_trading_ready().await?;
        let _market_guard = if request.reduce_only {
            None
        } else {
            Some(self.market_lock(&request.coin)?.lock_owned().await)
        };
        if !request.reduce_only {
            let leverage = request
                .leverage
                .ok_or_else(|| HlBrokerError::InvalidRequest {
                    message: "opening orders require leverage".to_string(),
                })?;
            self.ensure_leverage(&request.coin, leverage).await?;
        }
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
        let mut sizing_audit = None;
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
                let markets = self
                    .markets
                    .lock()
                    .map_err(|_| HlBrokerError::StateUnavailable)?;
                let market =
                    markets
                        .get(&request.coin)
                        .ok_or_else(|| HlBrokerError::InvalidRequest {
                            message: format!(
                                "margin-fraction sizing requires configured leverage for {}",
                                request.coin.0
                            ),
                        })?;
                let account = self.account_state()?;
                let available_collateral = self
                    .collateral
                    .read()
                    .map_err(|_| HlBrokerError::StateUnavailable)?
                    .available_collateral(&account);
                let reserve_margin = account.equity * reserve_fraction;
                let available_after_reserve =
                    (available_collateral - reserve_margin).max(Decimal::ZERO);
                let planned_margin = available_after_reserve * margin_fraction;
                let planned_notional = planned_margin * Decimal::from(market.leverage);
                sizing_audit = Some(serde_json::json!({
                    "total_collateral": account.equity.to_string(),
                    "available_collateral": available_collateral.to_string(),
                    "reserve_base": account.equity.to_string(),
                    "reserve_fraction": reserve_fraction.to_string(),
                    "reserve_amount": reserve_margin.to_string(),
                    "available_after_reserve": available_after_reserve.to_string(),
                    "margin_fraction": margin_fraction.to_string(),
                    "planned_margin": planned_margin.to_string(),
                    "planned_notional": planned_notional.to_string(),
                }));
                resolve_margin_fraction_size(
                    account.equity,
                    available_collateral,
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
        if !request.reduce_only {
            ensure_minimum_order_notional(normalized_size, price)?;
        }
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
        let action = request.to_order_action(asset, price, normalized_size);
        self.register_order(submitted.clone())?;
        let mut audit_data = self.order_audit_context(&request, &client_order_id, Some(&submitted));
        if let Some(sizing_audit) = sizing_audit {
            audit_data["sizing"] = sizing_audit;
        }
        audit_data["stage"] = serde_json::json!("submit_attempt");
        audit_data["wire_action"] = action.to_hyperliquid_json();
        self.record_audit(
            AuditAction::PlaceOrder,
            Some(request.coin.0.clone()),
            audit_data,
        );
        let signer = &self.signer;
        let ws = &self.ws;
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
        let result = match parse_order_outcome(&raw) {
            Ok(outcome) => HlOrderResult {
                submitted,
                outcome,
                raw,
            },
            Err(message) => {
                self.update_order_state(
                    &client_order_id,
                    HlTrackedOrderState::Rejected,
                    None,
                    None,
                );
                return Err(HlBrokerError::ExchangeRejected { message, raw });
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

    async fn cancel_order_inner(
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
        let result = parse_cancel_response(raw.clone())
            .map_err(|message| HlBrokerError::ExchangeRejected { message, raw });
        if let Ok(mut pending_cancels) = self.pending_cancels.lock() {
            pending_cancels.remove(&target);
        }
        result
    }

    async fn close_position_inner(
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

        self.place_order_inner(HlOrderRequest {
            coin: request.coin,
            side: if position.size.is_sign_positive() {
                crate::core::Side::Sell
            } else {
                crate::core::Side::Buy
            },
            size: HlOrderSize::Exact(size),
            leverage: None,
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

    async fn place_order(&self, request: HlOrderRequest) -> Result<HlOrderResult, HlBrokerError> {
        let mut request = request;
        if request.client_order_id.is_none() {
            request.client_order_id = Some(self.next_client_order_id());
        }
        let client_order_id = request
            .client_order_id
            .clone()
            .expect("client order id generated above");
        let symbol = Some(request.coin.0.clone());
        let result = self.place_order_inner(request.clone()).await;
        let submitted = result
            .as_ref()
            .ok()
            .map(|result| result.submitted.clone())
            .or_else(|| self.order(&client_order_id).map(|order| order.submitted));
        let mut data = self.order_audit_context(&request, &client_order_id, submitted.as_ref());
        data["stage"] = serde_json::json!("submit_result");
        match &result {
            Ok(result) => {
                data["outcome"] = serde_json::json!("accepted");
                data["exchange_status"] = serde_json::json!(match &result.outcome {
                    HlOrderOutcome::Resting { .. } => "resting",
                    HlOrderOutcome::Filled { .. } => "filled",
                });
                data["response"] = result.raw.clone();
            }
            Err(error) => {
                let audit_error = Self::audit_error(error);
                data["exchange_status"] = audit_error["outcome"].clone();
                data["error"] = audit_error;
            }
        }
        self.record_audit(AuditAction::PlaceOrder, symbol, data);
        result
    }

    async fn cancel_order(
        &self,
        request: HlCancelRequest,
    ) -> Result<HlCancelResponse, HlBrokerError> {
        let request_data = serde_json::to_value(&request).unwrap_or(serde_json::Value::Null);
        let symbol = Some(request.coin.0.clone());
        let result = self.cancel_order_inner(request).await;
        let data = match &result {
            Ok(result) => serde_json::json!({
                "request": request_data,
                "outcome": "accepted",
                "response": result.raw,
            }),
            Err(error) => serde_json::json!({
                "request": request_data,
                "error": Self::audit_error(error),
            }),
        };
        self.record_audit(AuditAction::CancelOrder, symbol, data);
        result
    }

    async fn close_position(
        &self,
        request: HlCloseRequest,
    ) -> Result<HlOrderResult, HlBrokerError> {
        let request_data = serde_json::to_value(&request).unwrap_or(serde_json::Value::Null);
        let symbol = Some(request.coin.0.clone());
        let result = self.close_position_inner(request).await;
        let data = match &result {
            Ok(result) => serde_json::json!({
                "request": request_data,
                "outcome": "accepted",
                "client_order_id": result.submitted.client_order_id.as_str(),
                "response": result.raw,
            }),
            Err(error) => serde_json::json!({
                "request": request_data,
                "error": Self::audit_error(error),
            }),
        };
        self.record_audit(AuditAction::ClosePosition, symbol, data);
        result
    }
}

fn stale_state_message(
    ws_ready: bool,
    mids_fresh: bool,
    account_fresh: bool,
    open_orders_fresh: bool,
) -> Option<String> {
    let mut stale = Vec::new();
    if !ws_ready {
        stale.push("websocket subscriptions");
    }
    if !mids_fresh {
        stale.push("market mids");
    }
    if !account_fresh {
        stale.push("account state");
    }
    if !open_orders_fresh {
        stale.push("open orders");
    }
    (!stale.is_empty()).then(|| stale.join(", "))
}

fn order_audit_context(
    network: HlNetwork,
    signer_address: String,
    account_address: &str,
    request: &HlOrderRequest,
    client_order_id: &HlClientOrderId,
    submitted: Option<&HlSubmittedOrder>,
) -> Value {
    serde_json::json!({
        "network": network.name(),
        "signer_address": signer_address,
        "account_address": account_address,
        "client_order_id": client_order_id.as_str(),
        "request": request,
        "submitted": submitted.map(|order| serde_json::json!({
            "symbol": order.coin.0.clone(),
            "side": order.side,
            "size": order.size,
            "price": order.limit_price,
            "reduce_only": order.reduce_only,
        })),
    })
}

fn trading_data_is_fresh(mids_fresh: bool, account_fresh: bool, open_orders_fresh: bool) -> bool {
    mids_fresh && account_fresh && open_orders_fresh
}

fn resolve_market_config(
    market: &HlMarketConfig,
    metadata: &HlMetadataSnapshot,
    default_margin_mode: HlMarginMode,
) -> Result<HlMarketConfig, HlBrokerError> {
    let asset = metadata
        .asset(&market.coin)
        .ok_or_else(|| HlBrokerError::InvalidRequest {
            message: format!("unknown Hyperliquid coin {}", market.coin.0),
        })?;
    Ok(HlMarketConfig {
        coin: market.coin.clone(),
        leverage: market.leverage,
        margin_mode: Some(resolve_margin_mode(
            market.margin_mode.unwrap_or(default_margin_mode),
            asset.only_isolated,
            &market.coin,
        )?),
    })
}

fn resolve_margin_mode(
    requested: HlMarginMode,
    only_isolated: bool,
    coin: &HlCoin,
) -> Result<HlMarginMode, HlBrokerError> {
    match (requested, only_isolated) {
        (HlMarginMode::Auto, true) => Ok(HlMarginMode::Isolated),
        (HlMarginMode::Auto, false) => Ok(HlMarginMode::Cross),
        (HlMarginMode::Cross, true) => Err(HlBrokerError::InvalidRequest {
            message: format!("{} only supports isolated margin", coin.0),
        }),
        (mode, _) => Ok(mode),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::JournalId;
    use crate::core::Side;
    use crate::hyperliquid::client::ws::{
        event_values, funding_events, non_funding_ledger_events, parse_order_outcome,
        parse_order_update, user_fills,
    };
    use crate::hyperliquid::types::HlCancelStatus;
    use crate::storage::{MemoryAuditSink, MemoryLedgerSink};

    #[test]
    fn records_connection_errors_with_stage_and_outcome() {
        let audit = MemoryAuditSink::new();
        let reader = audit.clone();
        let journal = RunJournal::new(JournalId::new("live-1"), MemoryLedgerSink::new())
            .with_audit_sink(audit);
        let error = HlBrokerError::Transport {
            message: "connection refused".to_string(),
        };

        HyperliquidLiveBroker::record_connection_error(
            &journal,
            &StrategyId::new("strategy-1"),
            "websocket",
            &error,
        );

        let events = reader.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].record.action, AuditAction::Connect);
        assert_eq!(events[0].record.data["stage"], "websocket");
        assert_eq!(events[0].record.data["error"]["outcome"], "failed");
        assert_eq!(
            events[0].record.data["error"]["error"],
            "transport failed: connection refused"
        );
    }

    #[test]
    fn classifies_order_wait_timeout_as_unknown_for_audit() {
        let error = HlBrokerError::OrderWaitTimeout {
            client_order_id: HlClientOrderId::new("0x00000000000000000000000000000001").unwrap(),
        };

        assert_eq!(
            HyperliquidLiveBroker::audit_error(&error)["outcome"],
            "outcome_unknown"
        );
    }

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
        assert_eq!(config.default_margin_mode, HlMarginMode::Auto);
        assert_eq!(config.default_market_slippage_bps, 100);
        assert_eq!(config.default_close_slippage_bps, 100);
    }

    #[test]
    fn auto_selects_isolated_for_isolated_only_market() {
        let coin = HlCoin::new("ETH");
        assert_eq!(
            resolve_margin_mode(HlMarginMode::Auto, true, &coin).unwrap(),
            HlMarginMode::Isolated
        );
    }

    #[test]
    fn auto_selects_cross_for_market_supporting_cross_margin() {
        let coin = HlCoin::new("BTC");
        assert_eq!(
            resolve_margin_mode(HlMarginMode::Auto, false, &coin).unwrap(),
            HlMarginMode::Cross
        );
    }

    #[test]
    fn rejects_explicit_cross_for_isolated_only_market() {
        let coin = HlCoin::new("ETH");
        let error = resolve_margin_mode(HlMarginMode::Cross, true, &coin).unwrap_err();

        assert!(matches!(
            error,
            HlBrokerError::InvalidRequest { message }
                if message == "ETH only supports isolated margin"
        ));
    }

    #[test]
    fn keeps_explicit_isolated_mode() {
        let coin = HlCoin::new("BTC");
        assert_eq!(
            resolve_margin_mode(HlMarginMode::Isolated, false, &coin).unwrap(),
            HlMarginMode::Isolated
        );
    }

    #[test]
    fn order_audit_context_records_replayable_order_identity_without_signature_material() {
        let client_order_id = HlClientOrderId::new("0x00000000000000000000000000000001").unwrap();
        let request = HlOrderRequest {
            coin: HlCoin::new("DOGE"),
            side: Side::Buy,
            size: HlOrderSize::Exact("12.3456".parse().unwrap()),
            leverage: Some(5),
            reduce_only: false,
            order_type: HlOrderType::Limit {
                limit_price: "0.123456".parse().unwrap(),
                tif: crate::hyperliquid::types::HlTimeInForce::Ioc,
            },
            client_order_id: Some(client_order_id.clone()),
            expires_after: None,
        };
        let submitted = HlSubmittedOrder {
            coin: HlCoin::new("DOGE"),
            side: Side::Buy,
            size: "12.34".parse().unwrap(),
            limit_price: "0.12346".parse().unwrap(),
            reduce_only: false,
            client_order_id: client_order_id.clone(),
        };

        let context = order_audit_context(
            HlNetwork::Mainnet,
            "0x0000000000000000000000000000000000000001".to_string(),
            "0x0000000000000000000000000000000000000002",
            &request,
            &client_order_id,
            Some(&submitted),
        );

        assert_eq!(context["network"], "mainnet");
        assert_eq!(context["client_order_id"], client_order_id.as_str());
        assert_eq!(
            context["signer_address"],
            "0x0000000000000000000000000000000000000001"
        );
        assert_eq!(context["submitted"]["symbol"], "DOGE");
        assert_eq!(context["submitted"]["size"], "12.34");
        assert_eq!(context["submitted"]["price"], "0.12346");
        assert!(context.get("nonce").is_none());
        assert!(context.get("signature").is_none());
    }

    #[test]
    fn identifies_each_stale_trading_state_component() {
        assert_eq!(stale_state_message(true, true, true, true), None);
        assert_eq!(
            stale_state_message(false, false, false, false).as_deref(),
            Some("websocket subscriptions, market mids, account state, open orders")
        );
        assert_eq!(
            stale_state_message(true, false, true, false).as_deref(),
            Some("market mids, open orders")
        );
    }

    #[test]
    fn reconnect_requires_all_trading_snapshots_but_not_order_event_channels() {
        assert!(!trading_data_is_fresh(false, true, true));
        assert!(!trading_data_is_fresh(true, false, true));
        assert!(!trading_data_is_fresh(true, true, false));
        assert!(trading_data_is_fresh(true, true, true));
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
        let result = parse_order_outcome(&raw)
            .map(|outcome| HlOrderResult {
                submitted: HlSubmittedOrder {
                    coin: HlCoin::new("ETH"),
                    side: crate::core::Side::Buy,
                    size: "0.02".parse().unwrap(),
                    limit_price: "1900".parse().unwrap(),
                    reduce_only: false,
                    client_order_id: HlClientOrderId::new("0x0123456789abcdef0123456789abcdef")
                        .unwrap(),
                },
                outcome,
                raw,
            })
            .unwrap();
        assert!(matches!(
            result.outcome,
            HlOrderOutcome::Filled { total_size, .. }
                if total_size == "0.02".parse().unwrap()
        ));
    }

    #[test]
    fn parses_direct_order_post_response_data() {
        let raw = serde_json::json!({
            "data": {
                "response": {
                    "payload": {
                        "data": {
                            "statuses": [{
                                "resting": {"oid": 42}
                            }]
                        }
                    }
                }
            }
        });
        let result = parse_order_outcome(&raw).unwrap();

        assert!(matches!(
            result,
            HlOrderOutcome::Resting { order_id } if order_id.0 == "42"
        ));
    }

    #[test]
    fn preserves_exchange_error_response_text() {
        let raw = serde_json::json!({
            "channel": "post",
            "data": {
                "response": {
                    "type": "action",
                    "payload": {
                        "status": "err",
                        "response": "User or API Wallet 0xdead does not exist."
                    }
                }
            }
        });
        let error = parse_order_outcome(&raw).unwrap_err();

        assert_eq!(error, "User or API Wallet 0xdead does not exist.");
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
    fn rejects_non_reduce_only_orders_below_minimum_notional() {
        let error =
            ensure_minimum_order_notional("0.41".parse().unwrap(), "22.935".parse().unwrap())
                .unwrap_err();

        assert!(matches!(
            error,
            HlBrokerError::InvalidRequest { message }
                if message == "order notional 9.40335 is below Hyperliquid minimum 10"
        ));
    }

    #[test]
    fn recognizes_minimum_notional_rejection_as_terminal() {
        assert_eq!(
            order_update_state("minTradeNtlRejected"),
            Some(HlTrackedOrderState::Rejected)
        );
    }

    #[test]
    fn structures_fill_event_and_keeps_raw_payload() {
        let message = serde_json::json!({
            "channel": "userFills",
            "data": {
                "fills": [{
                    "oid": 42,
                    "cloid": "0xabc",
                    "coin": "BTC",
                    "side": "B",
                    "sz": "0.1",
                    "px": "100.5",
                    "fee": "0.01",
                    "time": 1_700_000_000_000_i64,
                    "tid": 99
                }]
            }
        });
        let fill = user_fills(&message).remove(0);
        assert_eq!(fill.order_id.as_deref(), Some("42"));
        assert_eq!(fill.coin.as_deref(), Some("BTC"));
        assert_eq!(fill.size, Some("0.1".parse().unwrap()));
        assert_eq!(fill.side, Some(Side::Buy));
        assert_eq!(fill.trade_id.as_deref(), Some("99"));
        assert_eq!(fill.raw, message);
    }

    #[test]
    fn extracts_nested_fill_and_funding_events() {
        let fills = serde_json::json!({
            "data": {"fills": [{"coin": "BTC"}]}
        });
        let fundings = serde_json::json!({
            "data": {"fundings": [{"coin": "ETH"}]}
        });

        assert_eq!(user_fills(&fills).len(), 1);
        assert_eq!(funding_events(&fundings).len(), 1);
        assert_eq!(user_fills(&fills)[0].coin.as_deref(), Some("BTC"));
        assert_eq!(funding_events(&fundings)[0]["coin"], "ETH");

        let ledger_updates = serde_json::json!({
            "data": {"nonFundingLedgerUpdates": [{"hash": "0x1"}]}
        });
        assert_eq!(non_funding_ledger_events(&ledger_updates).len(), 1);
    }

    #[test]
    fn parses_fill_timestamp_and_side() {
        let fill = serde_json::json!({
            "side": "A",
            "time": 1_700_000_000_000_i64,
        });

        let parsed = user_fills(&serde_json::json!({"data": {"fills": [fill]}}));
        assert_eq!(parsed[0].side, Some(Side::Sell));
        assert_eq!(
            parsed[0].timestamp.unwrap().timestamp_millis(),
            1_700_000_000_000_i64
        );
    }

    #[test]
    fn parses_decimal_json_numbers_for_exchange_liquidations() {
        let liquidation = serde_json::json!({"szi": 0.125});

        assert_eq!(
            decimal_field(&liquidation, &["szi"]),
            Some("0.125".parse().unwrap())
        );
    }

    #[test]
    fn converts_exchange_fill_to_stable_ledger_event() {
        let timestamp = Utc
            .timestamp_millis_opt(1_700_000_000_000_i64)
            .single()
            .unwrap();
        let fill = HlUserFill {
            order_id: Some("42".to_string()),
            client_order_id: Some("0x00000000000000000000000000000001".to_string()),
            coin: Some("BTC".to_string()),
            size: Some("0.1".parse().unwrap()),
            price: Some("100.5".parse().unwrap()),
            fee: Some("0.01".parse().unwrap()),
            side: Some(Side::Buy),
            timestamp: Some(timestamp),
            trade_id: Some("99".to_string()),
            raw: serde_json::Value::Null,
        };

        let (event_id, event_timestamp, event) =
            fill_ledger_event(&fill, fill.client_order_id.clone(), Some(false)).unwrap();

        assert_eq!(event_id, "hl-fill-1700000000000-BTC-99");
        assert_eq!(event_timestamp, timestamp);
        assert!(matches!(
            event,
            LedgerEventKind::Fill {
                ref symbol,
                side: Side::Buy,
                size,
                price,
                fee: Some(fee),
                reduce_only: Some(false),
                ..
            } if symbol == "BTC"
                && size == "0.1".parse().unwrap()
                && price == "100.5".parse().unwrap()
                && fee == "0.01".parse().unwrap()
        ));
    }

    #[test]
    fn fills_can_use_order_association_when_the_exchange_omits_cloid() {
        let fill = HlUserFill {
            order_id: Some("42".to_string()),
            client_order_id: None,
            coin: Some("BTC".to_string()),
            size: Some(Decimal::ONE),
            price: Some(Decimal::from(100)),
            fee: None,
            side: Some(Side::Buy),
            timestamp: Some(
                Utc.timestamp_millis_opt(1_700_000_000_000_i64)
                    .single()
                    .unwrap(),
            ),
            trade_id: Some("99".to_string()),
            raw: serde_json::Value::Null,
        };
        let client_order_id = "0x00000000000000000000000000000001".to_string();

        let (_, _, event) =
            fill_ledger_event(&fill, Some(client_order_id.clone()), Some(true)).unwrap();

        assert!(matches!(
            event,
            LedgerEventKind::Fill {
                client_order_id: Some(ref value),
                reduce_only: Some(true),
                ..
            } if value == &client_order_id
        ));
    }

    #[test]
    fn parses_nested_websocket_order_updates() {
        let message = serde_json::json!({
            "channel": "orderUpdates",
            "data": [{
                "order": {
                    "cloid": "0x00000000000000000000019fa823f0d4",
                    "oid": 504208918599_u64,
                },
                "status": "filled",
            }]
        });
        let update = parse_order_update(&message, event_values(&message)[0]);

        assert_eq!(update.order_id.as_deref(), Some("504208918599"));
        assert_eq!(
            update.client_order_id.as_deref(),
            Some("0x00000000000000000000019fa823f0d4")
        );
        assert_eq!(update.status.as_deref(), Some("filled"));
    }

    #[test]
    fn order_notifier_retains_terminal_state_without_a_receiver() {
        let pending = HlTrackedOrder {
            submitted: HlSubmittedOrder {
                coin: HlCoin::new("DOGE"),
                side: crate::core::Side::Buy,
                size: "1".parse().unwrap(),
                limit_price: "0.1".parse().unwrap(),
                reduce_only: false,
                client_order_id: HlClientOrderId::new("0x00000000000000000000000000000001")
                    .unwrap(),
            },
            order_id: None,
            filled_size: Decimal::ZERO,
            state: HlTrackedOrderState::PendingSubmit,
        };
        let (sender, _) = watch::channel(pending);
        let filled = HlTrackedOrder {
            order_id: Some("42".to_string()),
            filled_size: "1".parse().unwrap(),
            state: HlTrackedOrderState::Filled,
            ..sender.borrow().clone()
        };

        sender.send_replace(filled);
        let receiver = sender.subscribe();

        assert!(receiver.borrow().state.is_terminal());
        assert_eq!(receiver.borrow().order_id.as_deref(), Some("42"));
    }

    #[test]
    fn resolves_margin_fraction_size_down() {
        let size = resolve_margin_fraction_size(
            "1000".parse().unwrap(),
            "900".parse().unwrap(),
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
    fn unified_collateral_drives_margin_fraction_sizing() {
        let size = resolve_margin_fraction_size(
            Decimal::from(999),
            Decimal::from(999),
            Decimal::from(1),
            2,
            1,
            "0.1".parse().unwrap(),
            "0.9".parse().unwrap(),
        )
        .unwrap();

        assert_eq!(size, "9.99".parse().unwrap());
    }

    #[test]
    fn unified_reserve_uses_total_collateral() {
        let error = resolve_margin_fraction_size(
            "998.758219".parse().unwrap(),
            "797.264".parse().unwrap(),
            Decimal::from(1),
            2,
            1,
            Decimal::ONE,
            "0.9".parse().unwrap(),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            HlBrokerError::InvalidRequest { message }
                if message == "sizing result is below the minimum quantity increment"
        ));
    }

    #[test]
    fn unified_collateral_survives_perp_account_updates() {
        let collateral = HlCollateralState {
            abstraction: HlUserAbstraction::UnifiedAccount,
            spot_usdc: Some(HlSpotUsdcState {
                total: Decimal::from(999),
                hold: Decimal::ZERO,
                available_after_maintenance: Decimal::from(999),
            }),
        };
        let mut perp_update = HlAccountState {
            equity: Decimal::ZERO,
            margin_used: Decimal::from(10),
            positions: HashMap::new(),
            updated_at: Utc::now(),
        };

        collateral.apply_to(&mut perp_update);

        assert_eq!(perp_update.equity, Decimal::from(999));
        assert_eq!(perp_update.margin_used, Decimal::from(10));
    }

    #[test]
    fn sizes_fractional_close_down_to_asset_increment() {
        let result =
            calculate_close_size("0.1019".parse().unwrap(), "0.5".parse().unwrap(), 3).unwrap();

        assert_eq!(result, "0.050".parse().unwrap());
    }

    #[test]
    fn parses_update_leverage_response() {
        let raw = serde_json::json!({
            "status": "ok",
            "response": {"type": "default"}
        });
        parse_default_action_response(&raw).unwrap();
    }

    #[test]
    fn parses_websocket_update_leverage_response() {
        let raw = serde_json::json!({
            "channel": "post",
            "data": {
                "response": {
                    "payload": {
                        "status": "ok",
                        "response": {"type": "default"}
                    }
                }
            }
        });
        parse_default_action_response(&raw).unwrap();
    }

    #[test]
    fn parses_nested_order_status_for_reconciliation() {
        let raw = serde_json::json!({
            "status": "ok",
            "response": {"data": {"orderStatus": "filled"}}
        });

        assert_eq!(order_status_from_response(&raw), Some("filled".to_string()));
    }
}
