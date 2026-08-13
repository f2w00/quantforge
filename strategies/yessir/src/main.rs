use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, bail};
use chrono::{DateTime, Utc};
use qf::audit::RunJournal;
use qf::core::{Decimal, JournalId, Side, StrategyId};
use qf::hyperliquid::broker::state::HlBrokerState;
use qf::hyperliquid::client::ws::ws_clearinghouse_state;
use qf::hyperliquid::client::{HyperliquidRestClient, HyperliquidWsClient, HyperliquidWsEvent};
use qf::hyperliquid::types::{
    HlAccountState, HlCoin, HlMidSnapshot, HlOrderRequest, HlOrderSize, HlOrderType, HlPosition,
};
use qf::hyperliquid::{HlNetwork, HyperliquidBacktestBroker, HyperliquidBroker};
use qf::performance::PerformanceReport;
use qf::risk::{RiskGuard, RiskLimits};
use qf::storage::{JsonlLedgerSink, JsonlWriter};
use rust_decimal::prelude::ToPrimitive;
use serde::Deserialize;
use tokio::sync::mpsc;

#[derive(Deserialize)]
struct Config {
    #[serde(default = "default_network")]
    network: String,
    #[serde(default)]
    taker_fee_bps: u32,
    #[serde(default)]
    market_slippage_bps: u32,
    leaders: Vec<LeaderConfig>,
}

#[derive(Deserialize)]
struct LeaderConfig {
    name: String,
    address: String,
    initial_equity: Decimal,
}

struct Leader {
    name: String,
    address: String,
    initial_equity: Decimal,
    copy_ratio: Decimal,
    previous_positions: HashMap<HlCoin, HlPosition>,
    known_leverage: HashMap<HlCoin, u32>,
    reset_baseline: bool,
    last_mark_snapshot_at: Option<DateTime<Utc>>,
    broker: Arc<HyperliquidBacktestBroker>,
    ledger_path: PathBuf,
}

enum StreamEvent {
    Mids(HyperliquidWsEvent),
    Leader(usize, HyperliquidWsEvent),
}

fn default_network() -> String {
    "mainnet".to_string()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let config_path = std::env::args()
        .nth(1)
        .context("usage: yessir <config.toml>")?;
    let config: Config = toml::from_str(
        &std::fs::read_to_string(&config_path)
            .with_context(|| format!("failed to read configuration: {config_path}"))?,
    )?;
    let network = parse_network(&config.network)?;
    validate_config(&config)?;

    let run_dir =
        PathBuf::from("runs/yessir").join(Utc::now().format("%Y%m%dT%H%M%SZ").to_string());
    std::fs::create_dir_all(&run_dir)?;

    let rest = HyperliquidRestClient::new(network.rest_url());
    let mut mids = rest.all_mids().await?;
    let mut leaders = initialize_leaders(&config, &rest, &run_dir).await?;
    write_summary(&run_dir, &leaders)?;

    let (events_tx, mut events_rx) = mpsc::channel(256);
    spawn_mid_stream(network, events_tx.clone()).await?;
    for (index, leader) in leaders.iter().enumerate() {
        spawn_leader_stream(network, index, leader.address.clone(), events_tx.clone()).await?;
    }
    drop(events_tx);

    while let Some(event) = events_rx.recv().await {
        match event {
            StreamEvent::Mids(HyperliquidWsEvent::Message(message)) => {
                match message.get("channel").and_then(serde_json::Value::as_str) {
                    Some("allMids") => {
                        mids.apply_ws_message(&message)?;
                        handle_mids(&mut leaders, &mids).await?;
                    }
                    _ => {}
                }
            }
            StreamEvent::Leader(index, HyperliquidWsEvent::Connected) => {
                // 重连后下一份快照会重建基线，避免断线期间追单。
                leaders[index].reset_baseline = true;
            }
            StreamEvent::Leader(index, HyperliquidWsEvent::Disconnected) => {
                eprintln!(
                    "{}: websocket disconnected; rebuilding baseline after reconnect",
                    leaders[index].name
                );
            }
            StreamEvent::Leader(index, HyperliquidWsEvent::Message(message)) => {
                match message.get("channel").and_then(serde_json::Value::as_str) {
                    Some("clearinghouseState") => {
                        let state = ws_clearinghouse_state(&message)?;
                        apply_snapshot(&mut leaders[index], state, &mids).await?;
                        write_summary(&run_dir, &leaders)?;
                    }
                    _ => {}
                }
            }
            StreamEvent::Mids(HyperliquidWsEvent::Disconnected) => {
                eprintln!("websocket disconnected; waiting to rebuild leader baselines");
            }
            StreamEvent::Mids(HyperliquidWsEvent::Connected) => {}
        }
    }

    bail!("Hyperliquid websocket event channel closed")
}

fn parse_network(value: &str) -> anyhow::Result<HlNetwork> {
    match value {
        "mainnet" => Ok(HlNetwork::Mainnet),
        "testnet" => Ok(HlNetwork::Testnet),
        _ => bail!("network must be mainnet or testnet"),
    }
}

fn validate_config(config: &Config) -> anyhow::Result<()> {
    if config.leaders.is_empty() {
        bail!("at least one leader is required");
    }
    let mut names = HashSet::new();
    let mut addresses = HashSet::new();
    for leader in &config.leaders {
        if leader.name.trim().is_empty() || leader.name.contains('/') || leader.name.contains('\\')
        {
            bail!("leader name must be a non-empty path-safe value");
        }
        if leader.initial_equity <= Decimal::ZERO {
            bail!("leader {} initial_equity must be positive", leader.name);
        }
        if !names.insert(leader.name.clone()) {
            bail!("leader names must be unique: {}", leader.name);
        }
        if !addresses.insert(leader.address.to_ascii_lowercase()) {
            bail!("leader addresses must be unique: {}", leader.address);
        }
    }
    Ok(())
}

async fn initialize_leaders(
    config: &Config,
    rest: &HyperliquidRestClient,
    run_dir: &Path,
) -> anyhow::Result<Vec<Leader>> {
    let mut leaders = Vec::with_capacity(config.leaders.len());
    for leader_config in &config.leaders {
        let state = rest
            .clearinghouse_state(&leader_config.address)
            .await
            .with_context(|| {
                format!(
                    "failed to read initial state for leader {}",
                    leader_config.name
                )
            })?;
        if state.equity <= Decimal::ZERO {
            bail!(
                "leader {} has non-positive source equity",
                leader_config.name
            );
        }
        let known_leverage = known_leverages(&state.positions)?;
        let ledger_path = run_dir.join(&leader_config.name).join("ledger.jsonl");
        std::fs::create_dir_all(ledger_path.parent().expect("ledger path has parent"))?;
        let broker = Arc::new(HyperliquidBacktestBroker::new(
            StrategyId::new(format!("yessir-{}", leader_config.name)),
            HlBrokerState {
                account: empty_account(leader_config.initial_equity),
                open_orders: Vec::new(),
            },
            RiskGuard::new(RiskLimits::default()),
            Arc::new(RunJournal::new(
                JournalId::new(format!("yessir-{}", leader_config.name)),
                JsonlLedgerSink::new(JsonlWriter::create(&ledger_path)?),
            )),
        ));
        broker.set_taker_fee_bps(config.taker_fee_bps)?;
        broker.set_market_slippage_bps(config.market_slippage_bps)?;
        leaders.push(Leader {
            name: leader_config.name.clone(),
            address: leader_config.address.clone(),
            initial_equity: leader_config.initial_equity,
            copy_ratio: leader_config.initial_equity / state.equity,
            previous_positions: state.positions,
            known_leverage,
            reset_baseline: false,
            last_mark_snapshot_at: None,
            broker,
            ledger_path,
        });
    }
    Ok(leaders)
}

fn empty_account(equity: Decimal) -> HlAccountState {
    HlAccountState {
        equity,
        margin_used: Decimal::ZERO,
        positions: HashMap::new(),
        updated_at: Utc::now(),
    }
}

async fn subscribe_leader_state(ws: &HyperliquidWsClient, address: &str) -> anyhow::Result<()> {
    ws.subscribe(serde_json::json!({
        "type": "clearinghouseState",
        "user": address,
    }))
    .await
}

async fn spawn_mid_stream(
    network: HlNetwork,
    events_tx: mpsc::Sender<StreamEvent>,
) -> anyhow::Result<()> {
    let (ws, mut events) = HyperliquidWsClient::connect(network.ws_url()).await?;
    ws.subscribe_all_mids().await?;
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            if events_tx.send(StreamEvent::Mids(event)).await.is_err() {
                return;
            }
        }
    });
    Ok(())
}

async fn spawn_leader_stream(
    network: HlNetwork,
    index: usize,
    address: String,
    events_tx: mpsc::Sender<StreamEvent>,
) -> anyhow::Result<()> {
    let (ws, mut events) = HyperliquidWsClient::connect(network.ws_url()).await?;
    subscribe_leader_state(&ws, &address).await?;
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            if events_tx
                .send(StreamEvent::Leader(index, event))
                .await
                .is_err()
            {
                return;
            }
        }
    });
    Ok(())
}

async fn handle_mids(leaders: &mut [Leader], mids: &HlMidSnapshot) -> anyhow::Result<()> {
    let now = Utc::now();
    for leader in leaders {
        if leader
            .last_mark_snapshot_at
            .is_some_and(|last_snapshot| now - last_snapshot < chrono::Duration::hours(1))
        {
            continue;
        }
        let positions = leader.broker.account_state().await?.positions;
        if positions.is_empty() {
            continue;
        }
        for coin in positions.keys() {
            if let Some(price) = mids.mids.get(coin) {
                leader.broker.set_mark_price(coin.clone(), *price)?;
            }
        }
        leader.last_mark_snapshot_at = Some(now);
    }
    Ok(())
}

async fn apply_snapshot(
    leader: &mut Leader,
    state: HlAccountState,
    mids: &HlMidSnapshot,
) -> anyhow::Result<()> {
    let current_positions = state.positions;
    if leader.reset_baseline {
        leader
            .known_leverage
            .extend(known_leverages(&current_positions)?);
        leader.previous_positions = current_positions;
        leader.reset_baseline = false;
        return Ok(());
    }
    let mut coins: HashSet<_> = leader.previous_positions.keys().cloned().collect();
    coins.extend(current_positions.keys().cloned());
    for coin in coins {
        let previous_size = leader
            .previous_positions
            .get(&coin)
            .map(|position| position.size)
            .unwrap_or(Decimal::ZERO);
        let current = current_positions.get(&coin);
        if let Some(position) = current {
            leader
                .known_leverage
                .insert(coin.clone(), leverage_as_u32(position)?);
        }
        let current_size = current
            .map(|position| position.size)
            .unwrap_or(Decimal::ZERO);
        let source_delta = current_size - previous_size;
        if source_delta == Decimal::ZERO {
            continue;
        }
        let Some(price) = mids.mids.get(&coin).copied() else {
            eprintln!("{}: skipped {coin:?}; no current mid price", leader.name);
            continue;
        };
        let Some(leverage) = leader.known_leverage.get(&coin).copied() else {
            eprintln!(
                "{}: skipped {coin:?}; no known leader leverage",
                leader.name
            );
            continue;
        };
        let size = (source_delta.abs() * leader.copy_ratio)
            .round_dp_with_strategy(8, rust_decimal::RoundingStrategy::ToZero);
        if size == Decimal::ZERO {
            continue;
        }
        leader.broker.set_mark_price(coin.clone(), price)?;
        leader.broker.set_leverage(coin.clone(), leverage)?;
        leader
            .broker
            .place_order(HlOrderRequest {
                coin: coin.clone(),
                side: if source_delta.is_sign_positive() {
                    Side::Buy
                } else {
                    Side::Sell
                },
                size: HlOrderSize::Exact(size),
                leverage: Some(leverage),
                reduce_only: false,
                order_type: HlOrderType::Market {
                    max_slippage_bps: None,
                },
                client_order_id: None,
                expires_after: None,
            })
            .await
            .with_context(|| format!("{}: failed to copy {coin:?} position change", leader.name))?;
    }
    leader.previous_positions = current_positions;
    Ok(())
}

fn known_leverages(
    positions: &HashMap<HlCoin, HlPosition>,
) -> anyhow::Result<HashMap<HlCoin, u32>> {
    positions
        .iter()
        .map(|(coin, position)| Ok((coin.clone(), leverage_as_u32(position)?)))
        .collect()
}

fn leverage_as_u32(position: &HlPosition) -> anyhow::Result<u32> {
    let Some(leverage) = position.leverage.to_u32() else {
        bail!(
            "{} has invalid leverage {}",
            position.coin.0,
            position.leverage
        );
    };
    if leverage == 0 || position.leverage != Decimal::from(leverage) {
        bail!(
            "{} has invalid leverage {}",
            position.coin.0,
            position.leverage
        );
    }
    Ok(leverage)
}

fn write_summary(run_dir: &Path, leaders: &[Leader]) -> anyhow::Result<()> {
    let mut summary = String::from("# Yessir Copy Simulation\n\n");
    for leader in leaders {
        let report = PerformanceReport::from_events(qf::storage::JsonlLedgerReader::read_all(
            &leader.ledger_path,
        )?);
        let directory = leader.ledger_path.parent().expect("ledger path has parent");
        std::fs::write(directory.join("performance.md"), report.to_markdown())?;
        summary.push_str(&format!(
            "- {}: `{}`; copy ratio `{}`; initial virtual equity `{}`\n",
            leader.name, leader.address, leader.copy_ratio, leader.initial_equity
        ));
    }
    std::fs::write(run_dir.join("summary.md"), summary)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use qf::audit::RunJournal;
    use qf::hyperliquid::broker::state::HlBrokerState;
    use qf::risk::{RiskGuard, RiskLimits};
    use qf::storage::MemoryLedgerSink;

    use super::*;

    fn position(coin: &str, size: Decimal, leverage: u32) -> HlPosition {
        HlPosition {
            coin: HlCoin::new(coin),
            size,
            entry_price: None,
            notional: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            return_on_equity: Decimal::ZERO,
            leverage: Decimal::from(leverage),
            liquidation_price: None,
        }
    }

    fn leader(copy_ratio: Decimal) -> Leader {
        Leader {
            name: "test".to_string(),
            address: "0xtest".to_string(),
            initial_equity: Decimal::from(1_000),
            copy_ratio,
            previous_positions: HashMap::new(),
            known_leverage: HashMap::new(),
            reset_baseline: false,
            last_mark_snapshot_at: None,
            broker: Arc::new(HyperliquidBacktestBroker::new(
                StrategyId::new("yessir-test"),
                HlBrokerState {
                    account: empty_account(Decimal::from(1_000)),
                    open_orders: Vec::new(),
                },
                RiskGuard::new(RiskLimits::default()),
                Arc::new(RunJournal::new(
                    JournalId::new("yessir-test"),
                    MemoryLedgerSink::new(),
                )),
            )),
            ledger_path: PathBuf::from("unused-ledger.jsonl"),
        }
    }

    #[test]
    fn validates_integer_leverage() {
        let position = HlPosition {
            coin: HlCoin::new("BTC"),
            size: Decimal::ONE,
            entry_price: None,
            notional: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            return_on_equity: Decimal::ZERO,
            leverage: Decimal::from(10),
            liquidation_price: None,
        };

        assert_eq!(leverage_as_u32(&position).unwrap(), 10);
    }

    #[test]
    fn rejects_fractional_leverage() {
        let position = HlPosition {
            coin: HlCoin::new("BTC"),
            size: Decimal::ONE,
            entry_price: None,
            notional: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            return_on_equity: Decimal::ZERO,
            leverage: Decimal::new(15, 1),
            liquidation_price: None,
        };

        assert!(leverage_as_u32(&position).is_err());
    }

    #[tokio::test]
    async fn copies_position_delta_at_fixed_ratio_with_leader_leverage() {
        let mut leader = leader(Decimal::new(1, 1));
        let coin = HlCoin::new("BTC");
        let mut mids = HlMidSnapshot::default();
        mids.mids.insert(coin.clone(), Decimal::from(100));
        let mut positions = HashMap::new();
        positions.insert(coin.clone(), position("BTC", Decimal::from(2), 10));

        apply_snapshot(
            &mut leader,
            HlAccountState {
                equity: Decimal::from(10_000),
                margin_used: Decimal::ZERO,
                positions,
                updated_at: Utc::now(),
            },
            &mids,
        )
        .await
        .unwrap();

        let copied = leader.broker.position(&coin).await.unwrap().unwrap();
        assert_eq!(copied.size, Decimal::new(2, 1));
        assert_eq!(copied.leverage, Decimal::from(10));
    }
}
