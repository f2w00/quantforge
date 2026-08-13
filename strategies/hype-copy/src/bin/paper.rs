use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use hype_copy::{
    COIN, Config, CopyState, parse_config, parse_network, rebalance, write_performance_report,
};
use qf::audit::RunJournal;
use qf::core::JournalId;
use qf::hyperliquid::broker::state::HlBrokerState;
use qf::hyperliquid::broker::{HyperliquidBacktestBroker, HyperliquidBroker};
use qf::hyperliquid::client::{
    HyperliquidWsClient, HyperliquidWsEvent, parse_user_fills, ws_clearinghouse_state,
};
use qf::hyperliquid::types::{HlAccountState, HlCoin};
use qf::risk::{RiskGuard, RiskLimits};
use qf::storage::{JsonlLedgerSink, JsonlWriter};
use serde_json::Value;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;
use tokio::time::{Duration, interval};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .context("usage: hype-copy-paper <config.toml>")?;
    let config = parse_config(&path)?;
    run(config).await
}

async fn run(config: Config) -> Result<()> {
    let network = parse_network(&config.network)?;
    let coin = HlCoin::new(COIN);
    let run_dir = Path::new("runs/hype-copy-paper");
    std::fs::create_dir_all(run_dir)?;
    let ledger_path = run_dir.join("ledger.jsonl");
    let report_path = run_dir.join("performance.md");
    spawn_performance_reporter(ledger_path.clone(), report_path.clone());
    let broker = Arc::new(HyperliquidBacktestBroker::new(
        qf::core::StrategyId::new("hype-copy-paper"),
        HlBrokerState {
            account: HlAccountState {
                equity: config.initial_equity,
                margin_used: 0.into(),
                positions: HashMap::new(),
                updated_at: chrono::Utc::now(),
            },
            open_orders: Vec::new(),
        },
        RiskGuard::new(RiskLimits::default()),
        Arc::new(RunJournal::new(
            JournalId::new("hype-copy-paper"),
            JsonlLedgerSink::new(JsonlWriter::create(&ledger_path)?),
        )),
    ));
    broker.set_leverage(coin.clone(), config.leverage)?;
    broker.set_taker_fee_bps(config.taker_fee_bps)?;
    broker.set_market_slippage_bps(config.market_slippage_bps)?;

    let (tx, mut rx) = mpsc::channel::<Value>(256);
    let (leader_ws, mut leader_events) = HyperliquidWsClient::connect(network.ws_url()).await?;
    leader_ws
        .subscribe(serde_json::json!({"type":"userFills","user":config.leader}))
        .await?;
    leader_ws
        .subscribe(serde_json::json!({"type":"clearinghouseState","user":config.leader}))
        .await?;
    let tx_leader = tx.clone();
    tokio::spawn(async move {
        while let Some(event) = leader_events.recv().await {
            if let HyperliquidWsEvent::Message(message) = event {
                let _ = tx_leader.send(message).await;
            }
        }
    });
    let (mids_ws, mut mids_events) = HyperliquidWsClient::connect(network.ws_url()).await?;
    mids_ws.subscribe_all_mids().await?;
    let tx_mids = tx.clone();
    tokio::spawn(async move {
        while let Some(event) = mids_events.recv().await {
            if let HyperliquidWsEvent::Message(message) = event {
                let _ = tx_mids.send(message).await;
            }
        }
    });
    drop(tx);

    let mut state = CopyState::default();
    let mut price = None;
    println!("hype-copy paper started for leader {}", config.leader);
    while let Some(message) = rx.recv().await {
        match message.get("channel").and_then(Value::as_str) {
            Some("allMids") => {
                price = message
                    .pointer("/data/mids/HYPE")
                    .and_then(Value::as_str)
                    .and_then(|v| v.parse().ok())
                    .or_else(|| {
                        message
                            .pointer("/data/HYPE")
                            .and_then(Value::as_str)
                            .and_then(|v| v.parse().ok())
                    });
                if let Some(px) = price {
                    broker.set_mark_price(coin.clone(), px)?;
                }
            }
            Some("userFills") => {
                for fill in parse_user_fills(&message) {
                    state.on_fill(&fill);
                }
                if let Some(px) = price {
                    let account = broker.account_state().await?;
                    state.refresh_target(account.equity, px);
                    rebalance(
                        broker.as_ref(),
                        &coin,
                        state.desired_size,
                        config.leverage,
                        config.market_slippage_bps,
                    )
                    .await?;
                }
            }
            Some("clearinghouseState") => {
                if let Some(px) = price {
                    let account = broker.account_state().await?;
                    let leader = ws_clearinghouse_state(&message)?;
                    let size = leader
                        .positions
                        .get(&coin)
                        .map(|position| position.size)
                        .unwrap_or_default();
                    state.on_snapshot(size, account.equity, px);
                    rebalance(
                        broker.as_ref(),
                        &coin,
                        state.desired_size,
                        config.leverage,
                        config.market_slippage_bps,
                    )
                    .await?;
                }
            }
            _ => {}
        }
    }
    write_performance_report(&ledger_path, &report_path)?;
    Ok(())
}

fn spawn_performance_reporter(ledger_path: PathBuf, report_path: PathBuf) {
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(5 * 60));
        loop {
            ticker.tick().await;
            if let Err(error) = write_performance_report(&ledger_path, &report_path) {
                eprintln!("failed to write paper performance report: {error:#}");
            }
        }
    });
}
