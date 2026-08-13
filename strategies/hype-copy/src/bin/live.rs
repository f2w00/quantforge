use anyhow::{Context, Result};
use hype_copy::{
    COIN, Config, CopyState, parse_config, parse_network, rebalance, write_performance_report,
};
use qf::audit::RunJournal;
use qf::core::JournalId;
use qf::hyperliquid::broker::HyperliquidBroker;
use qf::hyperliquid::client::{
    HyperliquidSigner, HyperliquidWsClient, HyperliquidWsEvent, parse_user_fills,
    ws_clearinghouse_state,
};
use qf::hyperliquid::types::HlCoin;
use qf::hyperliquid::{HlMarginMode, HlRestBrokerConfig, HyperliquidRestBroker};
use qf::risk::{RiskGuard, RiskLimits};
use qf::storage::{JsonlAuditSink, JsonlLedgerSink, JsonlWriter};
use serde_json::Value;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{Duration, interval};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .context("usage: hype-copy-live <config.toml>")?;
    let config = parse_config(&path)?;
    run(config).await
}

async fn run(config: Config) -> Result<()> {
    let network = parse_network(&config.network)?;
    let address = std::env::var("HL_ACCOUNT_ADDRESS")
        .context("HL_ACCOUNT_ADDRESS is required")?
        .parse()?;
    let key = std::env::var("HL_PRIVATE_KEY").context("HL_PRIVATE_KEY is required")?;
    let audit_path = std::env::var("QF_AUDIT_PATH")
        .unwrap_or_else(|_| "runs/hype-copy-live/audit.jsonl".to_string());
    let run_dir = std::path::Path::new("runs/hype-copy-live");
    std::fs::create_dir_all(run_dir)?;
    let ledger_path = run_dir.join("ledger.jsonl");
    let report_path = run_dir.join("performance.md");
    spawn_performance_reporter(ledger_path.clone(), report_path.clone());
    let journal = Arc::new(
        RunJournal::new(
            JournalId::new("hype-copy-live"),
            JsonlLedgerSink::new(JsonlWriter::create(&ledger_path)?),
        )
        .with_audit_sink(JsonlAuditSink::new(
            JsonlWriter::create(audit_path)?,
            10_000,
        )?),
    );
    let signer = Arc::new(HyperliquidSigner::from_private_key(&key)?);
    let mut broker_config =
        HlRestBrokerConfig::new(qf::core::StrategyId::new("hype-copy-live"), address);
    broker_config.network = network;
    broker_config.default_margin_mode = HlMarginMode::Auto;
    broker_config.default_market_slippage_bps = config.market_slippage_bps;
    broker_config.default_close_slippage_bps = config.close_slippage_bps;
    let broker = HyperliquidRestBroker::connect(
        broker_config,
        signer,
        RiskGuard::new(RiskLimits::default()),
        journal,
    )
    .await?;

    let (tx, mut rx) = mpsc::channel::<Value>(256);
    let (ws, mut events) = HyperliquidWsClient::connect(network.ws_url()).await?;
    ws.subscribe(serde_json::json!({"type":"userFills","user":config.leader}))
        .await?;
    ws.subscribe(serde_json::json!({"type":"clearinghouseState","user":config.leader}))
        .await?;
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            if let HyperliquidWsEvent::Message(message) = event {
                let _ = tx.send(message).await;
            }
        }
    });
    let coin = HlCoin::new(COIN);
    let mut state = CopyState::default();
    println!("hype-copy live started for leader {}", config.leader);
    while let Some(message) = rx.recv().await {
        match message.get("channel").and_then(Value::as_str) {
            Some("userFills") => {
                for fill in parse_user_fills(&message) {
                    state.on_fill(&fill);
                }
                let account = broker.account_state().await?;
                let book = qf::hyperliquid::client::HyperliquidRestClient::new(network.rest_url())
                    .l2_book(&coin)
                    .await?;
                state.refresh_target(
                    account.equity,
                    (book.best_bid + book.best_ask) / qf::core::Decimal::from(2),
                );
                rebalance(
                    broker.as_ref(),
                    &coin,
                    state.desired_size,
                    config.leverage,
                    config.market_slippage_bps,
                )
                .await?;
            }
            Some("clearinghouseState") => {
                let account = broker.account_state().await?;
                let book = qf::hyperliquid::client::HyperliquidRestClient::new(network.rest_url())
                    .l2_book(&coin)
                    .await?;
                let leader = ws_clearinghouse_state(&message)?;
                let size = leader
                    .positions
                    .get(&coin)
                    .map(|position| position.size)
                    .unwrap_or_default();
                state.on_snapshot(
                    size,
                    account.equity,
                    (book.best_bid + book.best_ask) / qf::core::Decimal::from(2),
                );
                rebalance(
                    broker.as_ref(),
                    &coin,
                    state.desired_size,
                    config.leverage,
                    config.market_slippage_bps,
                )
                .await?;
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
                eprintln!("failed to write live performance report: {error:#}");
            }
        }
    });
}
