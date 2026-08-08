use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use qf::audit::{LedgerEvent, LedgerSink, RunJournal};
use qf::core::{JournalId, StrategyId};
use qf::hyperliquid::client::HyperliquidSigner;
use qf::hyperliquid::{
    HlLiveBrokerConfig, HlMarginMode, HlMarketConfig, HlNetwork, HyperliquidLiveBroker,
};
use qf::risk::{RiskGuard, RiskLimits};
use qf::storage::{JsonlAuditSink, JsonlWriter};

use crate::strategy::ExampleStrategy;

pub async fn run(
    mut strategy: ExampleStrategy,
    account_address: alloy::primitives::Address,
    private_key: &str,
) -> anyhow::Result<()> {
    let signer = Arc::new(HyperliquidSigner::from_private_key(private_key)?);
    let mut config = HlLiveBrokerConfig::new(StrategyId::new("example"), account_address);
    config.network = HlNetwork::Testnet;
    config.markets = vec![HlMarketConfig {
        coin: strategy.coin().clone(),
        leverage: 1,
        margin_mode: HlMarginMode::Auto,
    }];
    let audit_path = std::env::var("QF_AUDIT_PATH")
        .unwrap_or_else(|_| "runs/example-live-audit.jsonl".to_string());
    let journal = RunJournal::new(JournalId::new("example-live"), NoopLedgerSink).with_audit_sink(
        JsonlAuditSink::new(JsonlWriter::create(audit_path)?, 10_000)?,
    );
    let broker = HyperliquidLiveBroker::connect(
        config,
        signer,
        RiskGuard::new(RiskLimits::default()),
        Arc::new(journal),
    )
    .await?;

    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        let Some(price) = broker.mid_price(strategy.coin()) else {
            continue;
        };
        strategy
            .on_price(broker.as_ref(), price)
            .await
            .context("example strategy failed")?;
    }
}

struct NoopLedgerSink;

impl LedgerSink for NoopLedgerSink {
    fn record(&mut self, _event: &LedgerEvent) -> anyhow::Result<()> {
        Ok(())
    }
}
