use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use qf::audit::{AuditAction, AuditRecord, LedgerEvent, LedgerSink, RunJournal};
use qf::core::{Decimal, JournalId, OrderId, RunMode, Side, StrategyId};
use qf::hyperliquid::client::HyperliquidSigner;
use qf::hyperliquid::types::{
    HlCancelRequest, HlCancelTarget, HlCloseRequest, HlCloseSize, HlCoin, HlOrderRequest,
    HlOrderSize, HlOrderType, HlTimeInForce,
};
use qf::hyperliquid::{
    HlLiveBrokerConfig, HlMarginMode, HlMarketConfig, HlNetwork, HyperliquidBroker,
    HyperliquidLiveBroker,
};
use qf::risk::{RiskGuard, RiskLimits};
use qf::storage::{JsonlAuditSink, JsonlWriter};
use rand::Rng;

const SOAK_COIN: &str = "BTC";
const MAX_SLIPPAGE_BPS: u32 = 100;
const ORDER_TIMEOUT: Duration = Duration::from_secs(20);
const STATE_TIMEOUT: Duration = Duration::from_secs(20);
const MIN_INTERVAL: Duration = Duration::from_secs(2);
const MAX_INTERVAL: Duration = Duration::from_secs(60 * 60);

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let account_address = std::env::var("HL_ACCOUNT_ADDRESS")
        .context("HL_ACCOUNT_ADDRESS is required")?
        .parse()
        .context("HL_ACCOUNT_ADDRESS must be a valid address")?;
    let private_key = std::env::var("HL_PRIVATE_KEY").context("HL_PRIVATE_KEY is required")?;
    let coin = HlCoin::new(SOAK_COIN);

    let signer = Arc::new(HyperliquidSigner::from_private_key(&private_key)?);
    let mut config = HlLiveBrokerConfig::new(StrategyId::new("live-broker-soak"), account_address);
    config.network = HlNetwork::Testnet;
    config.markets = vec![HlMarketConfig {
        coin: coin.clone(),
        leverage: 1,
        margin_mode: HlMarginMode::Auto,
    }];

    let audit_path = std::env::var("QF_AUDIT_PATH")
        .unwrap_or_else(|_| "runs/live-broker-soak/audit.jsonl".to_string());
    let journal = Arc::new(
        RunJournal::new(JournalId::new("live-broker-soak"), NoopLedgerSink).with_audit_sink(
            JsonlAuditSink::new(JsonlWriter::create(audit_path)?, 10_000)?,
        ),
    );
    let broker = HyperliquidLiveBroker::connect(
        config,
        signer,
        RiskGuard::new(soak_risk_limits()),
        Arc::clone(&journal),
    )
    .await?;

    record_startup_sizing(&journal, &broker, &coin)?;
    ensure_account_is_idle(&broker, &coin).await?;
    println!("testnet soak started for {SOAK_COIN}; press Ctrl-C to stop");

    let mut round = 1_u64;
    loop {
        tokio::select! {
            result = run_round(&broker, &coin, round, journal.as_ref()) => result?,
            _ = wait_for_shutdown() => {
                println!("shutdown requested; no new order will be submitted");
                return Ok(());
            }
        }

        round += 1;
        let interval = random_interval();
        println!(
            "round complete; next probe in {} seconds",
            interval.as_secs()
        );
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = wait_for_shutdown() => {
                println!("shutdown requested; no new order will be submitted");
                return Ok(());
            }
        }
    }
}

fn soak_risk_limits() -> RiskLimits {
    RiskLimits {
        max_leverage: None,
        max_order_notional: Some(max_order_notional()),
        max_post_trade_notional: Some(max_order_notional()),
        max_open_orders: Some(1),
        reduce_only: false,
    }
}

fn max_order_notional() -> Decimal {
    Decimal::from(20)
}

fn margin_fraction() -> Decimal {
    Decimal::new(15, 3)
}

fn reserve_fraction() -> Decimal {
    Decimal::ZERO
}

fn soak_order_size() -> HlOrderSize {
    HlOrderSize::MarginFraction {
        margin_fraction: margin_fraction(),
        reserve_fraction: reserve_fraction(),
    }
}

fn record_startup_sizing(
    journal: &RunJournal,
    broker: &HyperliquidLiveBroker,
    coin: &HlCoin,
) -> Result<()> {
    let account = broker
        .account_state()
        .context("account state is unavailable for sizing audit")?;
    let mid_price = broker
        .mid_price(coin)
        .context("market mid price is unavailable for sizing audit")?;
    let asset = broker
        .metadata()
        .asset(coin)
        .cloned()
        .context("market metadata is unavailable for sizing audit")?;
    let resolved_margin_mode = if asset.only_isolated {
        "isolated"
    } else {
        "cross"
    };
    let reserve_margin = account.equity * reserve_fraction();
    let available_margin =
        (account.equity - account.margin_used - reserve_margin).max(Decimal::ZERO);
    let planned_notional = available_margin * margin_fraction();

    journal.record_audit(AuditRecord {
        strategy_id: StrategyId::new("live-broker-soak"),
        mode: RunMode::Live,
        exchange: "hyperliquid".to_string(),
        symbol: Some(coin.0.clone()),
        action: AuditAction::ReconcileState,
        data: serde_json::json!({
            "kind": "soak_sizing_configuration",
            "account": {
                "equity": account.equity,
                "margin_used": account.margin_used,
            },
            "market": {
                "mid_price": mid_price,
                "size_decimals": asset.size_decimals,
                "exchange_leverage": 1,
                "requested_margin_mode": "auto",
                "resolved_margin_mode": resolved_margin_mode,
                "only_isolated": asset.only_isolated,
            },
            "sizing": {
                "margin_fraction": margin_fraction(),
                "reserve_fraction": reserve_fraction(),
                "available_margin": available_margin,
                "planned_notional": planned_notional,
                "max_order_notional": max_order_notional(),
                "max_post_trade_notional": max_order_notional(),
            },
        }),
    });
    Ok(())
}

async fn run_round(
    broker: &HyperliquidLiveBroker,
    coin: &HlCoin,
    round: u64,
    journal: &RunJournal,
) -> Result<()> {
    let round_started = Instant::now();
    let ready_started = Instant::now();
    broker
        .wait_until_trading_ready()
        .await
        .context("broker is not ready for trading")?;
    let ready_ms = elapsed_ms(ready_started);

    let precheck_started = Instant::now();
    ensure_account_is_idle(broker, coin).await?;
    let precheck_ms = elapsed_ms(precheck_started);

    let mid = broker
        .mid_price(coin)
        .context("missing market mid price after readiness check")?;
    let resting_submit_started = Instant::now();
    let resting = broker
        .place_order(HlOrderRequest {
            coin: coin.clone(),
            side: Side::Buy,
            size: soak_order_size(),
            reduce_only: false,
            order_type: HlOrderType::Limit {
                limit_price: mid * Decimal::new(99, 2),
                tif: HlTimeInForce::Alo,
            },
            client_order_id: None,
            expires_after: None,
        })
        .await
        .context("failed to submit post-only probe order")?;
    let resting_submit_ms = elapsed_ms(resting_submit_started);
    println!(
        "round={round} ready_ms={ready_ms} precheck_ms={precheck_ms} \
         resting_submit_ms={resting_submit_ms} client_order_id={}",
        resting.submitted.client_order_id.as_str(),
    );

    let cancel_started = Instant::now();
    broker
        .cancel_order(HlCancelRequest {
            coin: coin.clone(),
            asset: None,
            order_id: OrderId::new("unused"),
            target: Some(HlCancelTarget::ClientOrderId(
                resting.submitted.client_order_id.clone(),
            )),
            fast: false,
            expires_after: None,
        })
        .await
        .context("failed to cancel post-only probe order")?;
    let cancel_submit_ms = elapsed_ms(cancel_started);
    let cancel_idle_started = Instant::now();
    wait_until_idle(broker, coin).await?;
    let cancel_to_idle_ms = elapsed_ms(cancel_idle_started);

    let entry_started = Instant::now();
    let opened = broker
        .place_order(HlOrderRequest {
            coin: coin.clone(),
            side: Side::Buy,
            size: soak_order_size(),
            reduce_only: false,
            order_type: HlOrderType::Market {
                max_slippage_bps: Some(MAX_SLIPPAGE_BPS),
            },
            client_order_id: None,
            expires_after: None,
        })
        .await
        .context("failed to submit market entry probe order")?;
    let entry_submit_ms = elapsed_ms(entry_started);
    let entry_terminal_started = Instant::now();
    println!(
        "round={round} cancel_submit_ms={cancel_submit_ms} \
         cancel_to_idle_ms={cancel_to_idle_ms} entry_submit_ms={entry_submit_ms} \
         entry_client_order_id={}",
        opened.submitted.client_order_id.as_str(),
    );
    broker
        .wait_order_terminal(&opened.submitted.client_order_id, ORDER_TIMEOUT)
        .await
        .context("entry probe order did not reach a terminal state")?;
    let entry_to_terminal_ms = elapsed_ms(entry_terminal_started) + entry_submit_ms;

    let entry_position_started = Instant::now();
    wait_until_position_exists(broker, coin).await?;
    let entry_to_position_ms = elapsed_ms(entry_position_started) + entry_to_terminal_ms;

    let close_started = Instant::now();
    let closed = broker
        .close_position(HlCloseRequest {
            coin: coin.clone(),
            size: HlCloseSize::Full,
            max_slippage_bps: Some(MAX_SLIPPAGE_BPS),
            client_order_id: None,
            expires_after: None,
        })
        .await
        .context("failed to submit reduce-only close order")?;
    let close_submit_ms = elapsed_ms(close_started);
    let close_terminal_started = Instant::now();
    println!(
        "round={round} entry_to_terminal_ms={entry_to_terminal_ms} \
         entry_to_position_ms={entry_to_position_ms} close_submit_ms={close_submit_ms} \
         close_client_order_id={}",
        closed.submitted.client_order_id.as_str(),
    );
    broker
        .wait_order_terminal(&closed.submitted.client_order_id, ORDER_TIMEOUT)
        .await
        .context("close probe order did not reach a terminal state")?;
    let close_to_terminal_ms = elapsed_ms(close_terminal_started) + close_submit_ms;
    let close_idle_started = Instant::now();
    wait_until_idle(broker, coin).await?;
    let close_to_idle_ms = elapsed_ms(close_idle_started) + close_to_terminal_ms;
    let round_total_ms = elapsed_ms(round_started);
    println!(
        "round={round} close_to_terminal_ms={close_to_terminal_ms} \
         close_to_idle_ms={close_to_idle_ms} round_total_ms={round_total_ms}",
    );
    journal.record_audit(AuditRecord {
        strategy_id: StrategyId::new("live-broker-soak"),
        mode: RunMode::Live,
        exchange: "hyperliquid".to_string(),
        symbol: Some(coin.0.clone()),
        action: AuditAction::ReconcileState,
        data: serde_json::json!({
            "kind": "soak_round_latency",
            "round": round,
            "latency_ms": {
                "ready": ready_ms,
                "precheck": precheck_ms,
                "resting_submit": resting_submit_ms,
                "cancel_submit": cancel_submit_ms,
                "cancel_to_idle": cancel_to_idle_ms,
                "entry_submit": entry_submit_ms,
                "entry_to_terminal": entry_to_terminal_ms,
                "entry_to_position": entry_to_position_ms,
                "close_submit": close_submit_ms,
                "close_to_terminal": close_to_terminal_ms,
                "close_to_idle": close_to_idle_ms,
                "round_total": round_total_ms,
            },
            "client_order_ids": {
                "resting": resting.submitted.client_order_id.as_str(),
                "entry": opened.submitted.client_order_id.as_str(),
                "close": closed.submitted.client_order_id.as_str(),
            },
        }),
    });
    Ok(())
}

fn elapsed_ms(started: Instant) -> u128 {
    started.elapsed().as_millis()
}

async fn ensure_account_is_idle(broker: &HyperliquidLiveBroker, coin: &HlCoin) -> Result<()> {
    let position = broker.position(coin);
    let open_orders = broker.open_orders_for(coin);
    if position.is_some_and(|position| position.size != Decimal::ZERO) || !open_orders.is_empty() {
        bail!(
            "refusing to trade while {} has an existing position or open order",
            coin.0
        );
    }
    Ok(())
}

async fn wait_until_position_exists(broker: &HyperliquidLiveBroker, coin: &HlCoin) -> Result<()> {
    tokio::time::timeout(STATE_TIMEOUT, async {
        loop {
            if broker
                .position(coin)
                .is_some_and(|position| position.size != Decimal::ZERO)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .context("position was not observed after entry probe")?;
    Ok(())
}

async fn wait_until_idle(broker: &HyperliquidLiveBroker, coin: &HlCoin) -> Result<()> {
    tokio::time::timeout(STATE_TIMEOUT, async {
        loop {
            if ensure_account_is_idle(broker, coin).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .context("account did not return to an idle state")?;
    ensure_account_is_idle(broker, coin).await
}

fn random_interval() -> Duration {
    let seconds = rand::rng().random_range(MIN_INTERVAL.as_secs()..=MAX_INTERVAL.as_secs());
    Duration::from_secs(seconds)
}

async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("SIGTERM handler must be installable");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c()
        .await
        .expect("Ctrl-C handler must be installable");
}

struct NoopLedgerSink;

impl LedgerSink for NoopLedgerSink {
    fn record(&mut self, _event: &LedgerEvent) -> anyhow::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_interval_stays_within_the_configured_range() {
        for _ in 0..1_000 {
            let interval = random_interval();
            assert!(interval >= MIN_INTERVAL);
            assert!(interval <= MAX_INTERVAL);
        }
    }

    #[test]
    fn soak_limits_allow_only_one_capped_open_order() {
        let limits = soak_risk_limits();

        assert_eq!(limits.max_order_notional, Some(max_order_notional()));
        assert_eq!(limits.max_post_trade_notional, Some(max_order_notional()));
        assert_eq!(limits.max_open_orders, Some(1));
        assert_eq!(limits.max_leverage, None);
        assert!(!limits.reduce_only);
    }

    #[test]
    fn soak_order_uses_a_small_fraction_of_available_margin() {
        let HlOrderSize::MarginFraction {
            margin_fraction,
            reserve_fraction,
        } = soak_order_size()
        else {
            panic!("soak order must use margin fraction sizing");
        };

        assert_eq!(margin_fraction, Decimal::new(15, 3));
        assert_eq!(reserve_fraction, Decimal::ZERO);
    }

    #[test]
    fn soak_market_automatically_selects_the_supported_margin_mode() {
        let market = HlMarketConfig {
            coin: HlCoin::new(SOAK_COIN),
            leverage: 1,
            margin_mode: HlMarginMode::Auto,
        };

        assert_eq!(market.coin, HlCoin::new("BTC"));
        assert_eq!(market.leverage, 1);
        assert_eq!(market.margin_mode, HlMarginMode::Auto);
    }
}
