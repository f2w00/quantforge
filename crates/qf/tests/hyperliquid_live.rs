use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use qf::audit::{LedgerEventKind, RunJournal};
use qf::core::{Decimal, JournalId, Side, StrategyId};
use qf::hyperliquid::broker::live::HlTrackedOrderState;
use qf::hyperliquid::client::HyperliquidSigner;
use qf::hyperliquid::types::{
    HlCloseRequest, HlCloseSize, HlCoin, HlOrderRequest, HlOrderSize, HlOrderType,
};
use qf::hyperliquid::{
    HlLiveBrokerConfig, HlMarketConfig, HlNetwork, HyperliquidBroker, HyperliquidLiveBroker,
};
use qf::risk::{RiskGuard, RiskLimits};
use qf::storage::{MemoryAuditSink, MemoryLedgerSink};

const POSITION_TIMEOUT: Duration = Duration::from_secs(15);
const CONNECTION_HOLD_DURATION: Duration = Duration::from_secs(5);

#[tokio::test]
#[ignore = "places and closes a real Hyperliquid Mainnet DOGE order"]
async fn mainnet_places_and_closes_doge_market_order() -> Result<()> {
    let audit = MemoryAuditSink::new();
    let audit_reader = audit.clone();
    let result = mainnet_places_and_closes_doge_market_order_inner(audit).await;
    if result.is_err() {
        match serde_json::to_string_pretty(&audit_reader.events()) {
            Ok(events) => eprintln!("Hyperliquid Mainnet in-memory audit events:\n{events}"),
            Err(error) => eprintln!("failed to serialize in-memory audit events: {error}"),
        }
    }
    result
}

async fn mainnet_places_and_closes_doge_market_order_inner(audit: MemoryAuditSink) -> Result<()> {
    let account_address = std::env::var("QF_MAINNET_ACCOUNT_ADDRESS")
        .context("QF_MAINNET_ACCOUNT_ADDRESS is required")?
        .parse()
        .context("QF_MAINNET_ACCOUNT_ADDRESS must be a valid address")?;
    let private_key =
        std::env::var("QF_MAINNET_PRIVATE_KEY").context("QF_MAINNET_PRIVATE_KEY is required")?;
    let coin = HlCoin::new("DOGE");
    let signer = Arc::new(HyperliquidSigner::from_private_key(&private_key)?);
    let mut config =
        HlLiveBrokerConfig::new(StrategyId::new("mainnet-order-smoke"), account_address);
    config.network = HlNetwork::Mainnet;
    config.markets = vec![HlMarketConfig {
        coin: coin.clone(),
        leverage: 5,
        margin_mode: None,
    }];
    let ledger = MemoryLedgerSink::new();
    let ledger_reader = ledger.clone();
    let broker = HyperliquidLiveBroker::connect(
        config,
        signer,
        RiskGuard::new(RiskLimits::default()),
        Arc::new(
            RunJournal::new(JournalId::new("mainnet-order-smoke"), ledger).with_audit_sink(audit),
        ),
    )
    .await
    .context("connect to Hyperliquid Mainnet")?;

    if broker
        .position(&coin)
        .is_some_and(|position| position.size != Decimal::ZERO)
    {
        bail!("Mainnet DOGE position must be flat before running this test");
    }
    broker
        .wait_until_trading_ready()
        .await
        .context("wait for Mainnet broker market, account, and open-order state")?;
    let initial_snapshot_count = ledger_reader
        .events()
        .iter()
        .filter(|event| matches!(event.event, LedgerEventKind::EquitySnapshot { .. }))
        .count();
    let price = broker
        .mid_price(&coin)
        .context("read Mainnet DOGE mid price")?;
    let size_decimals = broker
        .metadata()
        .asset(&coin)
        .context("read Mainnet DOGE size precision")?
        .size_decimals;
    let order_size = minimum_order_size(price, size_decimals)?;
    assert!(order_size * price > Decimal::from(10));

    let open_decided_at = Instant::now();
    let open = broker
        .place_order(HlOrderRequest {
            coin: coin.clone(),
            side: Side::Buy,
            size: HlOrderSize::Exact(order_size),
            leverage: Some(1),
            reduce_only: false,
            order_type: HlOrderType::Market {
                max_slippage_bps: Some(100),
            },
            client_order_id: None,
            expires_after: None,
        })
        .await
        .context("place Mainnet DOGE market buy order")?;
    let open_submitted_at = Instant::now();
    assert_eq!(open.submitted.size, order_size);

    let opened_result = wait_for_position(&broker, &coin, true).await;
    let open_order_result = broker
        .wait_order_terminal(&open.submitted.client_order_id, POSITION_TIMEOUT)
        .await;
    let open_confirmed_at = Instant::now();
    let open_status_result = broker
        .order_status(open.submitted.client_order_id.as_str())
        .await;
    let connection_result = hold_connection_with_open_position(&broker, &coin).await;
    let close_decided_at = Instant::now();
    let close_result = broker
        .close_position(HlCloseRequest {
            coin: coin.clone(),
            size: HlCloseSize::Full,
            max_slippage_bps: Some(100),
            client_order_id: None,
            expires_after: None,
        })
        .await;
    let close_submitted_at = Instant::now();
    let close_order_result = match &close_result {
        Ok(close) => Some(
            broker
                .wait_order_terminal(&close.submitted.client_order_id, POSITION_TIMEOUT)
                .await,
        ),
        Err(_) => None,
    };
    let close_confirmed_at = Instant::now();
    let flat_result = wait_for_position(&broker, &coin, false).await;

    opened_result.context("confirm Mainnet DOGE position is open after order")?;
    let open_order =
        open_order_result.context("wait for Mainnet DOGE open order terminal state")?;
    assert!(matches!(open_order.state, HlTrackedOrderState::Filled));
    open_status_result.context("query Mainnet DOGE open order status")?;
    connection_result.context("keep Mainnet broker connected while holding DOGE")?;
    let close = close_result.context("submit Mainnet DOGE reduce-only close order")?;
    let close_order = close_order_result
        .expect("close order wait is created after submitting the close")
        .context("wait for Mainnet DOGE close order terminal state")?;
    assert_eq!(
        close_order.submitted.client_order_id,
        close.submitted.client_order_id
    );
    assert!(matches!(close_order.state, HlTrackedOrderState::Filled));
    flat_result.context("confirm Mainnet DOGE position is flat after close")?;
    println!(
        "Mainnet DOGE timing: open submit={}ms, open fill={}ms, close submit={}ms, close fill={}ms",
        open_submitted_at
            .duration_since(open_decided_at)
            .as_millis(),
        open_confirmed_at
            .duration_since(open_decided_at)
            .as_millis(),
        close_submitted_at
            .duration_since(close_decided_at)
            .as_millis(),
        close_confirmed_at
            .duration_since(close_decided_at)
            .as_millis(),
    );
    wait_for_ledger_facts(
        &ledger_reader,
        &coin,
        &open.submitted.client_order_id,
        &close.submitted.client_order_id,
        open.submitted.size,
        close.submitted.size,
        initial_snapshot_count,
    )
    .await?;
    Ok(())
}

async fn hold_connection_with_open_position(
    broker: &HyperliquidLiveBroker,
    coin: &HlCoin,
) -> Result<()> {
    let until = tokio::time::Instant::now() + CONNECTION_HOLD_DURATION;
    while tokio::time::Instant::now() < until {
        let price = broker
            .mid_price(coin)
            .context("receive a live DOGE mid price while holding position")?;
        if price <= Decimal::ZERO {
            bail!("live DOGE mid price must be positive");
        }
        let account = broker
            .account_state()
            .context("read live account state while holding position")?;
        let position = account
            .positions
            .get(coin)
            .context("live account state must retain the DOGE position")?;
        if position.size == Decimal::ZERO {
            bail!("live DOGE position unexpectedly became flat while holding");
        }
        if broker
            .open_orders_for(coin)
            .iter()
            .any(|order| order.reduce_only)
        {
            bail!("unexpected reduce-only DOGE order while holding position");
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}

async fn wait_for_position(
    broker: &HyperliquidLiveBroker,
    coin: &HlCoin,
    expected_open: bool,
) -> Result<()> {
    tokio::time::timeout(POSITION_TIMEOUT, async {
        loop {
            let is_open = broker
                .position(coin)
                .is_some_and(|position| position.size != Decimal::ZERO);
            if is_open == expected_open {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .context("wait for Mainnet position update")?;
    Ok(())
}

async fn wait_for_ledger_facts(
    ledger: &MemoryLedgerSink,
    coin: &HlCoin,
    open_client_order_id: &qf::hyperliquid::types::HlClientOrderId,
    close_client_order_id: &qf::hyperliquid::types::HlClientOrderId,
    open_size: Decimal,
    close_size: Decimal,
    initial_snapshot_count: usize,
) -> Result<()> {
    tokio::time::timeout(POSITION_TIMEOUT, async {
        loop {
            let events = ledger.events();
            let open_fill = events.iter().find(|event| {
                matches!(
                    &event.event,
                    LedgerEventKind::Fill {
                        client_order_id: Some(client_order_id),
                        symbol,
                        side: Side::Buy,
                        size,
                        price,
                        fee: Some(_),
                        reduce_only: Some(false),
                        ..
                    } if client_order_id == open_client_order_id.as_str()
                        && symbol == &coin.0
                        && *size == open_size
                        && *price > Decimal::ZERO
                )
            });
            let close_fill = events.iter().find(|event| {
                matches!(
                    &event.event,
                    LedgerEventKind::Fill {
                        client_order_id: Some(client_order_id),
                        symbol,
                        side: Side::Sell,
                        size,
                        price,
                        fee: Some(_),
                        reduce_only: Some(true),
                        ..
                    } if client_order_id == close_client_order_id.as_str()
                        && symbol == &coin.0
                        && *size == close_size
                        && *price > Decimal::ZERO
                )
            });
            let has_new_equity_snapshot = events
                .iter()
                .filter(|event| matches!(event.event, LedgerEventKind::EquitySnapshot { .. }))
                .count()
                > initial_snapshot_count;
            if let (Some(open_fill), Some(close_fill)) = (open_fill, close_fill)
                && has_new_equity_snapshot
            {
                assert!(open_fill.event_id.starts_with("hl-fill-"));
                assert!(close_fill.event_id.starts_with("hl-fill-"));
                assert!(open_fill.timestamp <= close_fill.timestamp);
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .context("wait for Mainnet DOGE order-correlated ledger fills and a new equity snapshot")?;
    Ok(())
}

fn minimum_order_size(price: Decimal, size_decimals: u32) -> Result<Decimal> {
    if price <= Decimal::ZERO {
        bail!("DOGE mid price must be positive");
    }
    let scale = Decimal::from(10_u64.pow(size_decimals));
    Ok(((Decimal::from(11) / price) * scale).ceil() / scale)
}
