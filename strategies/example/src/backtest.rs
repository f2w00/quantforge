use std::collections::HashMap;
use std::sync::Arc;

use qf::audit::RunJournal;
use qf::core::{Decimal, JournalId, StrategyId};
use qf::hyperliquid::broker::state::HlBrokerState;
use qf::hyperliquid::types::HlAccountState;
use qf::hyperliquid::{HyperliquidBacktestBroker, HyperliquidBroker};
use qf::performance::PerformanceReport;
use qf::risk::{RiskGuard, RiskLimits};
use qf::storage::MemoryLedgerSink;

use crate::strategy::ExampleStrategy;

pub struct BacktestResult {
    pub final_position: Option<Decimal>,
    pub performance: PerformanceReport,
}

pub async fn run(
    strategy: &mut ExampleStrategy,
    prices: &[Decimal],
) -> anyhow::Result<BacktestResult> {
    let ledger_sink = MemoryLedgerSink::new();
    let broker = HyperliquidBacktestBroker::new(
        StrategyId::new("example"),
        HlBrokerState {
            account: HlAccountState {
                equity: Decimal::from(1_000),
                margin_used: Decimal::ZERO,
                positions: HashMap::new(),
                updated_at: chrono::Utc::now(),
            },
            open_orders: Vec::new(),
        },
        RiskGuard::new(RiskLimits::default()),
        Arc::new(RunJournal::new(
            JournalId::new("example-backtest"),
            ledger_sink.clone(),
        )),
    );
    broker.set_leverage(strategy.coin().clone(), 1)?;

    for &price in prices {
        broker.set_mark_price(strategy.coin().clone(), price)?;
        strategy.on_price(&broker, price).await?;
    }

    let performance = PerformanceReport::from_events(ledger_sink.events());
    Ok(BacktestResult {
        final_position: broker
            .position(strategy.coin())
            .await?
            .map(|position| position.size),
        performance,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use qf::hyperliquid::types::HlCoin;

    #[tokio::test]
    async fn repeats_entry_and_exit_cycles_and_generates_performance_report() {
        let mut strategy =
            ExampleStrategy::with_hold_ticks(HlCoin::new("BTC"), Decimal::from(2), 100, 2);

        let result = run(
            &mut strategy,
            &[
                Decimal::from(100),
                Decimal::from(102),
                Decimal::from(104),
                Decimal::from(103),
                Decimal::from(105),
                Decimal::from(107),
            ],
        )
        .await
        .unwrap();

        assert_eq!(result.final_position, None);
        assert_eq!(result.performance.closed_trades, 2);
        assert_eq!(result.performance.realized_pnl, Decimal::from(16));
    }

    #[tokio::test]
    async fn keeps_position_open_before_hold_ticks_are_reached() {
        let mut strategy =
            ExampleStrategy::with_hold_ticks(HlCoin::new("BTC"), Decimal::ONE, 100, 3);

        let result = run(&mut strategy, &[Decimal::from(100), Decimal::from(101)])
            .await
            .unwrap();

        assert_eq!(result.final_position, Some(Decimal::ONE));
        assert_eq!(result.performance.closed_trades, 0);
    }

    #[tokio::test]
    async fn completes_three_hundred_trade_cycles() {
        let mut strategy = ExampleStrategy::new(HlCoin::new("BTC"), Decimal::ONE, 100);
        let mut prices = Vec::with_capacity(1_200);
        let mut price = Decimal::from(100);
        for _ in 0..300 {
            prices.push(price);
            for _ in 0..3 {
                price += Decimal::ONE;
                prices.push(price);
            }
        }

        let result = run(&mut strategy, &prices).await.unwrap();

        assert_eq!(result.final_position, None);
        assert_eq!(result.performance.closed_trades, 300);
        assert_eq!(result.performance.realized_pnl, Decimal::from(900));
    }
}
