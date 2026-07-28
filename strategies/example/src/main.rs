mod backtest;
mod live;
mod strategy;

use anyhow::Context;
use qf::core::Decimal;
use qf::hyperliquid::types::HlCoin;
use strategy::ExampleStrategy;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let mode = std::env::args()
        .nth(1)
        .context("usage: example-strategy <backtest|live>")?;
    let mut strategy = ExampleStrategy::new(HlCoin::new("BTC"), Decimal::ONE, 100);

    match mode.as_str() {
        "backtest" => {
            let prices = backtest_prices(300);
            let result = backtest::run(&mut strategy, &prices).await?;
            let report_path = "runs/example-backtest/performance.md";
            let report_dir = std::path::Path::new(report_path)
                .parent()
                .context("performance report path has no parent directory")?;
            std::fs::create_dir_all(report_dir)?;
            std::fs::write(report_path, result.performance.to_markdown())?;

            println!("backtest final BTC position: {:?}", result.final_position);
            println!("performance report: {report_path}");
            Ok(())
        }
        "live" => {
            let account_address = std::env::var("HL_ACCOUNT_ADDRESS")
                .context("HL_ACCOUNT_ADDRESS is required for live mode")?
                .parse()
                .context("HL_ACCOUNT_ADDRESS must be a valid address")?;
            let private_key = std::env::var("HL_PRIVATE_KEY")
                .context("HL_PRIVATE_KEY is required for live mode")?;
            live::run(strategy, account_address, &private_key).await
        }
        _ => anyhow::bail!("usage: example-strategy <backtest|live>"),
    }
}

fn backtest_prices(trade_count: usize) -> Vec<Decimal> {
    let mut prices = Vec::with_capacity(trade_count * 4);
    let mut state = 0x5eed_u64;
    for tick in 0..trade_count * 4 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let noise = ((state >> 32) % 7) as i64 - 3;
        let trend = (tick / 8) as i64;
        prices.push(Decimal::from(100 + trend + noise));
    }
    prices
}

#[cfg(test)]
fn deterministic_backtest_prices(trade_count: usize) -> Vec<Decimal> {
    let mut prices = Vec::with_capacity(trade_count * 4);
    let mut price = Decimal::from(100);
    for _ in 0..trade_count {
        prices.push(price);
        for _ in 0..3 {
            price += Decimal::ONE;
            prices.push(price);
        }
    }
    prices
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_reproducible_prices_with_trend_and_volatility() {
        let prices = backtest_prices(300);

        assert_eq!(prices, backtest_prices(300));
        assert!(prices.windows(2).any(|window| window[1] > window[0]));
        assert!(prices.windows(2).any(|window| window[1] < window[0]));
        assert!(prices.last() > prices.first());
    }

    #[test]
    fn deterministic_prices_keep_one_entry_and_three_hold_ticks_per_trade() {
        assert_eq!(deterministic_backtest_prices(300).len(), 1_200);
    }
}
