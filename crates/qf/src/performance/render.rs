use crate::core::Decimal;

use super::PerformanceReport;

impl PerformanceReport {
    /// 将报告渲染为适合终端、文档和代码评审阅读的 Markdown。
    pub fn to_markdown(&self) -> String {
        format!(
            "# Performance Report\n\
\n\
## Returns\n\
\n\
| Metric | Value |\n\
| --- | ---: |\n\
| Initial equity | {} |\n\
| Final equity | {} |\n\
| Total PnL | {} |\n\
| Total return | {} |\n\
\n\
## Trades\n\
\n\
| Metric | Value |\n\
| --- | ---: |\n\
| Closed trades | {} |\n\
| Wins / losses / breakeven | {} / {} / {} |\n\
| Win rate | {} |\n\
| Average win | {} |\n\
| Average loss | {} |\n\
| Profit factor | {} |\n\
| Expectancy | {} |\n\
\n\
## Activity\n\
\n\
| Metric | Value |\n\
| --- | ---: |\n\
| Fills | {} |\n\
| Buy / sell fills | {} / {} |\n\
| Opened / closed positions | {} / {} |\n\
| Total volume | {} |\n\
| Total notional | {} |\n\
| First fill | {} |\n\
| Last fill | {} |\n\
| Active duration | {} |\n\
| Average fill interval | {} |\n\
| Fills per day | {} |\n\
\n\
## PnL\n\
\n\
| Metric | Value |\n\
| --- | ---: |\n\
| Realized PnL | {} |\n\
| Unrealized PnL | {} |\n\
| Funding PnL | {} |\n\
\n\
## Costs\n\
\n\
| Metric | Value |\n\
| --- | ---: |\n\
| Trading fees | {} |\n\
| Known / missing fee values | {} / {} |\n\
| Funding income | {} |\n\
| Funding expense | {} |\n\
| Total cost | {} |\n\
| Effective fee rate | {} |\n\
| Cost rate | {} |\n\
\n\
## Risk\n\
\n\
| Metric | Value |\n\
| --- | ---: |\n\
| Max drawdown | {} |\n\
| Max drawdown rate | {} |\n\
| Liquidations | {} |\n\
\n\
## Data Quality\n\
\n\
| Metric | Value |\n\
| --- | ---: |\n\
| Ledger events | {} |\n\
| Duplicate events removed | {} |\n\
| Equity snapshots available | {} |\n",
            optional_amount(self.initial_equity, false),
            optional_amount(self.final_equity, false),
            optional_amount(self.total_pnl, true),
            optional_percentage(self.total_return, true),
            self.closed_trades,
            self.winning_trades,
            self.losing_trades,
            self.breakeven_trades,
            optional_percentage(self.win_rate, false),
            optional_amount(self.average_win, true),
            optional_amount(self.average_loss, true),
            optional_number(self.profit_factor),
            optional_amount(self.expectancy, true),
            self.trading.fill_count,
            self.trading.buy_fill_count,
            self.trading.sell_fill_count,
            self.trading.opened_position_count,
            self.trading.closed_position_count,
            amount(self.trading.total_volume, false),
            amount(self.trading.total_notional, false),
            optional_timestamp(self.trading.first_fill_at),
            optional_timestamp(self.trading.last_fill_at),
            optional_duration(self.trading.active_duration_seconds),
            optional_seconds(self.trading.average_fill_interval_seconds),
            optional_number(self.trading.fills_per_day),
            amount(self.realized_pnl, true),
            optional_amount(self.unrealized_pnl, true),
            amount(self.funding_pnl, true),
            amount(self.costs.trading_fees, false),
            self.costs.known_fee_count,
            self.costs.missing_fee_count,
            amount(self.costs.funding_income, false),
            amount(self.costs.funding_expense, false),
            amount(self.costs.total_cost, false),
            optional_percentage(self.costs.effective_fee_rate, false),
            optional_percentage(self.costs.cost_rate, false),
            optional_amount(self.max_drawdown, false),
            optional_percentage(self.max_drawdown_pct, false),
            self.liquidation_count,
            self.event_count,
            self.data_quality.duplicate_event_count,
            yes_no(self.data_quality.has_equity_snapshots),
        )
    }

    /// 返回保留原始数值类型、可反序列化回 `PerformanceReport` 的格式化 JSON。
    pub fn to_pretty_json(&self) -> serde_json::Result<String> {
        serde_json::to_string_pretty(self)
    }
}

fn optional_amount(value: Option<Decimal>, signed: bool) -> String {
    value.map(|value| amount(value, signed)).unwrap_or_else(na)
}

fn optional_percentage(value: Option<Decimal>, signed: bool) -> String {
    value
        .map(|value| percentage(value, signed))
        .unwrap_or_else(na)
}

fn optional_number(value: Option<Decimal>) -> String {
    value.map(number).unwrap_or_else(na)
}

fn optional_timestamp(value: Option<chrono::DateTime<chrono::Utc>>) -> String {
    value.map(|value| value.to_rfc3339()).unwrap_or_else(na)
}

fn optional_duration(value: Option<u64>) -> String {
    value.map(|value| format!("{value}s")).unwrap_or_else(na)
}

fn optional_seconds(value: Option<Decimal>) -> String {
    value
        .map(|value| format!("{}s", number(value)))
        .unwrap_or_else(na)
}

fn amount(value: Decimal, signed: bool) -> String {
    let formatted = grouped_fixed(value, 2);
    if signed && value.is_sign_positive() {
        format!("+{formatted}")
    } else {
        formatted
    }
}

fn percentage(value: Decimal, signed: bool) -> String {
    format!("{}%", amount(value * Decimal::from(100), signed))
}

fn number(value: Decimal) -> String {
    grouped_fixed(value, 2)
}

fn grouped_fixed(value: Decimal, decimal_places: usize) -> String {
    let formatted = format!("{value:.decimal_places$}");
    let (sign, unsigned) = formatted
        .strip_prefix('-')
        .map_or(("", formatted.as_str()), |value| ("-", value));
    let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let grouped_integer = integer
        .chars()
        .rev()
        .enumerate()
        .fold(String::new(), |mut output, (index, character)| {
            if index != 0 && index % 3 == 0 {
                output.push(',');
            }
            output.push(character);
            output
        })
        .chars()
        .rev()
        .collect::<String>();

    format!("{sign}{grouped_integer}.{fraction}")
}

fn yes_no(value: bool) -> &'static str {
    if value { "Yes" } else { "No" }
}

fn na() -> String {
    "N/A".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::performance::{CostStatistics, PerformanceDataQuality, TradingStatistics};

    fn report() -> PerformanceReport {
        PerformanceReport {
            event_count: 1_234,
            initial_equity: Some(Decimal::from(1_000)),
            final_equity: Some(Decimal::new(1_120_500, 3)),
            total_pnl: Some(Decimal::new(120_500, 3)),
            total_return: Some(Decimal::new(1205, 4)),
            realized_pnl: Decimal::from(100),
            unrealized_pnl: Some(Decimal::new(20_500, 3)),
            funding_pnl: Decimal::new(-180, 2),
            closed_trades: 12,
            winning_trades: 7,
            losing_trades: 4,
            breakeven_trades: 1,
            gross_profit: Decimal::from(126),
            gross_loss: Decimal::from(-42),
            win_rate: Some(Decimal::new(6364, 4)),
            average_win: Some(Decimal::from(18)),
            average_loss: Some(Decimal::new(-105, 1)),
            profit_factor: Some(Decimal::new(185, 2)),
            expectancy: Some(Decimal::new(842, 2)),
            max_drawdown: Some(Decimal::from(45)),
            max_drawdown_pct: Some(Decimal::new(410, 4)),
            liquidation_count: 0,
            trading: TradingStatistics {
                fill_count: 24,
                buy_fill_count: 13,
                sell_fill_count: 11,
                opened_position_count: 12,
                closed_position_count: 12,
                total_volume: Decimal::from(240),
                total_notional: Decimal::from(12_000),
                first_fill_at: None,
                last_fill_at: None,
                active_duration_seconds: Some(3_600),
                average_fill_interval_seconds: Some(Decimal::new(1565, 2)),
                fills_per_day: Some(Decimal::from(24)),
            },
            costs: CostStatistics {
                trading_fees: Decimal::new(820, 2),
                known_fee_count: 23,
                missing_fee_count: 1,
                funding_income: Decimal::from(3),
                funding_expense: Decimal::new(480, 2),
                total_cost: Decimal::new(10, 1),
                effective_fee_rate: Some(Decimal::new(683, 5)),
                cost_rate: Some(Decimal::new(833, 5)),
            },
            data_quality: PerformanceDataQuality {
                duplicate_event_count: 2,
                has_equity_snapshots: true,
            },
        }
    }

    #[test]
    fn renders_human_readable_markdown() {
        let markdown = report().to_markdown();

        assert!(markdown.contains("| Total PnL | +120.50 |"));
        assert!(markdown.contains("| Total return | +12.05% |"));
        assert!(markdown.contains("| Funding PnL | -1.80 |"));
        assert!(markdown.contains("## Activity"));
        assert!(markdown.contains("| Fills | 24 |"));
        assert!(markdown.contains("## Costs"));
        assert!(markdown.contains("| Trading fees | 8.20 |"));
        assert!(markdown.contains("| Ledger events | 1234 |"));
    }

    #[test]
    fn renders_missing_values_as_na_in_markdown() {
        let markdown = PerformanceReport::default().to_markdown();

        assert!(markdown.contains("| Initial equity | N/A |"));
        assert!(markdown.contains("| Profit factor | N/A |"));
    }

    #[test]
    fn pretty_json_round_trips_to_the_original_report() {
        let report = report();
        let json = report.to_pretty_json().unwrap();
        let decoded: PerformanceReport = serde_json::from_str(&json).unwrap();

        assert!(json.contains("\n  \"event_count\""));
        assert_eq!(decoded.total_pnl, report.total_pnl);
        assert_eq!(decoded.total_return, report.total_return);
        assert_eq!(
            decoded.costs.missing_fee_count,
            report.costs.missing_fee_count
        );
    }
}
