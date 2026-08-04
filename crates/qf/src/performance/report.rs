use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::audit::{LedgerEvent, LedgerEventKind};
use crate::core::{Decimal, Side, Timestamp};

/// 从账本事件回放得到的策略绩效投影。
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct PerformanceReport {
    pub event_count: usize,
    pub initial_equity: Option<Decimal>,
    pub final_equity: Option<Decimal>,
    pub total_pnl: Option<Decimal>,
    pub total_return: Option<Decimal>,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Option<Decimal>,
    pub funding_pnl: Decimal,
    pub closed_trades: u64,
    pub winning_trades: u64,
    pub losing_trades: u64,
    pub breakeven_trades: u64,
    pub gross_profit: Decimal,
    pub gross_loss: Decimal,
    pub win_rate: Option<Decimal>,
    pub average_win: Option<Decimal>,
    pub average_loss: Option<Decimal>,
    pub profit_factor: Option<Decimal>,
    pub expectancy: Option<Decimal>,
    pub max_drawdown: Option<Decimal>,
    pub max_drawdown_pct: Option<Decimal>,
    pub liquidation_count: u64,
    pub trading: TradingStatistics,
    pub costs: CostStatistics,
    pub data_quality: PerformanceDataQuality,
}

/// 基于实际成交回放得到的交易活动统计。
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TradingStatistics {
    pub fill_count: u64,
    pub buy_fill_count: u64,
    pub sell_fill_count: u64,
    pub opened_position_count: u64,
    pub closed_position_count: u64,
    pub total_volume: Decimal,
    pub total_notional: Decimal,
    pub first_fill_at: Option<Timestamp>,
    pub last_fill_at: Option<Timestamp>,
    pub active_duration_seconds: Option<u64>,
    pub average_fill_interval_seconds: Option<Decimal>,
    pub fills_per_day: Option<Decimal>,
}

/// 基于账本已知费用数据回放得到的成本统计。
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct CostStatistics {
    pub trading_fees: Decimal,
    pub known_fee_count: u64,
    pub missing_fee_count: u64,
    pub funding_income: Decimal,
    pub funding_expense: Decimal,
    pub total_cost: Decimal,
    pub effective_fee_rate: Option<Decimal>,
    pub cost_rate: Option<Decimal>,
}

/// 报告中缺失或被去重的账本数据状态。
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct PerformanceDataQuality {
    pub duplicate_event_count: usize,
    pub has_equity_snapshots: bool,
}

#[derive(Clone, Debug)]
struct OpenTrade {
    signed_size: Decimal,
    entry_price: Decimal,
    realized_pnl: Decimal,
    fees: Decimal,
    funding_pnl: Decimal,
}

impl PerformanceReport {
    /// 去重并按事件时间回放账本，生成不修改原始数据的绩效报告。
    pub fn from_events(events: impl IntoIterator<Item = LedgerEvent>) -> Self {
        let mut events: Vec<_> = events.into_iter().collect();
        events.sort_by(|left, right| {
            left.timestamp
                .cmp(&right.timestamp)
                .then_with(|| left.event_id.cmp(&right.event_id))
        });

        let mut report = Self::default();
        let mut seen_event_ids = HashSet::new();
        let mut open_trades = HashMap::new();
        let mut peak_equity = None;

        for event in events {
            if !seen_event_ids.insert(event.event_id) {
                report.data_quality.duplicate_event_count += 1;
                continue;
            }
            report.event_count += 1;

            match event.event {
                LedgerEventKind::Fill {
                    symbol,
                    side,
                    size,
                    price,
                    fee,
                    ..
                } => {
                    report.record_fill(&side, size, price, event.timestamp);
                    let fee = match fee {
                        Some(fee) => {
                            report.costs.known_fee_count += 1;
                            fee
                        }
                        None => {
                            report.costs.missing_fee_count += 1;
                            Decimal::ZERO
                        }
                    };
                    report.costs.trading_fees += fee;
                    Self::apply_fill(
                        &mut report,
                        &mut open_trades,
                        symbol,
                        side,
                        size,
                        price,
                        fee,
                    );
                }
                LedgerEventKind::Funding {
                    symbol, cashflow, ..
                } => {
                    report.funding_pnl += cashflow;
                    if cashflow.is_sign_positive() {
                        report.costs.funding_income += cashflow;
                    } else {
                        report.costs.funding_expense -= cashflow;
                    }
                    if let Some(trade) = open_trades.get_mut(&symbol) {
                        trade.funding_pnl += cashflow;
                    }
                }
                LedgerEventKind::Liquidation {
                    symbol,
                    realized_pnl,
                    fee,
                    ..
                } => {
                    report.liquidation_count += 1;
                    if let Some(fee) = fee {
                        report.costs.trading_fees += fee;
                        report.costs.known_fee_count += 1;
                    } else {
                        report.costs.missing_fee_count += 1;
                    }
                    if let (Some(symbol), Some(realized_pnl)) = (symbol, realized_pnl) {
                        if let Some(mut trade) = open_trades.remove(&symbol) {
                            report.trading.closed_position_count += 1;
                            trade.realized_pnl += realized_pnl;
                            report.realized_pnl += realized_pnl;
                            if let Some(fee) = fee {
                                trade.fees += fee;
                            }
                            Self::close_trade(&mut report, trade);
                        } else {
                            report.realized_pnl += realized_pnl;
                        }
                    }
                }
                LedgerEventKind::EquitySnapshot {
                    equity,
                    realized_pnl,
                    unrealized_pnl,
                    trading_fees,
                    funding_pnl,
                    ..
                } => {
                    report.data_quality.has_equity_snapshots = true;
                    report.initial_equity.get_or_insert(equity);
                    report.final_equity = Some(equity);
                    if let Some(peak) = peak_equity {
                        let drawdown = peak - equity;
                        if drawdown > report.max_drawdown.unwrap_or(Decimal::ZERO) {
                            report.max_drawdown = Some(drawdown);
                            report.max_drawdown_pct =
                                (peak != Decimal::ZERO).then(|| drawdown / peak);
                        }
                        if equity > peak {
                            peak_equity = Some(equity);
                        }
                    } else {
                        peak_equity = Some(equity);
                    }
                    if let Some(realized_pnl) = realized_pnl {
                        report.realized_pnl = realized_pnl;
                    }
                    report.unrealized_pnl = unrealized_pnl;
                    if let Some(trading_fees) = trading_fees {
                        report.costs.trading_fees = trading_fees;
                    }
                    if let Some(funding_pnl) = funding_pnl {
                        report.funding_pnl = funding_pnl;
                    }
                }
            }
        }

        if let (Some(initial_equity), Some(final_equity)) =
            (report.initial_equity, report.final_equity)
        {
            report.total_pnl = Some(final_equity - initial_equity);
            report.total_return = (initial_equity != Decimal::ZERO)
                .then(|| (final_equity - initial_equity) / initial_equity);
        }
        report.finish_trade_metrics();
        report.finish_statistics();
        report
    }

    fn record_fill(&mut self, side: &Side, size: Decimal, price: Decimal, timestamp: Timestamp) {
        self.trading.fill_count += 1;
        match side {
            Side::Buy => self.trading.buy_fill_count += 1,
            Side::Sell => self.trading.sell_fill_count += 1,
        }
        self.trading.total_volume += size.abs();
        self.trading.total_notional += size.abs() * price;
        self.trading.first_fill_at.get_or_insert(timestamp.clone());
        self.trading.last_fill_at = Some(timestamp);
    }

    fn apply_fill(
        report: &mut Self,
        open_trades: &mut HashMap<String, OpenTrade>,
        symbol: String,
        side: Side,
        size: Decimal,
        price: Decimal,
        fee: Decimal,
    ) {
        let signed_size = match side {
            Side::Buy => size,
            Side::Sell => -size,
        };
        let Some(mut trade) = open_trades.remove(&symbol) else {
            open_trades.insert(
                symbol,
                OpenTrade {
                    signed_size,
                    entry_price: price,
                    realized_pnl: Decimal::ZERO,
                    fees: fee,
                    funding_pnl: Decimal::ZERO,
                },
            );
            report.trading.opened_position_count += 1;
            return;
        };

        if trade.signed_size.is_sign_positive() == signed_size.is_sign_positive() {
            let new_size = trade.signed_size + signed_size;
            trade.entry_price = (trade.entry_price * trade.signed_size.abs()
                + price * signed_size.abs())
                / new_size.abs();
            trade.signed_size = new_size;
            trade.fees += fee;
            open_trades.insert(symbol, trade);
            return;
        }

        let closed_size = trade.signed_size.abs().min(signed_size.abs());
        let direction = if trade.signed_size.is_sign_positive() {
            Decimal::ONE
        } else {
            -Decimal::ONE
        };
        let realized_pnl = (price - trade.entry_price) * direction * closed_size;
        trade.realized_pnl += realized_pnl;
        report.realized_pnl += realized_pnl;
        let remaining_fill = signed_size.abs() - closed_size;

        if remaining_fill == Decimal::ZERO {
            trade.signed_size += signed_size;
            trade.fees += fee;
            if trade.signed_size == Decimal::ZERO {
                report.trading.closed_position_count += 1;
                Self::close_trade(report, trade);
            } else {
                open_trades.insert(symbol, trade);
            }
            return;
        }

        let close_fee = fee * closed_size / signed_size.abs();
        trade.fees += close_fee;
        report.trading.closed_position_count += 1;
        Self::close_trade(report, trade);
        open_trades.insert(
            symbol,
            OpenTrade {
                signed_size: if signed_size.is_sign_positive() {
                    remaining_fill
                } else {
                    -remaining_fill
                },
                entry_price: price,
                realized_pnl: Decimal::ZERO,
                fees: fee - close_fee,
                funding_pnl: Decimal::ZERO,
            },
        );
        report.trading.opened_position_count += 1;
    }

    fn close_trade(report: &mut Self, trade: OpenTrade) {
        report.closed_trades += 1;
        let net_pnl = trade.realized_pnl + trade.funding_pnl - trade.fees;
        if net_pnl > Decimal::ZERO {
            report.winning_trades += 1;
            report.gross_profit += net_pnl;
        } else if net_pnl < Decimal::ZERO {
            report.losing_trades += 1;
            report.gross_loss += net_pnl;
        } else {
            report.breakeven_trades += 1;
        }
    }

    fn finish_trade_metrics(&mut self) {
        let decided_trades = self.winning_trades + self.losing_trades;
        self.win_rate = (decided_trades != 0)
            .then(|| Decimal::from(self.winning_trades) / Decimal::from(decided_trades));
        self.average_win = (self.winning_trades != 0)
            .then(|| self.gross_profit / Decimal::from(self.winning_trades));
        self.average_loss =
            (self.losing_trades != 0).then(|| self.gross_loss / Decimal::from(self.losing_trades));
        self.profit_factor =
            (self.gross_loss != Decimal::ZERO).then(|| self.gross_profit / -self.gross_loss);
        self.expectancy = (self.closed_trades != 0)
            .then(|| (self.gross_profit + self.gross_loss) / Decimal::from(self.closed_trades));
    }

    fn finish_statistics(&mut self) {
        let trading = &mut self.trading;
        if let (Some(first_fill_at), Some(last_fill_at)) =
            (trading.first_fill_at, trading.last_fill_at)
        {
            let active_duration = (last_fill_at - first_fill_at).num_seconds() as u64;
            trading.active_duration_seconds = Some(active_duration);
            trading.average_fill_interval_seconds = (trading.fill_count > 1)
                .then(|| Decimal::from(active_duration) / Decimal::from(trading.fill_count - 1));
            let active_days =
                (last_fill_at.date_naive() - first_fill_at.date_naive()).num_days() + 1;
            trading.fills_per_day =
                Some(Decimal::from(trading.fill_count) / Decimal::from(active_days));
        }

        self.costs.total_cost =
            self.costs.trading_fees + self.costs.funding_expense - self.costs.funding_income;
        self.costs.effective_fee_rate = (trading.total_notional != Decimal::ZERO)
            .then(|| self.costs.trading_fees / trading.total_notional);
        self.costs.cost_rate = (trading.total_notional != Decimal::ZERO)
            .then(|| self.costs.total_cost / trading.total_notional);
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;
    use crate::core::{RunMode, StrategyId};

    fn event(id: &str, second: i64, event: LedgerEventKind) -> LedgerEvent {
        LedgerEvent {
            event_id: id.to_string(),
            strategy_id: StrategyId::new("strategy-1"),
            mode: RunMode::Backtest,
            exchange: "hyperliquid".to_string(),
            timestamp: chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
                + chrono::Duration::seconds(second),
            event,
        }
    }

    fn fill(id: &str, second: i64, side: Side, size: i64, price: i64, fee: i64) -> LedgerEvent {
        event(
            id,
            second,
            LedgerEventKind::Fill {
                order_id: None,
                client_order_id: None,
                symbol: "BTC".to_string(),
                side,
                size: Decimal::from(size),
                price: Decimal::from(price),
                fee: Some(Decimal::from(fee)),
                reduce_only: None,
            },
        )
    }

    #[test]
    fn calculates_trade_metrics_for_scaled_position() {
        let report = PerformanceReport::from_events([
            fill("1", 1, Side::Buy, 1, 100, 1),
            fill("2", 2, Side::Buy, 1, 120, 1),
            fill("3", 3, Side::Sell, 2, 130, 2),
        ]);

        assert_eq!(report.closed_trades, 1);
        assert_eq!(report.winning_trades, 1);
        assert_eq!(report.realized_pnl, Decimal::from(40));
        assert_eq!(report.gross_profit, Decimal::from(36));
        assert_eq!(report.win_rate, Some(Decimal::ONE));
    }

    #[test]
    fn closes_then_opens_a_new_trade_when_position_reverses() {
        let report = PerformanceReport::from_events([
            fill("1", 1, Side::Buy, 1, 100, 0),
            fill("2", 2, Side::Sell, 2, 90, 0),
            fill("3", 3, Side::Buy, 1, 80, 0),
        ]);

        assert_eq!(report.closed_trades, 2);
        assert_eq!(report.losing_trades, 1);
        assert_eq!(report.winning_trades, 1);
        assert_eq!(report.realized_pnl, Decimal::ZERO);
    }

    #[test]
    fn recognizes_realized_pnl_before_a_position_is_fully_closed() {
        let report = PerformanceReport::from_events([
            fill("1", 1, Side::Buy, 2, 100, 0),
            fill("2", 2, Side::Sell, 1, 120, 0),
        ]);

        assert_eq!(report.closed_trades, 0);
        assert_eq!(report.realized_pnl, Decimal::from(20));
    }

    #[test]
    fn deduplicates_events_and_calculates_equity_return_and_drawdown() {
        let initial = event(
            "snapshot-1",
            1,
            LedgerEventKind::EquitySnapshot {
                equity: Decimal::from(100),
                margin_used: Decimal::ZERO,
                realized_pnl: Some(Decimal::ZERO),
                unrealized_pnl: Some(Decimal::ZERO),
                trading_fees: Some(Decimal::ZERO),
                funding_pnl: Some(Decimal::ZERO),
            },
        );
        let trough = event(
            "snapshot-2",
            2,
            LedgerEventKind::EquitySnapshot {
                equity: Decimal::from(80),
                margin_used: Decimal::ZERO,
                realized_pnl: Some(Decimal::ZERO),
                unrealized_pnl: Some(Decimal::ZERO),
                trading_fees: Some(Decimal::ZERO),
                funding_pnl: Some(Decimal::ZERO),
            },
        );
        let final_snapshot = event(
            "snapshot-3",
            3,
            LedgerEventKind::EquitySnapshot {
                equity: Decimal::from(110),
                margin_used: Decimal::ZERO,
                realized_pnl: Some(Decimal::from(10)),
                unrealized_pnl: Some(Decimal::ZERO),
                trading_fees: Some(Decimal::ZERO),
                funding_pnl: Some(Decimal::ZERO),
            },
        );

        let report =
            PerformanceReport::from_events([final_snapshot, initial.clone(), trough, initial]);

        assert_eq!(report.event_count, 3);
        assert_eq!(report.data_quality.duplicate_event_count, 1);
        assert_eq!(report.total_pnl, Some(Decimal::from(10)));
        assert_eq!(report.total_return, Some(Decimal::new(1, 1)));
        assert_eq!(report.max_drawdown, Some(Decimal::from(20)));
        assert_eq!(report.max_drawdown_pct, Some(Decimal::new(2, 1)));
    }

    #[test]
    fn records_missing_fees_without_treating_them_as_known_zero() {
        let report = PerformanceReport::from_events([event(
            "fill-1",
            1,
            LedgerEventKind::Fill {
                order_id: None,
                client_order_id: None,
                symbol: "BTC".to_string(),
                side: Side::Buy,
                size: Decimal::ONE,
                price: Decimal::from(100),
                fee: None,
                reduce_only: None,
            },
        )]);

        assert_eq!(report.costs.missing_fee_count, 1);
        assert_eq!(report.costs.known_fee_count, 0);
        assert_eq!(report.costs.trading_fees, Decimal::ZERO);
        assert_eq!(report.costs.effective_fee_rate, Some(Decimal::ZERO));
    }

    #[test]
    fn calculates_trading_activity_and_cost_statistics() {
        let report = PerformanceReport::from_events([
            fill("1", 0, Side::Buy, 2, 100, 1),
            fill("2", 60, Side::Buy, 1, 120, 2),
            fill("3", 86_460, Side::Sell, 3, 130, 3),
            event(
                "funding-1",
                86_461,
                LedgerEventKind::Funding {
                    symbol: "BTC".to_string(),
                    funding_rate: None,
                    settlement_price: None,
                    cashflow: Decimal::from(-4),
                },
            ),
        ]);

        assert_eq!(report.trading.fill_count, 3);
        assert_eq!(report.trading.buy_fill_count, 2);
        assert_eq!(report.trading.sell_fill_count, 1);
        assert_eq!(report.trading.opened_position_count, 1);
        assert_eq!(report.trading.closed_position_count, 1);
        assert_eq!(report.trading.total_volume, Decimal::from(6));
        assert_eq!(report.trading.total_notional, Decimal::from(710));
        assert_eq!(report.trading.active_duration_seconds, Some(86_460));
        assert_eq!(
            report.trading.average_fill_interval_seconds,
            Some(Decimal::from(43_230))
        );
        assert_eq!(report.trading.fills_per_day, Some(Decimal::new(15, 1)));
        assert_eq!(report.costs.trading_fees, Decimal::from(6));
        assert_eq!(report.costs.known_fee_count, 3);
        assert_eq!(report.costs.funding_expense, Decimal::from(4));
        assert_eq!(report.costs.total_cost, Decimal::from(10));
        assert_eq!(
            report.costs.effective_fee_rate,
            Some(Decimal::from(6) / Decimal::from(710))
        );
    }
}
