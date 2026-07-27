use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::audit::{LedgerEvent, LedgerEventKind};
use crate::core::{Decimal, Side};

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
    pub trading_fees: Decimal,
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
    pub data_quality: PerformanceDataQuality,
}

/// 报告中缺失或被去重的账本数据状态。
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct PerformanceDataQuality {
    pub duplicate_event_count: usize,
    pub missing_fee_count: usize,
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
                    let fee = match fee {
                        Some(fee) => fee,
                        None => {
                            report.data_quality.missing_fee_count += 1;
                            Decimal::ZERO
                        }
                    };
                    report.trading_fees += fee;
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
                        report.trading_fees += fee;
                    } else {
                        report.data_quality.missing_fee_count += 1;
                    }
                    if let (Some(symbol), Some(realized_pnl)) = (symbol, realized_pnl) {
                        if let Some(mut trade) = open_trades.remove(&symbol) {
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
                        report.trading_fees = trading_fees;
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
        report
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
                Self::close_trade(report, trade);
            } else {
                open_trades.insert(symbol, trade);
            }
            return;
        }

        let close_fee = fee * closed_size / signed_size.abs();
        trade.fees += close_fee;
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
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;
    use crate::core::{RunMode, StrategyId};

    fn event(id: &str, second: u32, event: LedgerEventKind) -> LedgerEvent {
        LedgerEvent {
            event_id: id.to_string(),
            strategy_id: StrategyId::new("strategy-1"),
            mode: RunMode::Backtest,
            exchange: "hyperliquid".to_string(),
            timestamp: chrono::Utc
                .with_ymd_and_hms(2026, 1, 1, 0, 0, second)
                .unwrap(),
            event,
        }
    }

    fn fill(id: &str, second: u32, side: Side, size: i64, price: i64, fee: i64) -> LedgerEvent {
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

        assert_eq!(report.data_quality.missing_fee_count, 1);
        assert_eq!(report.trading_fees, Decimal::ZERO);
    }
}
