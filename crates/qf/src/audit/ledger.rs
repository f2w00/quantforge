use serde::{Deserialize, Serialize};

use crate::core::{Decimal, RunId, RunMode, Side, StrategyId, Timestamp};

/// 跨运行模式的不可变交易账本事实。
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LedgerEvent {
    /// 事件源生成的稳定唯一标识，用于回放去重。
    pub event_id: String,
    pub run_id: RunId,
    pub strategy_id: StrategyId,
    pub mode: RunMode,
    pub exchange: String,
    /// 经济事件实际发生的时间，而非本地写入时间。
    pub timestamp: Timestamp,
    pub event: LedgerEventKind,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum LedgerEventKind {
    Fill {
        fill_id: Option<String>,
        order_id: Option<String>,
        client_order_id: Option<String>,
        symbol: String,
        side: Side,
        size: Decimal,
        price: Decimal,
        /// None 表示来源未提供手续费，不能按零处理。
        fee: Option<Decimal>,
        reduce_only: bool,
    },
    Funding {
        settlement_id: Option<String>,
        symbol: String,
        funding_rate: Decimal,
        settlement_price: Decimal,
        cashflow: Decimal,
    },
    Liquidation {
        liquidation_id: Option<String>,
        symbol: Option<String>,
        size: Option<Decimal>,
        price: Option<Decimal>,
        realized_pnl: Decimal,
        fee: Option<Decimal>,
        reason: String,
    },
    EquitySnapshot {
        equity: Decimal,
        margin_used: Decimal,
        realized_pnl: Decimal,
        unrealized_pnl: Decimal,
        trading_fees: Decimal,
        funding_pnl: Decimal,
    },
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn round_trips_every_ledger_event_kind() {
        let timestamp = chrono::Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
        let events = [
            LedgerEvent {
                event_id: "fill-1".to_string(),
                run_id: RunId::new("run-1"),
                strategy_id: StrategyId::new("strategy-1"),
                mode: RunMode::Backtest,
                exchange: "hyperliquid".to_string(),
                timestamp,
                event: LedgerEventKind::Fill {
                    fill_id: Some("fill-1".to_string()),
                    order_id: Some("order-1".to_string()),
                    client_order_id: Some("client-1".to_string()),
                    symbol: "BTC".to_string(),
                    side: Side::Buy,
                    size: Decimal::new(2, 0),
                    price: Decimal::new(100, 0),
                    fee: Some(Decimal::new(4, 1)),
                    reduce_only: false,
                },
            },
            LedgerEvent {
                event_id: "funding-1".to_string(),
                run_id: RunId::new("run-1"),
                strategy_id: StrategyId::new("strategy-1"),
                mode: RunMode::Backtest,
                exchange: "hyperliquid".to_string(),
                timestamp,
                event: LedgerEventKind::Funding {
                    settlement_id: Some("settlement-1".to_string()),
                    symbol: "BTC".to_string(),
                    funding_rate: Decimal::new(1, 3),
                    settlement_price: Decimal::new(100, 0),
                    cashflow: Decimal::new(-2, 1),
                },
            },
            LedgerEvent {
                event_id: "liquidation-1".to_string(),
                run_id: RunId::new("run-1"),
                strategy_id: StrategyId::new("strategy-1"),
                mode: RunMode::Backtest,
                exchange: "hyperliquid".to_string(),
                timestamp,
                event: LedgerEventKind::Liquidation {
                    liquidation_id: Some("liquidation-1".to_string()),
                    symbol: Some("BTC".to_string()),
                    size: Some(Decimal::ONE),
                    price: Some(Decimal::new(50, 0)),
                    realized_pnl: Decimal::new(-50, 0),
                    fee: None,
                    reason: "maintenance_margin_breach".to_string(),
                },
            },
            LedgerEvent {
                event_id: "snapshot-1".to_string(),
                run_id: RunId::new("run-1"),
                strategy_id: StrategyId::new("strategy-1"),
                mode: RunMode::Backtest,
                exchange: "hyperliquid".to_string(),
                timestamp,
                event: LedgerEventKind::EquitySnapshot {
                    equity: Decimal::new(1_000, 0),
                    margin_used: Decimal::new(100, 0),
                    realized_pnl: Decimal::new(10, 0),
                    unrealized_pnl: Decimal::new(-5, 0),
                    trading_fees: Decimal::new(1, 0),
                    funding_pnl: Decimal::new(-2, 0),
                },
            },
        ];

        for event in events {
            let encoded = serde_json::to_string(&event).unwrap();
            let decoded: LedgerEvent = serde_json::from_str(&encoded).unwrap();
            assert_eq!(decoded.event_id, event.event_id);
            assert_eq!(decoded.timestamp, event.timestamp);
            assert_eq!(
                serde_json::to_value(decoded.event).unwrap(),
                serde_json::to_value(event.event).unwrap()
            );
        }
    }
}
