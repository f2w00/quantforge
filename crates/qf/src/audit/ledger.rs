use serde::{Deserialize, Serialize};

use crate::core::{Decimal, RunMode, Side, StrategyId, Timestamp};

/// 跨运行模式的不可变交易账本事实。
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LedgerEvent {
    /// 来源经济事实的稳定唯一标识，用于回放去重。
    pub event_id: String,
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
        order_id: Option<String>,
        client_order_id: Option<String>,
        symbol: String,
        side: Side,
        size: Decimal,
        price: Decimal,
        /// None 表示来源未提供手续费，不能按零处理。
        fee: Option<Decimal>,
        /// None 表示来源或订单关联未提供 reduce-only 属性。
        reduce_only: Option<bool>,
    },
    Funding {
        symbol: String,
        funding_rate: Option<Decimal>,
        settlement_price: Option<Decimal>,
        cashflow: Decimal,
    },
    Liquidation {
        symbol: Option<String>,
        size: Option<Decimal>,
        price: Option<Decimal>,
        realized_pnl: Option<Decimal>,
        fee: Option<Decimal>,
        reason: Option<String>,
    },
    EquitySnapshot {
        equity: Decimal,
        margin_used: Decimal,
        realized_pnl: Option<Decimal>,
        unrealized_pnl: Option<Decimal>,
        trading_fees: Option<Decimal>,
        funding_pnl: Option<Decimal>,
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
                strategy_id: StrategyId::new("strategy-1"),
                mode: RunMode::Backtest,
                exchange: "hyperliquid".to_string(),
                timestamp,
                event: LedgerEventKind::Fill {
                    order_id: Some("order-1".to_string()),
                    client_order_id: Some("client-1".to_string()),
                    symbol: "BTC".to_string(),
                    side: Side::Buy,
                    size: Decimal::new(2, 0),
                    price: Decimal::new(100, 0),
                    fee: Some(Decimal::new(4, 1)),
                    reduce_only: Some(false),
                },
            },
            LedgerEvent {
                event_id: "funding-1".to_string(),
                strategy_id: StrategyId::new("strategy-1"),
                mode: RunMode::Backtest,
                exchange: "hyperliquid".to_string(),
                timestamp,
                event: LedgerEventKind::Funding {
                    symbol: "BTC".to_string(),
                    funding_rate: Some(Decimal::new(1, 3)),
                    settlement_price: Some(Decimal::new(100, 0)),
                    cashflow: Decimal::new(-2, 1),
                },
            },
            LedgerEvent {
                event_id: "liquidation-1".to_string(),
                strategy_id: StrategyId::new("strategy-1"),
                mode: RunMode::Backtest,
                exchange: "hyperliquid".to_string(),
                timestamp,
                event: LedgerEventKind::Liquidation {
                    symbol: Some("BTC".to_string()),
                    size: Some(Decimal::ONE),
                    price: Some(Decimal::new(50, 0)),
                    realized_pnl: Some(Decimal::new(-50, 0)),
                    fee: None,
                    reason: Some("maintenance_margin_breach".to_string()),
                },
            },
            LedgerEvent {
                event_id: "snapshot-1".to_string(),
                strategy_id: StrategyId::new("strategy-1"),
                mode: RunMode::Backtest,
                exchange: "hyperliquid".to_string(),
                timestamp,
                event: LedgerEventKind::EquitySnapshot {
                    equity: Decimal::new(1_000, 0),
                    margin_used: Decimal::new(100, 0),
                    realized_pnl: Some(Decimal::new(10, 0)),
                    unrealized_pnl: Some(Decimal::new(-5, 0)),
                    trading_fees: Some(Decimal::new(1, 0)),
                    funding_pnl: Some(Decimal::new(-2, 0)),
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
