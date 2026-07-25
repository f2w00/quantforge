use serde::{Deserialize, Serialize};

use crate::core::{Decimal, Side, StrategyId, Symbol};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RiskCheckInput {
    pub strategy_id: StrategyId,
    pub exchange: String,
    pub symbol: Symbol,
    pub side: Side,
    pub reduce_only: bool,
    pub order_notional: Decimal,
    pub post_trade_notional: Decimal,
    pub post_trade_leverage: Decimal,
    pub account_equity: Decimal,
    pub open_order_count: usize,
}
