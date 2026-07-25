use serde::{Deserialize, Serialize};

use crate::core::Decimal;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct RiskLimits {
    pub max_leverage: Option<Decimal>,
    pub max_order_notional: Option<Decimal>,
    pub max_post_trade_notional: Option<Decimal>,
    pub max_open_orders: Option<usize>,
    pub reduce_only: bool,
}
