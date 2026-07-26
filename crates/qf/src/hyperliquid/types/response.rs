use serde::{Deserialize, Serialize};

use crate::core::{Decimal, OrderId, Side};
use crate::hyperliquid::types::{HlClientOrderId, HlCoin};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlResponse {
    pub raw: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlOrderResult {
    pub submitted: HlSubmittedOrder,
    pub outcome: HlOrderOutcome,
    pub raw: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlCancelResponse {
    pub success: bool,
    pub statuses: Vec<HlCancelStatus>,
    pub raw: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlSubmittedOrder {
    pub coin: HlCoin,
    pub side: Side,
    pub size: Decimal,
    pub limit_price: Decimal,
    pub reduce_only: bool,
    pub client_order_id: HlClientOrderId,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum HlOrderOutcome {
    Resting {
        order_id: OrderId,
    },
    Filled {
        order_id: OrderId,
        total_size: Decimal,
        avg_price: Decimal,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum HlCancelStatus {
    Success,
    Error { message: String },
}
