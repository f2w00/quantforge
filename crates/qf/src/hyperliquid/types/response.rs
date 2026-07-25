use serde::{Deserialize, Serialize};

use crate::core::{Decimal, OrderId};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlResponse {
    pub raw: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlOrderResponse {
    pub order_id: Option<OrderId>,
    pub statuses: Vec<HlOrderStatus>,
    pub raw: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlCancelResponse {
    pub success: bool,
    pub statuses: Vec<HlCancelStatus>,
    pub raw: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum HlOrderStatus {
    Accepted,
    Resting {
        order_id: OrderId,
    },
    Filled {
        order_id: OrderId,
        total_size: Decimal,
        avg_price: Decimal,
    },
    Error {
        message: String,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum HlCancelStatus {
    Success,
    Error { message: String },
}
