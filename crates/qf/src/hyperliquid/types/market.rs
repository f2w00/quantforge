use serde::{Deserialize, Serialize};

use crate::core::{Decimal, Timestamp};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct HlCoin(pub String);

impl HlCoin {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlMarkPrice {
    pub coin: HlCoin,
    pub price: Decimal,
    pub timestamp: Timestamp,
}
