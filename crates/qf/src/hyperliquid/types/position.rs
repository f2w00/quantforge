use serde::{Deserialize, Serialize};

use crate::core::Decimal;
use crate::hyperliquid::types::HlCoin;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlPosition {
    pub coin: HlCoin,
    pub size: Decimal,
    pub entry_price: Option<Decimal>,
    pub notional: Decimal,
    pub leverage: Decimal,
    pub liquidation_price: Option<Decimal>,
}
