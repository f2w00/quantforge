use serde::{Deserialize, Serialize};

use crate::core::Decimal;
use crate::hyperliquid::types::HlPosition;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct HlAccountState {
    pub equity: Decimal,
    pub margin_used: Decimal,
    pub positions: Vec<HlPosition>,
}
