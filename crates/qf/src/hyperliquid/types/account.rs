use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::core::{Decimal, Timestamp};
use crate::hyperliquid::types::{HlCoin, HlPosition};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct HlAccountState {
    pub equity: Decimal,
    pub margin_used: Decimal,
    pub positions: HashMap<HlCoin, HlPosition>,
    /// 交易所生成该账户快照的时间。
    pub updated_at: Option<Timestamp>,
}
