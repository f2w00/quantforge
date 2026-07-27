use crate::hyperliquid::types::{HlAccountState, HlCoin, HlOpenOrder, HlPosition};

#[derive(Clone, Debug)]
pub struct HlBrokerState {
    pub account: HlAccountState,
    pub open_orders: Vec<HlOpenOrder>,
}

impl HlBrokerState {
    pub fn position(&self, coin: &HlCoin) -> Option<HlPosition> {
        self.account.positions.get(coin).cloned()
    }

    pub fn open_orders(&self, coin: &HlCoin) -> Vec<HlOpenOrder> {
        self.open_orders
            .iter()
            .filter(|order| &order.coin == coin)
            .cloned()
            .collect()
    }
}
