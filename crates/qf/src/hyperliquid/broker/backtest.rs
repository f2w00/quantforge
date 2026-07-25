use anyhow::bail;
use serde_json::json;

use crate::core::StrategyId;
use crate::hyperliquid::broker::risk_adapter::order_risk_input;
use crate::hyperliquid::broker::state::HlBrokerState;
use crate::hyperliquid::broker::traits::HyperliquidBroker;
use crate::hyperliquid::types::{
    HlAccountState, HlCancelRequest, HlCancelResponse, HlCancelStatus, HlCloseOptions, HlCoin,
    HlOpenOrder, HlOrderRequest, HlOrderResponse, HlOrderStatus, HlPosition,
};
use crate::risk::{RiskDecision, RiskGuard};

pub struct HyperliquidBacktestBroker {
    strategy_id: StrategyId,
    state: HlBrokerState,
    risk_guard: RiskGuard,
    next_order_id: u64,
}

impl HyperliquidBacktestBroker {
    pub fn new(strategy_id: StrategyId, state: HlBrokerState, risk_guard: RiskGuard) -> Self {
        Self {
            strategy_id,
            state,
            risk_guard,
            next_order_id: 1,
        }
    }
}

impl HyperliquidBroker for HyperliquidBacktestBroker {
    fn account_state(&self) -> HlAccountState {
        self.state.account.clone()
    }

    fn position(&self, coin: &HlCoin) -> Option<HlPosition> {
        self.state.position(coin)
    }

    fn open_orders(&self, coin: &HlCoin) -> Vec<HlOpenOrder> {
        self.state.open_orders(coin)
    }

    fn place_order(&mut self, request: HlOrderRequest) -> anyhow::Result<HlOrderResponse> {
        let input = order_risk_input(
            self.strategy_id.clone(),
            &self.state.account,
            &request,
            self.state.open_orders.len(),
        );

        if let RiskDecision::Rejected { violations } = self.risk_guard.check(&input) {
            bail!("risk rejected order: {violations:?}");
        }

        let order_id = crate::core::OrderId::new(format!("bt-{}", self.next_order_id));
        self.next_order_id += 1;

        Ok(HlOrderResponse {
            order_id: Some(order_id),
            statuses: vec![HlOrderStatus::Accepted],
            raw: json!({ "mode": "backtest", "status": "accepted" }),
        })
    }

    fn cancel_order(&mut self, _request: HlCancelRequest) -> anyhow::Result<HlCancelResponse> {
        Ok(HlCancelResponse {
            success: true,
            statuses: vec![HlCancelStatus::Success],
            raw: json!({ "mode": "backtest", "status": "cancelled" }),
        })
    }

    fn close_position(
        &mut self,
        coin: &HlCoin,
        _options: HlCloseOptions,
    ) -> anyhow::Result<HlOrderResponse> {
        Ok(HlOrderResponse {
            order_id: Some(crate::core::OrderId::new(format!("bt-close-{}", coin.0))),
            statuses: vec![HlOrderStatus::Accepted],
            raw: json!({ "mode": "backtest", "status": "close_requested" }),
        })
    }
}
