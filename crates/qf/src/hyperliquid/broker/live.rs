use anyhow::bail;
use serde_json::json;

use crate::core::StrategyId;
use crate::hyperliquid::broker::risk_adapter::order_risk_input;
use crate::hyperliquid::broker::state::HlBrokerState;
use crate::hyperliquid::broker::traits::HyperliquidBroker;
use crate::hyperliquid::client::HyperliquidRestClient;
use crate::hyperliquid::types::{
    HlAccountState, HlCancelRequest, HlCancelResponse, HlCancelStatus, HlCloseOptions, HlCoin,
    HlOpenOrder, HlOrderRequest, HlOrderResponse, HlOrderStatus, HlPosition,
};
use crate::risk::{RiskDecision, RiskGuard};

pub struct HyperliquidLiveBroker {
    strategy_id: StrategyId,
    state: HlBrokerState,
    risk_guard: RiskGuard,
    client: HyperliquidRestClient,
}

impl HyperliquidLiveBroker {
    pub fn new(
        strategy_id: StrategyId,
        state: HlBrokerState,
        risk_guard: RiskGuard,
        client: HyperliquidRestClient,
    ) -> Self {
        Self {
            strategy_id,
            state,
            risk_guard,
            client,
        }
    }
}

impl HyperliquidBroker for HyperliquidLiveBroker {
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

        Ok(HlOrderResponse {
            order_id: None,
            statuses: vec![HlOrderStatus::Accepted],
            raw: json!({
                "mode": "live",
                "status": "not_implemented",
                "base_url": self.client.base_url,
            }),
        })
    }

    fn cancel_order(&mut self, _request: HlCancelRequest) -> anyhow::Result<HlCancelResponse> {
        Ok(HlCancelResponse {
            success: false,
            statuses: vec![HlCancelStatus::Error {
                message: "not_implemented".to_string(),
            }],
            raw: json!({ "mode": "live", "status": "not_implemented" }),
        })
    }

    fn close_position(
        &mut self,
        coin: &HlCoin,
        _options: HlCloseOptions,
    ) -> anyhow::Result<HlOrderResponse> {
        Ok(HlOrderResponse {
            order_id: None,
            statuses: vec![HlOrderStatus::Accepted],
            raw: json!({
                "mode": "live",
                "status": "not_implemented",
                "coin": coin.0,
            }),
        })
    }
}
