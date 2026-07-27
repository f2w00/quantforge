use crate::hyperliquid::types::{
    HlAccountState, HlCancelRequest, HlCancelResponse, HlCloseRequest, HlCoin, HlOpenOrder,
    HlOrderRequest, HlOrderResult, HlPosition,
};

use super::HlBrokerError;

/// Hyperliquid 专属 broker 接口。
///
/// Live、paper、backtest broker 都应该实现这个 trait；策略依赖这个接口，
/// 不直接接触 REST/WS client、signer 或 API key。
#[async_trait::async_trait]
pub trait HyperliquidBroker: Send + Sync {
    /// 返回 broker 当前维护的本地账户快照，不主动发起远端同步。
    fn account_state(&self) -> HlAccountState;

    /// 返回账户级本地 open order 快照。
    fn open_orders(&self) -> Vec<HlOpenOrder>;

    async fn place_order(&self, request: HlOrderRequest) -> Result<HlOrderResult, HlBrokerError>;

    async fn cancel_order(
        &self,
        request: HlCancelRequest,
    ) -> Result<HlCancelResponse, HlBrokerError>;

    /// 根据本地仓位提交 reduce-only 市价平仓单，不保证返回时仓位已经归零。
    async fn close_position(&self, request: HlCloseRequest)
    -> Result<HlOrderResult, HlBrokerError>;

    /// 返回指定 coin 的本地仓位快照。
    fn position(&self, coin: &HlCoin) -> Option<HlPosition> {
        self.account_state().positions.get(coin).cloned()
    }

    /// 返回指定 coin 的本地 open order 快照。
    fn open_orders_for(&self, coin: &HlCoin) -> Vec<HlOpenOrder> {
        self.open_orders()
            .into_iter()
            .filter(|order| &order.coin == coin)
            .collect()
    }
}
