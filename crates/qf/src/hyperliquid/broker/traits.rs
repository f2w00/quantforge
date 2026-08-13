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
    /// 返回账户快照；具体实现可按需从远端查询。
    async fn account_state(&self) -> Result<HlAccountState, HlBrokerError>;

    /// 返回账户级 open order 快照；具体实现可按需从远端查询。
    async fn open_orders(&self) -> Result<Vec<HlOpenOrder>, HlBrokerError>;

    async fn place_order(&self, request: HlOrderRequest) -> Result<HlOrderResult, HlBrokerError>;

    async fn cancel_order(
        &self,
        request: HlCancelRequest,
    ) -> Result<HlCancelResponse, HlBrokerError>;

    /// 根据当前仓位提交 reduce-only 市价平仓单，不保证返回时仓位已经归零。
    async fn close_position(&self, request: HlCloseRequest)
    -> Result<HlOrderResult, HlBrokerError>;

    /// 返回指定 coin 的仓位快照。
    async fn position(&self, coin: &HlCoin) -> Result<Option<HlPosition>, HlBrokerError> {
        Ok(self.account_state().await?.positions.get(coin).cloned())
    }

    /// 返回指定 coin 的 open order 快照。
    async fn open_orders_for(&self, coin: &HlCoin) -> Result<Vec<HlOpenOrder>, HlBrokerError> {
        Ok(self
            .open_orders()
            .await?
            .into_iter()
            .filter(|order| &order.coin == coin)
            .collect())
    }
}
