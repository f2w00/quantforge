use crate::hyperliquid::types::{
    HlAccountState, HlCancelRequest, HlCancelResponse, HlCloseOptions, HlCoin, HlOpenOrder,
    HlOrderRequest, HlOrderResponse, HlPosition,
};

/// Hyperliquid 专属 broker 接口。
///
/// Live、paper、backtest broker 都应该实现这个 trait；策略依赖这个接口，
/// 不直接接触 REST/WS client、signer 或 API key。
pub trait HyperliquidBroker {
    /// 返回 broker 当前维护的本地账户快照，不主动发起远端同步。
    fn account_state(&self) -> HlAccountState;

    /// 返回指定 coin 的本地仓位快照。
    fn position(&self, coin: &HlCoin) -> Option<HlPosition>;

    /// 返回指定 coin 的本地 open order 快照。
    fn open_orders(&self, coin: &HlCoin) -> Vec<HlOpenOrder>;

    fn place_order(&mut self, request: HlOrderRequest) -> anyhow::Result<HlOrderResponse>;

    fn cancel_order(&mut self, request: HlCancelRequest) -> anyhow::Result<HlCancelResponse>;

    fn close_position(
        &mut self,
        coin: &HlCoin,
        options: HlCloseOptions,
    ) -> anyhow::Result<HlOrderResponse>;
}
