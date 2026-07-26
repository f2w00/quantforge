use crate::core::{Decimal, StrategyId, Symbol};
use crate::hyperliquid::types::{HlAccountState, HlOrderRequest};
use crate::risk::RiskCheckInput;

pub fn order_risk_input_at_price(
    strategy_id: StrategyId,
    account: &HlAccountState,
    request: &HlOrderRequest,
    price: Decimal,
    open_order_count: usize,
) -> RiskCheckInput {
    let order_notional = price * request.size.abs();

    RiskCheckInput {
        strategy_id,
        exchange: "hyperliquid".to_string(),
        symbol: Symbol::new(request.coin.0.clone()),
        side: request.side,
        reduce_only: request.reduce_only,
        order_notional,
        post_trade_notional: order_notional,
        post_trade_leverage: Decimal::ZERO,
        account_equity: account.equity,
        open_order_count,
    }
}
