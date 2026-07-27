use crate::core::{Decimal, Side, StrategyId, Symbol};
use crate::hyperliquid::types::{HlAccountState, HlOpenOrder, HlOrderRequest};
use crate::risk::RiskCheckInput;

pub fn order_risk_input_at_price(
    strategy_id: StrategyId,
    account: &HlAccountState,
    request: &HlOrderRequest,
    size: Decimal,
    price: Decimal,
    open_orders: &[HlOpenOrder],
    pending_notional: Decimal,
) -> RiskCheckInput {
    let order_notional = price * size.abs();
    let existing_position_notional: Decimal = account
        .positions
        .iter()
        .map(|position| position.notional.abs())
        .sum();
    let existing_open_order_notional: Decimal = open_orders
        .iter()
        .filter(|order| !order.reduce_only)
        .filter_map(|order| order.limit_price.map(|price| price * order.size.abs()))
        .sum();
    let projected_position_notional = projected_position_notional(account, request, order_notional);
    let post_trade_notional = existing_position_notional
        - account
            .positions
            .iter()
            .find(|position| position.coin == request.coin)
            .map(|position| position.notional.abs())
            .unwrap_or(Decimal::ZERO)
        + projected_position_notional
        + existing_open_order_notional;
    let post_trade_notional = post_trade_notional + pending_notional;
    let post_trade_leverage = if account.equity > Decimal::ZERO {
        post_trade_notional / account.equity
    } else {
        Decimal::MAX
    };

    RiskCheckInput {
        strategy_id,
        exchange: "hyperliquid".to_string(),
        symbol: Symbol::new(request.coin.0.clone()),
        side: request.side,
        reduce_only: request.reduce_only,
        order_notional,
        post_trade_notional,
        post_trade_leverage,
        account_equity: account.equity,
        open_order_count: open_orders.len(),
    }
}

fn projected_position_notional(
    account: &HlAccountState,
    request: &HlOrderRequest,
    order_notional: Decimal,
) -> Decimal {
    let Some(position) = account
        .positions
        .iter()
        .find(|position| position.coin == request.coin)
    else {
        return if request.reduce_only {
            Decimal::ZERO
        } else {
            order_notional
        };
    };
    let position_notional = position.notional.abs();
    let order_is_buy = request.side == Side::Buy;
    let position_is_buy = position.size.is_sign_positive();
    if request.reduce_only || order_is_buy != position_is_buy {
        (position_notional - order_notional).max(Decimal::ZERO)
    } else {
        position_notional + order_notional
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::core::Side;
    use crate::hyperliquid::types::{HlCoin, HlOrderType, HlPosition};

    fn decimal(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    #[test]
    fn includes_existing_position_in_post_trade_leverage() {
        let account = HlAccountState {
            equity: decimal("100"),
            margin_used: Decimal::ZERO,
            positions: vec![HlPosition {
                coin: HlCoin::new("BTC"),
                size: decimal("1"),
                entry_price: Some(decimal("100")),
                notional: decimal("100"),
                leverage: decimal("1"),
                liquidation_price: None,
            }],
        };
        let request = HlOrderRequest {
            coin: HlCoin::new("BTC"),
            side: Side::Buy,
            size: crate::hyperliquid::types::HlOrderSize::Exact(decimal("1")),
            reduce_only: false,
            order_type: HlOrderType::Market {
                max_slippage_bps: Some(100),
            },
            client_order_id: None,
            expires_after: None,
        };

        let input = order_risk_input_at_price(
            StrategyId::new("test"),
            &account,
            &request,
            decimal("1"),
            decimal("100"),
            &[],
            Decimal::ZERO,
        );

        assert_eq!(input.post_trade_notional, decimal("200"));
        assert_eq!(input.post_trade_leverage, decimal("2"));
    }
}
