use std::collections::HashMap;
use std::sync::Mutex;

use serde_json::json;

use crate::core::{Decimal, OrderId, Side, StrategyId};
use crate::hyperliquid::broker::HlBrokerError;
use crate::hyperliquid::broker::risk_adapter::order_risk_input_at_price;
use crate::hyperliquid::broker::state::HlBrokerState;
use crate::hyperliquid::broker::traits::HyperliquidBroker;
use crate::hyperliquid::types::{
    HlAccountState, HlCancelRequest, HlCancelResponse, HlCancelStatus, HlClientOrderId,
    HlCloseRequest, HlCloseSize, HlCoin, HlOpenOrder, HlOrderOutcome, HlOrderRequest,
    HlOrderResult, HlOrderSize, HlOrderType, HlPosition, HlSubmittedOrder,
};
use crate::risk::{RiskDecision, RiskGuard};

pub struct HyperliquidBacktestBroker {
    strategy_id: StrategyId,
    risk_guard: RiskGuard,
    inner: Mutex<HyperliquidBacktestInner>,
}

struct HyperliquidBacktestInner {
    state: HlBrokerState,
    mark_prices: HashMap<HlCoin, Decimal>,
    leverages: HashMap<HlCoin, u32>,
    initial_equity: Decimal,
    realized_pnl: Decimal,
    next_order_id: u64,
}

impl HyperliquidBacktestBroker {
    pub fn new(strategy_id: StrategyId, state: HlBrokerState, risk_guard: RiskGuard) -> Self {
        let initial_equity = state.account.equity;
        Self {
            strategy_id,
            risk_guard,
            inner: Mutex::new(HyperliquidBacktestInner {
                state,
                mark_prices: HashMap::new(),
                leverages: HashMap::new(),
                initial_equity,
                realized_pnl: Decimal::ZERO,
                next_order_id: 1,
            }),
        }
    }

    pub fn set_mark_price(&self, coin: HlCoin, price: Decimal) -> Result<(), HlBrokerError> {
        if price <= Decimal::ZERO {
            return Err(invalid_request("mark price must be positive"));
        }
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| HlBrokerError::StateUnavailable)?;
        inner.mark_prices.insert(coin, price);
        inner.revalue_account();
        Ok(())
    }

    pub fn set_leverage(&self, coin: HlCoin, leverage: u32) -> Result<(), HlBrokerError> {
        if leverage == 0 {
            return Err(invalid_request("leverage must be positive"));
        }
        self.inner
            .lock()
            .map_err(|_| HlBrokerError::StateUnavailable)?
            .leverages
            .insert(coin, leverage);
        Ok(())
    }

    pub fn realized_pnl(&self) -> Decimal {
        self.inner
            .lock()
            .map(|inner| inner.realized_pnl)
            .unwrap_or(Decimal::ZERO)
    }

    pub fn unrealized_pnl(&self) -> Decimal {
        self.inner
            .lock()
            .map(|inner| inner.unrealized_pnl())
            .unwrap_or(Decimal::ZERO)
    }
}

impl HyperliquidBacktestInner {
    fn unrealized_pnl(&self) -> Decimal {
        self.state
            .account
            .positions
            .values()
            .filter_map(|position| {
                let mark_price = self.mark_prices.get(&position.coin)?;
                let entry_price = position.entry_price?;
                Some((*mark_price - entry_price) * position.size)
            })
            .sum()
    }

    fn next_order_id(&mut self) -> OrderId {
        let order_id = OrderId::new(format!("bt-{}", self.next_order_id));
        self.next_order_id += 1;
        order_id
    }

    fn revalue_account(&mut self) {
        for position in self.state.account.positions.values_mut() {
            if let Some(mark_price) = self.mark_prices.get(&position.coin) {
                position.notional = position.size.abs() * *mark_price;
            }
        }

        self.state.account.equity = self.initial_equity + self.realized_pnl + self.unrealized_pnl();
        self.state.account.margin_used = self
            .state
            .account
            .positions
            .values()
            .filter(|position| position.leverage > Decimal::ZERO)
            .map(|position| position.notional / position.leverage)
            .sum();
    }

    fn fill_market_order(
        &mut self,
        request: &HlOrderRequest,
        mark_price: Decimal,
    ) -> Result<Decimal, HlBrokerError> {
        let HlOrderSize::Exact(size) = request.size else {
            return Err(invalid_request("backtest order size must be resolved"));
        };
        if size <= Decimal::ZERO {
            return Err(invalid_request("order size must be positive"));
        }

        let requested_size = match request.side {
            Side::Buy => size,
            Side::Sell => -size,
        };
        let current_position = self.state.account.positions.get(&request.coin);
        let current_size = current_position
            .map(|position| position.size)
            .unwrap_or(Decimal::ZERO);

        let fill_size = if request.reduce_only {
            if current_size == Decimal::ZERO {
                return Err(invalid_request("reduce-only order has no position"));
            }
            if current_size.is_sign_positive() == requested_size.is_sign_positive() {
                return Err(invalid_request(
                    "reduce-only order would increase the position",
                ));
            }
            let direction = if requested_size.is_sign_positive() {
                Decimal::ONE
            } else {
                -Decimal::ONE
            };
            direction * size.min(current_size.abs())
        } else {
            requested_size
        };

        let current_entry_price = current_position
            .and_then(|position| position.entry_price)
            .unwrap_or(mark_price);
        let new_size = current_size + fill_size;

        if current_size != Decimal::ZERO
            && current_size.is_sign_positive() != fill_size.is_sign_positive()
        {
            let closed_size = current_size.abs().min(fill_size.abs());
            let position_direction = if current_size.is_sign_positive() {
                Decimal::ONE
            } else {
                -Decimal::ONE
            };
            self.realized_pnl +=
                (mark_price - current_entry_price) * position_direction * closed_size;
        }

        if new_size == Decimal::ZERO {
            self.state.account.positions.remove(&request.coin);
        } else {
            let entry_price = if current_size == Decimal::ZERO
                || current_size.is_sign_positive() != new_size.is_sign_positive()
            {
                mark_price
            } else if current_size.is_sign_positive() == fill_size.is_sign_positive() {
                ((current_entry_price * current_size.abs()) + (mark_price * fill_size.abs()))
                    / new_size.abs()
            } else {
                current_entry_price
            };

            let position = HlPosition {
                coin: request.coin.clone(),
                size: new_size,
                entry_price: Some(entry_price),
                notional: new_size.abs() * mark_price,
                unrealized_pnl: Decimal::ZERO,
                return_on_equity: Decimal::ZERO,
                leverage: current_position
                    .map(|position| position.leverage)
                    .filter(|leverage| *leverage > Decimal::ZERO)
                    .unwrap_or(Decimal::ONE),
                liquidation_price: current_position.and_then(|position| position.liquidation_price),
            };

            self.state
                .account
                .positions
                .insert(request.coin.clone(), position);
        }

        self.revalue_account();
        Ok(fill_size.abs())
    }
}

fn resolve_backtest_size(
    account: &HlAccountState,
    price: Decimal,
    leverage: u32,
    margin_fraction: Decimal,
    reserve_fraction: Decimal,
) -> Result<Decimal, HlBrokerError> {
    if margin_fraction <= Decimal::ZERO || margin_fraction > Decimal::ONE {
        return Err(invalid_request("margin fraction must be in (0, 1]"));
    }
    if reserve_fraction < Decimal::ZERO || reserve_fraction >= Decimal::ONE {
        return Err(invalid_request("reserve fraction must be in [0, 1)"));
    }
    let available_margin =
        (account.equity - account.margin_used - (account.equity * reserve_fraction))
            .max(Decimal::ZERO);
    let size = (available_margin * margin_fraction * Decimal::from(leverage) / price)
        .round_dp_with_strategy(8, rust_decimal::RoundingStrategy::ToZero);
    if size <= Decimal::ZERO {
        return Err(invalid_request(
            "sizing result is below the minimum quantity increment",
        ));
    }
    Ok(size)
}

fn invalid_request(message: impl Into<String>) -> HlBrokerError {
    HlBrokerError::InvalidRequest {
        message: message.into(),
    }
}

#[async_trait::async_trait]
impl HyperliquidBroker for HyperliquidBacktestBroker {
    fn account_state(&self) -> Result<HlAccountState, HlBrokerError> {
        self.inner
            .lock()
            .map(|inner| inner.state.account.clone())
            .map_err(|_| HlBrokerError::StateUnavailable)
    }

    fn open_orders(&self) -> Vec<HlOpenOrder> {
        self.inner
            .lock()
            .map(|inner| inner.state.open_orders.clone())
            .unwrap_or_default()
    }

    async fn place_order(&self, request: HlOrderRequest) -> Result<HlOrderResult, HlBrokerError> {
        request.validate().map_err(invalid_request)?;
        if !matches!(request.order_type, HlOrderType::Market { .. }) {
            return Err(invalid_request(
                "simple backtest broker only supports market orders",
            ));
        }
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| HlBrokerError::StateUnavailable)?;
        let mark_price = inner
            .mark_prices
            .get(&request.coin)
            .copied()
            .ok_or(HlBrokerError::StateUnavailable)?;
        let size = match request.size {
            HlOrderSize::Exact(size) if size > Decimal::ZERO => size,
            HlOrderSize::Exact(_) => return Err(invalid_request("order size must be positive")),
            HlOrderSize::MarginFraction {
                margin_fraction,
                reserve_fraction,
            } => resolve_backtest_size(
                &inner.state.account,
                mark_price,
                *inner.leverages.get(&request.coin).ok_or_else(|| {
                    invalid_request("margin-fraction sizing requires configured leverage")
                })?,
                margin_fraction,
                reserve_fraction,
            )?,
        };
        let mut request = request;
        request.size = HlOrderSize::Exact(size);
        let input = order_risk_input_at_price(
            self.strategy_id.clone(),
            &inner.state.account,
            &request,
            size,
            mark_price,
            &inner.state.open_orders,
            Decimal::ZERO,
        );

        if let RiskDecision::Rejected { violations } = self.risk_guard.check(&input) {
            return Err(HlBrokerError::RiskRejected { violations });
        }

        let filled_size = inner.fill_market_order(&request, mark_price)?;
        let order_id = inner.next_order_id();
        let client_order_id = match request.client_order_id {
            Some(client_order_id) => client_order_id,
            None => HlClientOrderId::new(format!("0x{:032x}", inner.next_order_id - 1))
                .map_err(invalid_request)?,
        };

        Ok(HlOrderResult {
            submitted: HlSubmittedOrder {
                coin: request.coin,
                side: request.side,
                size,
                limit_price: mark_price,
                reduce_only: request.reduce_only,
                client_order_id,
            },
            outcome: HlOrderOutcome::Filled {
                order_id,
                total_size: filled_size,
                avg_price: mark_price,
            },
            raw: json!({
                "mode": "backtest",
                "status": "filled",
                "price": mark_price.to_string(),
                "size": filled_size.to_string(),
            }),
        })
    }

    async fn cancel_order(
        &self,
        _request: HlCancelRequest,
    ) -> Result<HlCancelResponse, HlBrokerError> {
        Ok(HlCancelResponse {
            success: false,
            statuses: vec![HlCancelStatus::Error {
                message: "simple backtest broker does not support open orders".to_string(),
            }],
            raw: json!({ "mode": "backtest", "status": "not_supported" }),
        })
    }

    async fn close_position(
        &self,
        request: HlCloseRequest,
    ) -> Result<HlOrderResult, HlBrokerError> {
        let position = self
            .position(&request.coin)
            .filter(|position| position.size != Decimal::ZERO)
            .ok_or_else(|| HlBrokerError::PositionUnavailable {
                coin: request.coin.clone(),
            })?;
        let size = match request.size {
            HlCloseSize::Full => position.size.abs(),
            HlCloseSize::Exact(size) if size > Decimal::ZERO => size,
            HlCloseSize::Exact(_) => {
                return Err(invalid_request("close size must be positive"));
            }
            HlCloseSize::Fraction(fraction) => {
                super::live::calculate_close_size(position.size, fraction, 8)?
            }
        };

        self.place_order(HlOrderRequest {
            coin: request.coin,
            side: if position.size.is_sign_positive() {
                Side::Sell
            } else {
                Side::Buy
            },
            size: HlOrderSize::Exact(size),
            reduce_only: true,
            order_type: HlOrderType::Market {
                max_slippage_bps: request.max_slippage_bps,
            },
            client_order_id: request.client_order_id,
            expires_after: request.expires_after,
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::risk::RiskLimits;

    fn decimal(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    fn broker() -> HyperliquidBacktestBroker {
        HyperliquidBacktestBroker::new(
            StrategyId::new("test"),
            HlBrokerState {
                account: HlAccountState {
                    equity: decimal("1000"),
                    margin_used: Decimal::ZERO,
                    positions: HashMap::new(),
                    updated_at: chrono::Utc::now(),
                },
                open_orders: Vec::new(),
            },
            RiskGuard::new(RiskLimits::default()),
        )
    }

    fn market_order(coin: &HlCoin, side: Side, size: &str) -> HlOrderRequest {
        HlOrderRequest {
            coin: coin.clone(),
            side,
            size: HlOrderSize::Exact(decimal(size)),
            reduce_only: false,
            order_type: HlOrderType::Market {
                max_slippage_bps: Some(100),
            },
            client_order_id: None,
            expires_after: None,
        }
    }

    #[tokio::test]
    async fn fills_market_order_and_updates_position() {
        let coin = HlCoin::new("BTC");
        let broker = broker();
        broker.set_mark_price(coin.clone(), decimal("100")).unwrap();

        let response = broker
            .place_order(market_order(&coin, Side::Buy, "2"))
            .await
            .unwrap();

        assert!(matches!(
            &response.outcome,
            HlOrderOutcome::Filled {
                total_size,
                avg_price,
                ..
            } if *total_size == decimal("2") && *avg_price == decimal("100")
        ));
        let position = broker.position(&coin).unwrap();
        assert_eq!(position.size, decimal("2"));
        assert_eq!(position.entry_price, Some(decimal("100")));
        assert_eq!(position.notional, decimal("200"));
    }

    #[tokio::test]
    async fn calculates_weighted_entry_and_realized_pnl() {
        let coin = HlCoin::new("BTC");
        let broker = broker();
        broker.set_mark_price(coin.clone(), decimal("100")).unwrap();
        broker
            .place_order(market_order(&coin, Side::Buy, "1"))
            .await
            .unwrap();
        broker.set_mark_price(coin.clone(), decimal("120")).unwrap();
        broker
            .place_order(market_order(&coin, Side::Buy, "1"))
            .await
            .unwrap();

        assert_eq!(
            broker.position(&coin).unwrap().entry_price,
            Some(decimal("110"))
        );

        broker.set_mark_price(coin.clone(), decimal("130")).unwrap();
        broker
            .place_order(market_order(&coin, Side::Sell, "1"))
            .await
            .unwrap();

        assert_eq!(broker.realized_pnl(), decimal("20"));
        assert_eq!(broker.unrealized_pnl(), decimal("20"));
        assert_eq!(broker.account_state().unwrap().equity, decimal("1040"));
    }

    #[tokio::test]
    async fn close_position_realizes_pnl() {
        let coin = HlCoin::new("BTC");
        let broker = broker();
        broker.set_mark_price(coin.clone(), decimal("100")).unwrap();
        broker
            .place_order(market_order(&coin, Side::Buy, "2"))
            .await
            .unwrap();
        broker.set_mark_price(coin.clone(), decimal("125")).unwrap();

        broker
            .close_position(HlCloseRequest {
                coin: coin.clone(),
                size: HlCloseSize::Full,
                max_slippage_bps: Some(100),
                client_order_id: None,
                expires_after: None,
            })
            .await
            .unwrap();

        assert!(broker.position(&coin).is_none());
        assert_eq!(broker.realized_pnl(), decimal("50"));
        assert_eq!(broker.account_state().unwrap().equity, decimal("1050"));
    }

    #[tokio::test]
    async fn closes_exact_position_size() {
        let coin = HlCoin::new("BTC");
        let broker = broker();
        broker.set_mark_price(coin.clone(), decimal("100")).unwrap();
        broker
            .place_order(market_order(&coin, Side::Buy, "2"))
            .await
            .unwrap();

        let result = broker
            .close_position(HlCloseRequest {
                coin: coin.clone(),
                size: HlCloseSize::Exact(decimal("0.5")),
                max_slippage_bps: Some(100),
                client_order_id: None,
                expires_after: None,
            })
            .await
            .unwrap();

        assert_eq!(broker.position(&coin).unwrap().size, decimal("1.5"));
        assert_eq!(result.submitted.size, decimal("0.5"));
        assert!(result.submitted.client_order_id.as_str().starts_with("0x"));
    }

    #[tokio::test]
    async fn closes_fractional_position_size() {
        let coin = HlCoin::new("BTC");
        let broker = broker();
        broker.set_mark_price(coin.clone(), decimal("100")).unwrap();
        broker
            .place_order(market_order(&coin, Side::Buy, "2"))
            .await
            .unwrap();

        let result = broker
            .close_position(HlCloseRequest {
                coin: coin.clone(),
                size: HlCloseSize::Fraction(decimal("0.25")),
                max_slippage_bps: Some(100),
                client_order_id: None,
                expires_after: None,
            })
            .await
            .unwrap();

        assert_eq!(result.submitted.size, decimal("0.5"));
        assert_eq!(broker.position(&coin).unwrap().size, decimal("1.5"));
    }

    #[tokio::test]
    async fn resolves_margin_fraction_inside_place_order() {
        let coin = HlCoin::new("BTC");
        let broker = broker();
        broker.set_mark_price(coin.clone(), decimal("100")).unwrap();
        broker.set_leverage(coin.clone(), 5).unwrap();

        let result = broker
            .place_order(HlOrderRequest {
                coin: coin.clone(),
                side: Side::Buy,
                size: HlOrderSize::MarginFraction {
                    margin_fraction: decimal("0.5"),
                    reserve_fraction: decimal("0.2"),
                },
                reduce_only: false,
                order_type: HlOrderType::Market {
                    max_slippage_bps: None,
                },
                client_order_id: None,
                expires_after: None,
            })
            .await
            .unwrap();

        assert_eq!(result.submitted.size, decimal("20"));
        assert_eq!(broker.position(&coin).unwrap().size, decimal("20"));
    }

    #[tokio::test]
    async fn reduce_only_order_cannot_increase_position() {
        let coin = HlCoin::new("BTC");
        let broker = broker();
        broker.set_mark_price(coin.clone(), decimal("100")).unwrap();
        broker
            .place_order(market_order(&coin, Side::Buy, "1"))
            .await
            .unwrap();
        let mut request = market_order(&coin, Side::Buy, "1");
        request.reduce_only = true;

        let error = broker.place_order(request).await.unwrap_err();

        assert!(error.to_string().contains("would increase"));
        assert_eq!(broker.position(&coin).unwrap().size, decimal("1"));
    }

    #[tokio::test]
    async fn rejects_non_market_order() {
        let coin = HlCoin::new("BTC");
        let broker = broker();
        broker.set_mark_price(coin.clone(), decimal("100")).unwrap();
        let mut request = market_order(&coin, Side::Buy, "1");
        request.order_type = HlOrderType::Limit {
            limit_price: decimal("100"),
            tif: crate::hyperliquid::types::HlTimeInForce::Gtc,
        };

        let error = broker.place_order(request).await.unwrap_err();

        assert!(error.to_string().contains("only supports market orders"));
    }

    #[test]
    fn validates_client_order_id() {
        assert!(HlClientOrderId::new("0x0123456789abcdef0123456789abcdef").is_ok());
        assert!(HlClientOrderId::new("0123456789abcdef0123456789abcdef").is_err());
        assert!(HlClientOrderId::new("0x1234").is_err());
        assert!(
            serde_json::from_str::<HlClientOrderId>("\"0x0123456789abcdef0123456789abcdef\"")
                .is_ok()
        );
        assert!(serde_json::from_str::<HlClientOrderId>("\"0x1234\"").is_err());
    }
}
