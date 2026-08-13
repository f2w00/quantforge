use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use alloy::primitives::Address;
use chrono::Utc;
use futures_util::TryFutureExt;

use crate::audit::{AuditAction, AuditRecord, RunJournal};
use crate::core::{Decimal, RunMode, Side, StrategyId};
use crate::hyperliquid::broker::live::{
    HlMarginMode, HlNetwork, calculate_close_size, ensure_minimum_order_notional,
    normalize_order_precision, protected_price, resolve_margin_mode, transport_error,
};
use crate::hyperliquid::broker::risk_adapter::order_risk_input_at_price;
use crate::hyperliquid::broker::traits::HyperliquidBroker;
use crate::hyperliquid::client::ws::{
    parse_cancel_response, parse_default_action_response, parse_order_outcome,
};
use crate::hyperliquid::client::{HyperliquidRestClient, HyperliquidSigner};
use crate::hyperliquid::types::{
    HlAccountState, HlCancelAction, HlCancelByCloidAction, HlCancelRequest, HlCancelResponse,
    HlCancelTarget, HlClientOrderId, HlCloseRequest, HlCloseSize, HlCoin, HlExchangeAction,
    HlMetadataSnapshot, HlOrderRequest, HlOrderResult, HlOrderSize, HlOrderType, HlSubmittedOrder,
    HlUpdateLeverageAction, HlWireCancel, HlWireCancelByCloid,
};
use crate::risk::{RiskDecision, RiskGuard};

use super::HlBrokerError;

const METADATA_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2 * 60 * 60);

#[derive(Clone, Debug)]
pub struct HlRestBrokerConfig {
    pub strategy_id: StrategyId,
    pub network: HlNetwork,
    pub account_address: Address,
    pub default_margin_mode: HlMarginMode,
    pub default_market_slippage_bps: u32,
    pub default_close_slippage_bps: u32,
}

impl HlRestBrokerConfig {
    pub fn new(strategy_id: StrategyId, account_address: Address) -> Self {
        Self {
            strategy_id,
            network: HlNetwork::Testnet,
            account_address,
            default_margin_mode: HlMarginMode::Auto,
            default_market_slippage_bps: 100,
            default_close_slippage_bps: 100,
        }
    }
}

pub struct HyperliquidRestBroker {
    strategy_id: StrategyId,
    journal: Arc<RunJournal>,
    risk_guard: RiskGuard,
    client: HyperliquidRestClient,
    metadata: Arc<RwLock<HlMetadataSnapshot>>,
    signer: Arc<HyperliquidSigner>,
    network: HlNetwork,
    account_address: String,
    next_client_order_id: AtomicU64,
    default_margin_mode: HlMarginMode,
    default_market_slippage_bps: u32,
    default_close_slippage_bps: u32,
}

impl HyperliquidRestBroker {
    pub async fn connect(
        config: HlRestBrokerConfig,
        signer: Arc<HyperliquidSigner>,
        risk_guard: RiskGuard,
        journal: Arc<RunJournal>,
    ) -> Result<Arc<Self>, HlBrokerError> {
        if config.default_market_slippage_bps >= 10_000
            || config.default_close_slippage_bps >= 10_000
        {
            return Err(HlBrokerError::InvalidRequest {
                message: "default slippage must be less than 10000 bps".to_string(),
            });
        }
        let client = HyperliquidRestClient::new(config.network.rest_url());
        let account_address = format!("{:#x}", config.account_address);
        let signer_address = format!("{:#x}", signer.wallet_address());
        let owner = client
            .agent_owner(&signer_address)
            .await
            .map_err(transport_error)?;
        if owner != config.account_address {
            return Err(HlBrokerError::InvalidRequest {
                message: format!(
                    "API wallet {signer_address} is authorized for {owner:#x}, not configured account {account_address}"
                ),
            });
        }
        let metadata = client.meta().await.map_err(transport_error)?;
        let broker = Arc::new(Self {
            strategy_id: config.strategy_id,
            journal,
            risk_guard,
            client,
            metadata: Arc::new(RwLock::new(metadata)),
            signer,
            network: config.network,
            account_address,
            next_client_order_id: AtomicU64::new(0),
            default_margin_mode: config.default_margin_mode,
            default_market_slippage_bps: config.default_market_slippage_bps,
            default_close_slippage_bps: config.default_close_slippage_bps,
        });
        broker.record_audit(
            AuditAction::Connect,
            None,
            serde_json::json!({"outcome": "accepted"}),
        );
        Arc::clone(&broker).spawn_metadata_refresh();
        Ok(broker)
    }

    fn spawn_metadata_refresh(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(METADATA_REFRESH_INTERVAL);
            interval.tick().await;
            loop {
                interval.tick().await;
                match self.client.meta().await {
                    Ok(metadata) => {
                        if let Ok(mut cached) = self.metadata.write() {
                            *cached = metadata;
                        }
                    }
                    Err(error) => self.record_audit(
                        AuditAction::ReconcileState,
                        None,
                        serde_json::json!({"stage": "metadata_refresh", "error": error.to_string()}),
                    ),
                }
            }
        });
    }

    fn record_audit(&self, action: AuditAction, symbol: Option<String>, data: serde_json::Value) {
        self.journal.record_audit(AuditRecord {
            strategy_id: self.strategy_id.clone(),
            mode: RunMode::Live,
            exchange: "hyperliquid".to_string(),
            symbol,
            action,
            data,
        });
    }

    fn next_client_order_id(&self) -> HlClientOrderId {
        let value = self.next_client_order_id.fetch_add(1, Ordering::SeqCst);
        HlClientOrderId::new(format!("0x{:032x}", value)).expect("generated client order id")
    }

    fn asset(
        &self,
        coin: &HlCoin,
    ) -> Result<crate::hyperliquid::types::HlAssetMeta, HlBrokerError> {
        self.metadata
            .read()
            .map_err(|_| HlBrokerError::StateUnavailable)?
            .asset(coin)
            .cloned()
            .ok_or_else(|| HlBrokerError::InvalidRequest {
                message: format!("unknown Hyperliquid coin {}", coin.0),
            })
    }

    async fn exchange(
        &self,
        action: HlExchangeAction,
        expires_after: Option<crate::core::Timestamp>,
    ) -> Result<serde_json::Value, HlBrokerError> {
        let signed = self
            .signer
            .sign_action(
                &action,
                self.signer.next_nonce(),
                None,
                expires_after.map(|value| value.timestamp_millis() as u64),
                self.network == HlNetwork::Mainnet,
            )
            .map_err(transport_error)?;
        self.client
            .exchange(signed.to_exchange_payload())
            .await
            .map_err(transport_error)
    }

    async fn set_leverage(&self, request: &HlOrderRequest) -> Result<(), HlBrokerError> {
        if request.reduce_only {
            return Ok(());
        }
        let leverage = request
            .leverage
            .ok_or_else(|| HlBrokerError::InvalidRequest {
                message: "opening orders require leverage".to_string(),
            })?;
        let asset = self.asset(&request.coin)?;
        if let Some(maximum) = asset.max_leverage {
            if leverage > maximum {
                return Err(HlBrokerError::InvalidRequest {
                    message: format!(
                        "leverage {leverage} exceeds {} maximum of {maximum}",
                        request.coin.0
                    ),
                });
            }
        }
        let margin_mode =
            resolve_margin_mode(self.default_margin_mode, asset.only_isolated, &request.coin)?;
        let action = HlExchangeAction::UpdateLeverage(HlUpdateLeverageAction {
            asset: asset.asset_id,
            is_cross: margin_mode.is_cross(),
            leverage,
        });
        let raw = self.exchange(action, None).await?;
        parse_default_action_response(&raw)
            .map_err(|message| HlBrokerError::ExchangeRejected { message, raw })
    }

    async fn place_order_inner(
        &self,
        mut request: HlOrderRequest,
    ) -> Result<HlOrderResult, HlBrokerError> {
        request
            .validate()
            .map_err(|message| HlBrokerError::InvalidRequest { message })?;
        if request
            .expires_after
            .is_some_and(|value| value <= Utc::now())
        {
            return Err(HlBrokerError::InvalidRequest {
                message: "order expiration must be in the future".to_string(),
            });
        }
        if request.client_order_id.is_none() {
            request.client_order_id = Some(self.next_client_order_id());
        }
        let asset = self.asset(&request.coin)?;
        let leverage_request = self.set_leverage(&request);
        let account_request = self.client.clearinghouse_state(&self.account_address);
        let spot_request = self.client.spot_clearinghouse_state(&self.account_address);
        let orders_request = self.client.open_orders(&self.account_address);
        let book_request = self.client.l2_book(&request.coin);
        let (mut account, spot, open_orders, book, ()) = tokio::try_join!(
            account_request.map_err(transport_error),
            spot_request.map_err(transport_error),
            orders_request.map_err(transport_error),
            book_request.map_err(transport_error),
            leverage_request,
        )?;
        account.equity = spot.total;
        let reference = if request.side == Side::Buy {
            book.best_ask
        } else {
            book.best_bid
        };
        let price = match &request.order_type {
            HlOrderType::Limit { limit_price, .. } => *limit_price,
            HlOrderType::Market { max_slippage_bps } => protected_price(
                reference,
                request.side,
                max_slippage_bps.unwrap_or(self.default_market_slippage_bps),
            ),
            HlOrderType::Trigger {
                trigger_price,
                execution,
                ..
            } => match execution {
                crate::hyperliquid::types::HlTriggerExecution::Market { max_slippage_bps } => {
                    protected_price(
                        *trigger_price,
                        request.side,
                        max_slippage_bps.unwrap_or(self.default_market_slippage_bps),
                    )
                }
                crate::hyperliquid::types::HlTriggerExecution::Limit { limit_price } => {
                    *limit_price
                }
            },
        };
        let size = match request.size {
            HlOrderSize::Exact(size) if size > Decimal::ZERO => size,
            HlOrderSize::Exact(_) => {
                return Err(HlBrokerError::InvalidRequest {
                    message: "order size must be positive".to_string(),
                });
            }
            HlOrderSize::MarginFraction {
                margin_fraction,
                reserve_fraction,
            } => {
                let leverage = request
                    .leverage
                    .expect("opening order leverage verified above");
                resolve_unified_margin_fraction_size(
                    spot.total,
                    spot.available_after_maintenance,
                    price,
                    asset.size_decimals,
                    leverage,
                    margin_fraction,
                    reserve_fraction,
                )?
            }
        };
        let (size, price) = normalize_order_precision(size, price, asset.size_decimals)
            .map_err(|message| HlBrokerError::InvalidRequest { message })?;
        if !request.reduce_only {
            ensure_minimum_order_notional(size, price)?;
        }
        let risk_input = order_risk_input_at_price(
            self.strategy_id.clone(),
            &account,
            &request,
            size,
            price,
            &open_orders,
            Decimal::ZERO,
        );
        if let RiskDecision::Rejected { violations } = self.risk_guard.check(&risk_input) {
            return Err(HlBrokerError::RiskRejected { violations });
        }
        let submitted = HlSubmittedOrder {
            coin: request.coin.clone(),
            side: request.side,
            size,
            limit_price: price,
            reduce_only: request.reduce_only,
            client_order_id: request.client_order_id.clone().expect("generated above"),
        };
        let raw = self
            .exchange(
                request.to_order_action(asset.asset_id, price, size),
                request.expires_after,
            )
            .await?;
        let outcome =
            parse_order_outcome(&raw).map_err(|message| HlBrokerError::ExchangeRejected {
                message,
                raw: raw.clone(),
            })?;
        Ok(HlOrderResult {
            submitted,
            outcome,
            raw,
        })
    }
}

fn resolve_unified_margin_fraction_size(
    total_collateral: Decimal,
    available_collateral: Decimal,
    reference_price: Decimal,
    size_decimals: u32,
    leverage: u32,
    margin_fraction: Decimal,
    reserve_fraction: Decimal,
) -> Result<Decimal, HlBrokerError> {
    if margin_fraction <= Decimal::ZERO || margin_fraction > Decimal::ONE {
        return Err(HlBrokerError::InvalidRequest {
            message: "margin fraction must be in (0, 1]".to_string(),
        });
    }
    if leverage == 0 {
        return Err(HlBrokerError::InvalidRequest {
            message: "leverage must be positive".to_string(),
        });
    }
    if reserve_fraction < Decimal::ZERO || reserve_fraction >= Decimal::ONE {
        return Err(HlBrokerError::InvalidRequest {
            message: "reserve fraction must be in [0, 1)".to_string(),
        });
    }
    if reference_price <= Decimal::ZERO {
        return Err(HlBrokerError::InvalidRequest {
            message: "reference price must be positive".to_string(),
        });
    }
    let reserve = total_collateral * reserve_fraction;
    let margin = total_collateral * margin_fraction;
    let available_after_reserve = (available_collateral - reserve).max(Decimal::ZERO);
    if margin > available_after_reserve {
        return Err(HlBrokerError::InvalidRequest {
            message: "planned margin exceeds available unified-account collateral".to_string(),
        });
    }
    let size = (margin * Decimal::from(leverage) / reference_price)
        .round_dp_with_strategy(size_decimals, rust_decimal::RoundingStrategy::ToZero);
    if size <= Decimal::ZERO {
        return Err(HlBrokerError::InvalidRequest {
            message: "sizing result is below the minimum quantity increment".to_string(),
        });
    }
    Ok(size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn margin_fraction_uses_total_unified_account_equity() {
        let size = resolve_unified_margin_fraction_size(
            Decimal::from(1_000),
            Decimal::from(500),
            Decimal::from(100),
            3,
            2,
            Decimal::new(1, 1),
            Decimal::ZERO,
        )
        .unwrap();

        assert_eq!(size, Decimal::from(2));
    }

    #[test]
    fn margin_fraction_rejects_insufficient_available_collateral() {
        assert!(
            resolve_unified_margin_fraction_size(
                Decimal::from(1_000),
                Decimal::from(50),
                Decimal::from(100),
                3,
                1,
                Decimal::new(1, 1),
                Decimal::ZERO,
            )
            .is_err()
        );
    }
}

#[async_trait::async_trait]
impl HyperliquidBroker for HyperliquidRestBroker {
    async fn account_state(&self) -> Result<HlAccountState, HlBrokerError> {
        let (mut account, spot) = tokio::try_join!(
            self.client
                .clearinghouse_state(&self.account_address)
                .map_err(transport_error),
            self.client
                .spot_clearinghouse_state(&self.account_address)
                .map_err(transport_error),
        )?;
        account.equity = spot.total;
        Ok(account)
    }

    async fn open_orders(
        &self,
    ) -> Result<Vec<crate::hyperliquid::types::HlOpenOrder>, HlBrokerError> {
        self.client
            .open_orders(&self.account_address)
            .await
            .map_err(transport_error)
    }

    async fn place_order(&self, request: HlOrderRequest) -> Result<HlOrderResult, HlBrokerError> {
        let result = self.place_order_inner(request.clone()).await;
        self.record_audit(
            AuditAction::PlaceOrder,
            Some(request.coin.0),
            serde_json::json!({
                "outcome": if result.is_ok() { "accepted" } else { "rejected" },
            }),
        );
        result
    }

    async fn cancel_order(
        &self,
        request: HlCancelRequest,
    ) -> Result<HlCancelResponse, HlBrokerError> {
        let asset = self.asset(&request.coin)?.asset_id;
        let action = match request.target {
            Some(HlCancelTarget::ClientOrderId(cloid)) => {
                HlExchangeAction::CancelByCloid(HlCancelByCloidAction {
                    cancels: vec![HlWireCancelByCloid {
                        asset,
                        client_order_id: cloid.as_str().to_string(),
                    }],
                    fast: request.fast,
                })
            }
            _ => HlExchangeAction::Cancel(HlCancelAction {
                cancels: vec![HlWireCancel {
                    asset,
                    order_id: request.order_id.0.parse().map_err(|_| {
                        HlBrokerError::InvalidRequest {
                            message: "Hyperliquid order id must be numeric".to_string(),
                        }
                    })?,
                }],
                fast: request.fast,
            }),
        };
        let raw = self.exchange(action, request.expires_after).await?;
        parse_cancel_response(raw.clone())
            .map_err(|message| HlBrokerError::ExchangeRejected { message, raw })
    }

    async fn close_position(
        &self,
        request: HlCloseRequest,
    ) -> Result<HlOrderResult, HlBrokerError> {
        let account = self
            .client
            .clearinghouse_state(&self.account_address)
            .await
            .map_err(transport_error)?;
        let position = account
            .positions
            .get(&request.coin)
            .cloned()
            .filter(|value| value.size != Decimal::ZERO)
            .ok_or_else(|| HlBrokerError::PositionUnavailable {
                coin: request.coin.clone(),
            })?;
        let asset = self.asset(&request.coin)?;
        let size = match request.size {
            HlCloseSize::Full => position.size.abs(),
            HlCloseSize::Exact(size) if size > Decimal::ZERO => size,
            HlCloseSize::Exact(_) => {
                return Err(HlBrokerError::InvalidRequest {
                    message: "close size must be positive".to_string(),
                });
            }
            HlCloseSize::Fraction(fraction) => {
                calculate_close_size(position.size, fraction, asset.size_decimals)?
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
            leverage: None,
            reduce_only: true,
            order_type: HlOrderType::Market {
                max_slippage_bps: request
                    .max_slippage_bps
                    .or(Some(self.default_close_slippage_bps)),
            },
            client_order_id: request.client_order_id,
            expires_after: request.expires_after,
        })
        .await
    }
}
