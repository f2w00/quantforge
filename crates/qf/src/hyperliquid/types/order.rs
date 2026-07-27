use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::core::{Decimal, OrderId, Side, TimeInForce, Timestamp};
use crate::hyperliquid::types::HlCoin;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct HlAssetId(pub u32);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub enum HlTimeInForce {
    Alo,
    Ioc,
    Gtc,
}

impl From<TimeInForce> for HlTimeInForce {
    fn from(value: TimeInForce) -> Self {
        match value {
            TimeInForce::Alo => Self::Alo,
            TimeInForce::Ioc => Self::Ioc,
            TimeInForce::Gtc => Self::Gtc,
        }
    }
}

impl HlTimeInForce {
    pub fn as_hyperliquid_str(self) -> &'static str {
        match self {
            Self::Alo => "Alo",
            Self::Ioc => "Ioc",
            Self::Gtc => "Gtc",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub enum HlTriggerKind {
    TakeProfit,
    StopLoss,
}

impl HlTriggerKind {
    pub fn as_hyperliquid_str(self) -> &'static str {
        match self {
            Self::TakeProfit => "tp",
            Self::StopLoss => "sl",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum HlOrderType {
    Market {
        max_slippage_bps: Option<u32>,
    },
    Limit {
        limit_price: Decimal,
        tif: HlTimeInForce,
    },
    Trigger {
        trigger_price: Decimal,
        trigger_kind: HlTriggerKind,
        execution: HlTriggerExecution,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum HlTriggerExecution {
    Market { max_slippage_bps: Option<u32> },
    Limit { limit_price: Decimal },
}

impl HlOrderType {
    pub fn to_hyperliquid_json(&self) -> serde_json::Value {
        match self {
            // Hyperliquid 没有独立的市价单类型，实盘发送时由价格保护的 IOC 单承载。
            Self::Market { .. } => json!({
                "limit": {
                    "tif": "Ioc",
                }
            }),
            Self::Limit { tif, .. } => json!({
                "limit": {
                    "tif": tif.as_hyperliquid_str(),
                }
            }),
            Self::Trigger {
                trigger_price,
                trigger_kind,
                execution,
            } => json!({
                "trigger": {
                    "isMarket": matches!(execution, HlTriggerExecution::Market { .. }),
                    "triggerPx": trigger_price.to_string(),
                    "tpsl": trigger_kind.as_hyperliquid_str(),
                }
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub enum HlOrderGrouping {
    Na,
    NormalTpsl,
    PositionTpsl,
}

impl HlOrderGrouping {
    pub fn as_hyperliquid_str(self) -> &'static str {
        match self {
            Self::Na => "na",
            Self::NormalTpsl => "normalTpsl",
            Self::PositionTpsl => "positionTpsl",
        }
    }
}

impl Default for HlOrderGrouping {
    fn default() -> Self {
        Self::Na
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlOrderRequest {
    pub coin: HlCoin,
    pub side: Side,
    pub size: HlOrderSize,
    pub reduce_only: bool,
    pub order_type: HlOrderType,
    pub client_order_id: Option<HlClientOrderId>,
    pub expires_after: Option<Timestamp>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum HlOrderSize {
    Exact(Decimal),
    MarginFraction {
        margin_fraction: Decimal,
        reserve_fraction: Decimal,
    },
}

impl HlOrderRequest {
    pub fn validate(&self) -> Result<(), String> {
        match &self.order_type {
            HlOrderType::Market { max_slippage_bps } => {
                if let Some(max_slippage_bps) = max_slippage_bps {
                    validate_slippage(*max_slippage_bps)?;
                }
            }
            HlOrderType::Limit { limit_price, .. } => {
                validate_price(*limit_price, "limit price")?;
            }
            HlOrderType::Trigger {
                trigger_price,
                execution,
                ..
            } => {
                if !self.reduce_only {
                    return Err("trigger TP/SL orders must be reduce-only".to_string());
                }
                validate_price(*trigger_price, "trigger price")?;
                match execution {
                    HlTriggerExecution::Market { max_slippage_bps } => {
                        if let Some(max_slippage_bps) = max_slippage_bps {
                            validate_slippage(*max_slippage_bps)?;
                        }
                    }
                    HlTriggerExecution::Limit { limit_price } => {
                        validate_price(*limit_price, "trigger limit price")?;
                    }
                }
            }
        }

        Ok(())
    }

    pub fn to_order_action(
        &self,
        asset: HlAssetId,
        price: Decimal,
        size: Decimal,
    ) -> HlExchangeAction {
        HlExchangeAction::Order(HlOrderAction {
            orders: vec![HlWireOrder::from_request(self, asset, price, size)],
            grouping: HlOrderGrouping::Na,
        })
    }
}

fn validate_price(price: Decimal, name: &str) -> Result<(), String> {
    if price <= Decimal::ZERO {
        return Err(format!("{name} must be positive"));
    }
    Ok(())
}

fn validate_slippage(max_slippage_bps: u32) -> Result<(), String> {
    if max_slippage_bps >= 10_000 {
        return Err("max slippage must be less than 10000 bps".to_string());
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
#[serde(try_from = "String", into = "String")]
pub struct HlClientOrderId(String);

impl HlClientOrderId {
    pub fn new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        let hex = value
            .strip_prefix("0x")
            .ok_or_else(|| "client order id must start with 0x".to_string())?;
        if hex.len() != 32 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err("client order id must contain exactly 16 hex bytes".to_string());
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for HlClientOrderId {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<HlClientOrderId> for String {
    fn from(value: HlClientOrderId) -> Self {
        value.0
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlCancelRequest {
    pub coin: HlCoin,
    pub asset: Option<HlAssetId>,
    pub order_id: OrderId,
    pub target: Option<HlCancelTarget>,
    pub fast: bool,
    pub expires_after: Option<Timestamp>,
}

impl HlCancelRequest {
    pub fn to_cancel_action(&self, asset: HlAssetId, order_id: u64) -> HlExchangeAction {
        HlExchangeAction::Cancel(HlCancelAction {
            cancels: vec![HlWireCancel { asset, order_id }],
            fast: self.fast,
        })
    }

    pub fn to_cancel_by_cloid_action(
        &self,
        asset: HlAssetId,
        client_order_id: impl Into<String>,
    ) -> HlExchangeAction {
        HlExchangeAction::CancelByCloid(HlCancelByCloidAction {
            cancels: vec![HlWireCancelByCloid {
                asset,
                client_order_id: client_order_id.into(),
            }],
            fast: self.fast,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum HlCancelTarget {
    OrderId(u64),
    ClientOrderId(HlClientOrderId),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlOpenOrder {
    pub coin: HlCoin,
    pub order_id: OrderId,
    pub side: Side,
    pub size: Decimal,
    pub limit_price: Option<Decimal>,
    pub reduce_only: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlCloseRequest {
    pub coin: HlCoin,
    pub size: HlCloseSize,
    pub max_slippage_bps: Option<u32>,
    pub client_order_id: Option<HlClientOrderId>,
    pub expires_after: Option<Timestamp>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum HlCloseSize {
    Full,
    Exact(Decimal),
    Fraction(Decimal),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlWireOrder {
    pub asset: HlAssetId,
    pub is_buy: bool,
    pub price: Decimal,
    pub size: Decimal,
    pub reduce_only: bool,
    pub order_type: HlOrderType,
    pub client_order_id: Option<HlClientOrderId>,
}

impl HlWireOrder {
    pub fn from_request(
        request: &HlOrderRequest,
        asset: HlAssetId,
        price: Decimal,
        size: Decimal,
    ) -> Self {
        Self {
            asset,
            is_buy: request.side == Side::Buy,
            price,
            size,
            reduce_only: request.reduce_only,
            order_type: request.order_type.clone(),
            client_order_id: request.client_order_id.clone(),
        }
    }

    pub fn to_hyperliquid_json(&self) -> serde_json::Value {
        let mut value = json!({
            "a": self.asset.0,
            "b": self.is_buy,
            "p": self.price.to_string(),
            "s": self.size.to_string(),
            "r": self.reduce_only,
            "t": self.order_type.to_hyperliquid_json(),
        });

        if let Some(client_order_id) = &self.client_order_id {
            value["c"] = json!(client_order_id.as_str());
        }

        value
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlOrderAction {
    pub orders: Vec<HlWireOrder>,
    pub grouping: HlOrderGrouping,
}

impl HlOrderAction {
    pub fn to_hyperliquid_json(&self) -> serde_json::Value {
        json!({
            "type": "order",
            "orders": self
                .orders
                .iter()
                .map(HlWireOrder::to_hyperliquid_json)
                .collect::<Vec<_>>(),
            "grouping": self.grouping.as_hyperliquid_str(),
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlWireCancel {
    pub asset: HlAssetId,
    pub order_id: u64,
}

impl HlWireCancel {
    pub fn to_hyperliquid_json(&self) -> serde_json::Value {
        json!({
            "a": self.asset.0,
            "o": self.order_id,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlWireCancelByCloid {
    pub asset: HlAssetId,
    pub client_order_id: String,
}

impl HlWireCancelByCloid {
    pub fn to_hyperliquid_json(&self) -> serde_json::Value {
        json!({
            "asset": self.asset.0,
            "cloid": self.client_order_id,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlCancelAction {
    pub cancels: Vec<HlWireCancel>,
    pub fast: bool,
}

impl HlCancelAction {
    pub fn to_hyperliquid_json(&self) -> serde_json::Value {
        let mut value = json!({
            "type": "cancel",
            "cancels": self
                .cancels
                .iter()
                .map(HlWireCancel::to_hyperliquid_json)
                .collect::<Vec<_>>(),
        });

        // Hyperliquid 要求 false 时不要编码 f 字段，否则 action hash 会不一致。
        if self.fast {
            value["f"] = json!(true);
        }

        value
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlCancelByCloidAction {
    pub cancels: Vec<HlWireCancelByCloid>,
    pub fast: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlUpdateLeverageAction {
    pub asset: HlAssetId,
    pub is_cross: bool,
    pub leverage: u32,
}

impl HlUpdateLeverageAction {
    pub fn to_hyperliquid_json(&self) -> serde_json::Value {
        json!({
            "type": "updateLeverage",
            "asset": self.asset.0,
            "isCross": self.is_cross,
            "leverage": self.leverage,
        })
    }
}

impl HlCancelByCloidAction {
    pub fn to_hyperliquid_json(&self) -> serde_json::Value {
        let mut value = json!({
            "type": "cancelByCloid",
            "cancels": self
                .cancels
                .iter()
                .map(HlWireCancelByCloid::to_hyperliquid_json)
                .collect::<Vec<_>>(),
        });

        // Hyperliquid 要求 false 时不要编码 f 字段，否则 action hash 会不一致。
        if self.fast {
            value["f"] = json!(true);
        }

        value
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum HlExchangeAction {
    Order(HlOrderAction),
    Cancel(HlCancelAction),
    CancelByCloid(HlCancelByCloidAction),
    UpdateLeverage(HlUpdateLeverageAction),
}

impl HlExchangeAction {
    pub fn to_hyperliquid_json(&self) -> serde_json::Value {
        match self {
            Self::Order(action) => action.to_hyperliquid_json(),
            Self::Cancel(action) => action.to_hyperliquid_json(),
            Self::CancelByCloid(action) => action.to_hyperliquid_json(),
            Self::UpdateLeverage(action) => action.to_hyperliquid_json(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_update_leverage_action() {
        let action = HlExchangeAction::UpdateLeverage(HlUpdateLeverageAction {
            asset: HlAssetId(7),
            is_cross: true,
            leverage: 5,
        });

        assert_eq!(
            action.to_hyperliquid_json(),
            json!({
                "type": "updateLeverage",
                "asset": 7,
                "isCross": true,
                "leverage": 5,
            })
        );
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlSignature {
    pub r: String,
    pub s: String,
    pub v: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlSignedAction {
    pub action: HlExchangeAction,
    pub nonce: u64,
    pub signature: HlSignature,
    pub vault_address: Option<String>,
    pub expires_after: Option<u64>,
}

impl HlSignedAction {
    pub fn to_exchange_payload(&self) -> serde_json::Value {
        let mut value = json!({
            "action": self.action.to_hyperliquid_json(),
            "nonce": self.nonce,
            "signature": self.signature,
        });

        if let Some(vault_address) = &self.vault_address {
            value["vaultAddress"] = json!(vault_address);
        }

        if let Some(expires_after) = self.expires_after {
            value["expiresAfter"] = json!(expires_after);
        }

        value
    }

    pub fn to_ws_post_payload(&self, id: u64) -> serde_json::Value {
        json!({
            "method": "post",
            "id": id,
            "request": {
                "type": "action",
                "payload": self.to_exchange_payload(),
            }
        })
    }
}
