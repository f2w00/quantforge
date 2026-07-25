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
    Limit {
        tif: HlTimeInForce,
    },
    Trigger {
        is_market: bool,
        trigger_price: Decimal,
        trigger_kind: HlTriggerKind,
    },
}

impl HlOrderType {
    pub fn to_hyperliquid_json(&self) -> serde_json::Value {
        match self {
            Self::Limit { tif } => json!({
                "limit": {
                    "tif": tif.as_hyperliquid_str(),
                }
            }),
            Self::Trigger {
                is_market,
                trigger_price,
                trigger_kind,
            } => json!({
                "trigger": {
                    "isMarket": is_market,
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
    pub asset: Option<HlAssetId>,
    pub side: Side,
    pub size: Decimal,
    pub limit_price: Option<Decimal>,
    pub reduce_only: bool,
    pub time_in_force: Option<TimeInForce>,
    pub order_type: Option<HlOrderType>,
    pub grouping: HlOrderGrouping,
    pub client_order_id: Option<String>,
    pub expires_after: Option<Timestamp>,
    pub raw: serde_json::Value,
}

impl HlOrderRequest {
    pub fn order_type(&self) -> HlOrderType {
        self.order_type
            .clone()
            .unwrap_or_else(|| HlOrderType::Limit {
                tif: self.time_in_force.unwrap_or(TimeInForce::Gtc).into(),
            })
    }

    pub fn to_order_action(&self, asset: HlAssetId) -> HlExchangeAction {
        HlExchangeAction::Order(HlOrderAction {
            orders: vec![HlWireOrder::from_request(self, asset)],
            grouping: self.grouping,
        })
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
    ClientOrderId(String),
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

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct HlCloseOptions {
    pub client_order_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlWireOrder {
    pub asset: HlAssetId,
    pub is_buy: bool,
    pub price: Decimal,
    pub size: Decimal,
    pub reduce_only: bool,
    pub order_type: HlOrderType,
    pub client_order_id: Option<String>,
}

impl HlWireOrder {
    pub fn from_request(request: &HlOrderRequest, asset: HlAssetId) -> Self {
        Self {
            asset,
            is_buy: request.side == Side::Buy,
            price: request.limit_price.unwrap_or(Decimal::ZERO),
            size: request.size,
            reduce_only: request.reduce_only,
            order_type: request.order_type(),
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
            value["c"] = json!(client_order_id);
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
}

impl HlExchangeAction {
    pub fn to_hyperliquid_json(&self) -> serde_json::Value {
        match self {
            Self::Order(action) => action.to_hyperliquid_json(),
            Self::Cancel(action) => action.to_hyperliquid_json(),
            Self::CancelByCloid(action) => action.to_hyperliquid_json(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlSignature {
    pub r: String,
    pub s: String,
    pub v: String,
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
