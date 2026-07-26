use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use alloy::primitives::{Address, B256, keccak256};
use alloy::signers::{SignerSync, local::PrivateKeySigner};
use alloy::sol;
use alloy::sol_types::{SolStruct, eip712_domain};
use serde::Serialize;

use crate::hyperliquid::types::{
    HlCancelAction, HlCancelByCloidAction, HlExchangeAction, HlOrderAction, HlOrderType,
    HlSignature, HlSignedAction, HlTriggerExecution, HlUpdateLeverageAction,
};

sol! {
    struct Agent {
        string source;
        bytes32 connectionId;
    }
}

#[derive(Serialize)]
struct OrderActionWire {
    #[serde(rename = "type")]
    action_type: &'static str,
    orders: Vec<OrderWire>,
    grouping: String,
}

#[derive(Serialize)]
struct OrderWire {
    a: u32,
    b: bool,
    p: String,
    s: String,
    r: bool,
    t: OrderTypeWire,
    #[serde(skip_serializing_if = "Option::is_none")]
    c: Option<String>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum OrderTypeWire {
    Limit { limit: LimitWire },
    Trigger { trigger: TriggerWire },
}

#[derive(Serialize)]
struct LimitWire {
    tif: String,
}

#[derive(Serialize)]
struct TriggerWire {
    #[serde(rename = "isMarket")]
    is_market: bool,
    #[serde(rename = "triggerPx")]
    trigger_px: String,
    tpsl: String,
}

#[derive(Serialize)]
struct CancelActionWire {
    #[serde(rename = "type")]
    action_type: &'static str,
    cancels: Vec<CancelWire>,
    #[serde(skip_serializing_if = "is_false")]
    f: bool,
}

#[derive(Serialize)]
struct CancelByCloidActionWire {
    #[serde(rename = "type")]
    action_type: &'static str,
    cancels: Vec<CancelByCloidWire>,
    #[serde(skip_serializing_if = "is_false")]
    f: bool,
}

#[derive(Serialize)]
struct CancelWire {
    a: u32,
    o: u64,
}

#[derive(Serialize)]
struct CancelByCloidWire {
    asset: u32,
    cloid: String,
}

#[derive(Serialize)]
struct UpdateLeverageWire {
    #[serde(rename = "type")]
    action_type: &'static str,
    asset: u32,
    #[serde(rename = "isCross")]
    is_cross: bool,
    leverage: u32,
}

fn is_false(value: &bool) -> bool {
    !*value
}

pub struct HyperliquidSigner {
    wallet: PrivateKeySigner,
    wallet_address: Address,
    next_nonce: AtomicU64,
}

impl std::fmt::Debug for HyperliquidSigner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HyperliquidSigner")
            .field("wallet_address", &self.wallet_address)
            .finish_non_exhaustive()
    }
}

impl HyperliquidSigner {
    pub fn from_private_key(private_key: &str) -> anyhow::Result<Self> {
        let wallet = private_key.parse::<PrivateKeySigner>()?;
        let wallet_address = wallet.address();
        Ok(Self {
            wallet,
            wallet_address,
            next_nonce: AtomicU64::new(current_millis()),
        })
    }

    pub fn wallet_address(&self) -> Address {
        self.wallet_address
    }

    pub fn next_nonce(&self) -> u64 {
        let now = current_millis();
        self.next_nonce
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                Some(value.saturating_add(1).max(now))
            })
            .unwrap_or(now)
    }

    pub fn sign_action(
        &self,
        action: &HlExchangeAction,
        nonce: u64,
        vault_address: Option<Address>,
        expires_after: Option<u64>,
        is_mainnet: bool,
    ) -> anyhow::Result<HlSignedAction> {
        let action_bytes = action_msgpack(action)?;
        let connection_id = action_hash(&action_bytes, nonce, vault_address, expires_after);
        let source = if is_mainnet { "a" } else { "b" };
        let agent = Agent {
            source: source.to_string(),
            connectionId: connection_id,
        };
        let domain = eip712_domain! {
            name: "Exchange",
            version: "1",
            chain_id: 1337,
            verifying_contract: Address::ZERO,
        };
        let digest = agent.eip712_signing_hash(&domain);
        let signature = self.wallet.sign_hash_sync(&digest)?;

        Ok(HlSignedAction {
            action: action.clone(),
            nonce,
            signature: HlSignature {
                r: format!("0x{:064x}", signature.r()),
                s: format!("0x{:064x}", signature.s()),
                v: 27 + signature.v() as u64,
            },
            vault_address: vault_address.map(|address| format!("{address:?}")),
            expires_after,
        })
    }
}

fn current_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn action_hash(
    action_bytes: &[u8],
    nonce: u64,
    vault_address: Option<Address>,
    expires_after: Option<u64>,
) -> B256 {
    let mut bytes = action_bytes.to_vec();
    bytes.extend(nonce.to_be_bytes());
    match vault_address {
        Some(address) => {
            bytes.push(1);
            bytes.extend(address.as_slice());
        }
        None => bytes.push(0),
    }
    if let Some(expires_after) = expires_after {
        bytes.push(0);
        bytes.extend(expires_after.to_be_bytes());
    }
    keccak256(bytes)
}

fn action_msgpack(action: &HlExchangeAction) -> anyhow::Result<Vec<u8>> {
    match action {
        HlExchangeAction::Order(action) => {
            rmp_serde::to_vec_named(&order_action_wire(action)).map_err(Into::into)
        }
        HlExchangeAction::Cancel(action) => {
            rmp_serde::to_vec_named(&cancel_action_wire(action)).map_err(Into::into)
        }
        HlExchangeAction::CancelByCloid(action) => {
            rmp_serde::to_vec_named(&cancel_by_cloid_action_wire(action)).map_err(Into::into)
        }
        HlExchangeAction::UpdateLeverage(action) => {
            rmp_serde::to_vec_named(&update_leverage_wire(action)).map_err(Into::into)
        }
    }
}

fn order_action_wire(action: &HlOrderAction) -> OrderActionWire {
    OrderActionWire {
        action_type: "order",
        orders: action.orders.iter().map(order_wire).collect(),
        grouping: action.grouping.as_hyperliquid_str().to_string(),
    }
}

fn order_wire(order: &crate::hyperliquid::types::HlWireOrder) -> OrderWire {
    let order_type = match &order.order_type {
        HlOrderType::Market { .. } => OrderTypeWire::Limit {
            limit: LimitWire {
                tif: "Ioc".to_string(),
            },
        },
        HlOrderType::Limit { tif, .. } => OrderTypeWire::Limit {
            limit: LimitWire {
                tif: tif.as_hyperliquid_str().to_string(),
            },
        },
        HlOrderType::Trigger {
            trigger_price,
            trigger_kind,
            execution,
        } => OrderTypeWire::Trigger {
            trigger: TriggerWire {
                is_market: matches!(execution, HlTriggerExecution::Market { .. }),
                trigger_px: trigger_price.to_string(),
                tpsl: trigger_kind.as_hyperliquid_str().to_string(),
            },
        },
    };
    OrderWire {
        a: order.asset.0,
        b: order.is_buy,
        p: order.price.to_string(),
        s: order.size.to_string(),
        r: order.reduce_only,
        t: order_type,
        c: order
            .client_order_id
            .as_ref()
            .map(|id| id.as_str().to_string()),
    }
}

fn cancel_action_wire(action: &HlCancelAction) -> CancelActionWire {
    CancelActionWire {
        action_type: "cancel",
        cancels: action
            .cancels
            .iter()
            .map(|cancel| CancelWire {
                a: cancel.asset.0,
                o: cancel.order_id,
            })
            .collect(),
        f: action.fast,
    }
}

fn cancel_by_cloid_action_wire(action: &HlCancelByCloidAction) -> CancelByCloidActionWire {
    CancelByCloidActionWire {
        action_type: "cancelByCloid",
        cancels: action
            .cancels
            .iter()
            .map(|cancel| CancelByCloidWire {
                asset: cancel.asset.0,
                cloid: cancel.client_order_id.clone(),
            })
            .collect(),
        f: action.fast,
    }
}

fn update_leverage_wire(action: &HlUpdateLeverageAction) -> UpdateLeverageWire {
    UpdateLeverageWire {
        action_type: "updateLeverage",
        asset: action.asset.0,
        is_cross: action.is_cross,
        leverage: action.leverage,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hyperliquid::types::{HlAssetId, HlOrderGrouping, HlTimeInForce, HlWireOrder};
    use rust_decimal::Decimal;

    #[test]
    fn signs_official_rust_sdk_limit_order_vector() {
        let signer = HyperliquidSigner::from_private_key(
            "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e",
        )
        .unwrap();
        let action = HlExchangeAction::Order(HlOrderAction {
            orders: vec![HlWireOrder {
                asset: HlAssetId(1),
                is_buy: true,
                price: Decimal::new(20000, 1),
                size: Decimal::new(35, 1),
                reduce_only: false,
                order_type: HlOrderType::Limit {
                    limit_price: Decimal::new(20000, 1),
                    tif: HlTimeInForce::Ioc,
                },
                client_order_id: None,
            }],
            grouping: HlOrderGrouping::Na,
        });
        let signed = signer
            .sign_action(&action, 1_583_838, None, None, true)
            .unwrap();
        assert_eq!(
            signed.signature.r,
            "0x77957e58e70f43b6b68581f2dc42011fc384538a2e5b7bf42d5b936f19fbb673"
        );
        assert_eq!(
            signed.signature.s,
            "0x60721a8598727230f67080efee48c812a6a4442013fd3b0eed509171bef9f23f"
        );
        assert_eq!(signed.signature.v, 28);

        let signed = signer
            .sign_action(&action, 1_583_838, None, None, false)
            .unwrap();
        assert_eq!(
            signed.signature.r,
            "0xcd0925372ff1ed499e54883e9a6205ecfadec748f80ec463fe2f84f120964877"
        );
    }

    #[test]
    fn vault_and_expiration_are_part_of_action_hash() {
        let action = HlExchangeAction::Cancel(HlCancelAction {
            cancels: vec![],
            fast: false,
        });
        let bytes = action_msgpack(&action).unwrap();
        let vault = "0x0000000000000000000000000000000000000001"
            .parse::<Address>()
            .unwrap();
        let base = action_hash(&bytes, 10, None, None);
        let with_vault = action_hash(&bytes, 10, Some(vault), None);
        let with_expiration = action_hash(&bytes, 10, None, Some(20));

        assert_ne!(base, with_vault);
        assert_ne!(base, with_expiration);
        assert_ne!(with_vault, with_expiration);
    }
}
