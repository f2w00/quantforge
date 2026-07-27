use chrono::{TimeZone, Utc};
use reqwest::Client;
use serde::Deserialize;

use crate::core::{Decimal, OrderId, Side};
use crate::hyperliquid::types::{
    HlAccountState, HlClientOrderId, HlCoin, HlMetaResponse, HlMetadataSnapshot, HlMidSnapshot,
    HlOpenOrder, HlPosition,
};

const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

#[derive(Clone, Debug)]
pub struct HyperliquidRestClient {
    pub base_url: String,
    client: Client,
}

impl HyperliquidRestClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            client: Client::builder()
                .connect_timeout(CONNECT_TIMEOUT)
                .timeout(REQUEST_TIMEOUT)
                .build()
                .expect("valid Hyperliquid REST client configuration"),
        }
    }

    pub async fn meta(&self) -> anyhow::Result<HlMetadataSnapshot> {
        let response: HlMetaResponse = self.info(serde_json::json!({ "type": "meta" })).await?;
        Ok(response.into_snapshot(Utc::now()))
    }

    pub async fn all_mids(&self) -> anyhow::Result<HlMidSnapshot> {
        let values: std::collections::HashMap<String, String> =
            self.info(serde_json::json!({ "type": "allMids" })).await?;
        let mids = values
            .into_iter()
            .map(|(coin, price)| Ok((HlCoin::new(coin), price.parse()?)))
            .collect::<anyhow::Result<_>>()?;
        Ok(HlMidSnapshot {
            mids,
            updated_at: Some(Utc::now()),
        })
    }

    pub async fn clearinghouse_state(&self, user: &str) -> anyhow::Result<HlAccountState> {
        let response: ClearinghouseStateWire = self
            .info(serde_json::json!({
                "type": "clearinghouseState",
                "user": user,
            }))
            .await?;
        response.try_into()
    }

    pub async fn open_orders(&self, user: &str) -> anyhow::Result<Vec<HlOpenOrder>> {
        let response: Vec<OpenOrderWire> = self
            .info(serde_json::json!({
                "type": "openOrders",
                "user": user,
            }))
            .await?;
        response.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn order_status(&self, user: &str, oid: &str) -> anyhow::Result<serde_json::Value> {
        self.info(serde_json::json!({
            "type": "orderStatus",
            "user": user,
            "oid": oid,
        }))
        .await
    }

    pub async fn agent_owner(&self, agent: &str) -> anyhow::Result<alloy::primitives::Address> {
        let response: UserRoleWire = self
            .info(serde_json::json!({
                "type": "userRole",
                "user": agent,
            }))
            .await?;
        parse_agent_owner(agent, response)
    }

    pub async fn exchange(&self, payload: serde_json::Value) -> anyhow::Result<serde_json::Value> {
        let url = format!("{}/exchange", self.base_url.trim_end_matches('/'));
        Ok(self
            .client
            .post(url)
            .json(&payload)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    async fn info<T: serde::de::DeserializeOwned>(
        &self,
        request: serde_json::Value,
    ) -> anyhow::Result<T> {
        let url = format!("{}/info", self.base_url.trim_end_matches('/'));
        Ok(self
            .client
            .post(url)
            .json(&request)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }
}

#[derive(Debug, Deserialize)]
struct UserRoleWire {
    role: String,
    data: Option<UserRoleDataWire>,
}

#[derive(Debug, Deserialize)]
struct UserRoleDataWire {
    user: String,
}

fn parse_agent_owner(
    agent: &str,
    response: UserRoleWire,
) -> anyhow::Result<alloy::primitives::Address> {
    if response.role != "agent" {
        anyhow::bail!(
            "signer {agent} is not an API wallet (role: {})",
            response.role
        );
    }
    response
        .data
        .and_then(|data| data.user.parse().ok())
        .ok_or_else(|| anyhow::anyhow!("API wallet {agent} has no valid owner"))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClearinghouseStateWire {
    margin_summary: MarginSummaryWire,
    asset_positions: Vec<AssetPositionWire>,
    time: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MarginSummaryWire {
    account_value: String,
    total_margin_used: String,
}

#[derive(Debug, Deserialize)]
struct AssetPositionWire {
    position: PositionWire,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PositionWire {
    coin: String,
    szi: String,
    entry_px: Option<String>,
    position_value: String,
    unrealized_pnl: String,
    return_on_equity: String,
    leverage: LeverageWire,
    liquidation_px: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LeverageWire {
    value: u32,
}

impl TryFrom<ClearinghouseStateWire> for HlAccountState {
    type Error = anyhow::Error;

    fn try_from(value: ClearinghouseStateWire) -> Result<Self, Self::Error> {
        let positions = value
            .asset_positions
            .into_iter()
            .map(|asset| {
                let position = asset.position;
                let coin = HlCoin::new(position.coin);
                Ok((
                    coin.clone(),
                    HlPosition {
                        coin,
                        size: position.szi.parse()?,
                        entry_price: position.entry_px.map(|price| price.parse()).transpose()?,
                        notional: position.position_value.parse::<Decimal>()?.abs(),
                        unrealized_pnl: position.unrealized_pnl.parse()?,
                        return_on_equity: position.return_on_equity.parse()?,
                        leverage: Decimal::from(position.leverage.value),
                        liquidation_price: position
                            .liquidation_px
                            .map(|price| price.parse())
                            .transpose()?,
                    },
                ))
            })
            .collect::<anyhow::Result<_>>()?;
        Ok(HlAccountState {
            equity: value.margin_summary.account_value.parse()?,
            margin_used: value.margin_summary.total_margin_used.parse()?,
            positions,
            updated_at: Utc
                .timestamp_millis_opt(value.time)
                .single()
                .ok_or_else(|| anyhow::anyhow!("invalid clearinghouseState timestamp"))?,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OpenOrderWire {
    coin: String,
    oid: u64,
    cloid: Option<String>,
    side: String,
    sz: String,
    limit_px: String,
    #[serde(default)]
    reduce_only: bool,
}

impl TryFrom<OpenOrderWire> for HlOpenOrder {
    type Error = anyhow::Error;

    fn try_from(value: OpenOrderWire) -> Result<Self, Self::Error> {
        let side = match value.side.as_str() {
            "B" => Side::Buy,
            "A" => Side::Sell,
            side => anyhow::bail!("unsupported Hyperliquid order side {side}"),
        };
        Ok(HlOpenOrder {
            coin: HlCoin::new(value.coin),
            order_id: OrderId::new(value.oid.to_string()),
            client_order_id: value
                .cloid
                .map(HlClientOrderId::new)
                .transpose()
                .map_err(anyhow::Error::msg)?,
            side,
            remaining_size: value.sz.parse()?,
            limit_price: value.limit_px.parse()?,
            reduce_only: value.reduce_only,
        })
    }
}

pub(crate) fn parse_ws_clearinghouse_state(
    message: &serde_json::Value,
) -> anyhow::Result<HlAccountState> {
    let value = message
        .pointer("/data/clearinghouseState")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("missing clearinghouseState data"))?;
    serde_json::from_value::<ClearinghouseStateWire>(value)?.try_into()
}

pub(crate) fn parse_ws_open_orders(
    message: &serde_json::Value,
) -> anyhow::Result<Vec<HlOpenOrder>> {
    let value = message
        .pointer("/data/orders")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("missing openOrders data"))?;
    serde_json::from_value::<Vec<OpenOrderWire>>(value)?
        .into_iter()
        .map(TryInto::try_into)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_agent_role_and_returns_owner() {
        let owner = parse_agent_owner(
            "0x0000000000000000000000000000000000000001",
            UserRoleWire {
                role: "agent".to_string(),
                data: Some(UserRoleDataWire {
                    user: "0x0000000000000000000000000000000000000002".to_string(),
                }),
            },
        )
        .unwrap();
        assert_eq!(
            owner,
            "0x0000000000000000000000000000000000000002"
                .parse::<alloy::primitives::Address>()
                .unwrap()
        );
    }

    #[test]
    fn rejects_non_agent_role() {
        let error = parse_agent_owner(
            "0x0000000000000000000000000000000000000001",
            UserRoleWire {
                role: "user".to_string(),
                data: None,
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("not an API wallet"));
    }

    #[test]
    fn parses_clearinghouse_snapshot() {
        let message = serde_json::json!({
            "data": {
                "clearinghouseState": {
                    "marginSummary": {
                        "accountValue": "1000",
                        "totalMarginUsed": "25"
                    },
                    "assetPositions": [{
                        "position": {
                            "coin": "BTC",
                            "szi": "0.1",
                            "entryPx": "100000",
                            "positionValue": "10000",
                            "unrealizedPnl": "10",
                            "returnOnEquity": "0.01",
                            "leverage": {"value": 10},
                            "liquidationPx": "90000"
                        }
                    }],
                    "time": 1_700_000_000_000_i64
                }
            }
        });
        let account = parse_ws_clearinghouse_state(&message).unwrap();
        assert_eq!(account.equity, "1000".parse().unwrap());
        let position = account.positions.get(&HlCoin::new("BTC")).unwrap();
        assert_eq!(position.coin, HlCoin::new("BTC"));
        assert_eq!(position.size, "0.1".parse().unwrap());
        assert_eq!(position.unrealized_pnl, "10".parse().unwrap());
        assert_eq!(position.return_on_equity, "0.01".parse().unwrap());
        assert_eq!(account.updated_at.timestamp_millis(), 1_700_000_000_000);
    }

    #[test]
    fn parses_open_orders_snapshot() {
        let message = serde_json::json!({
            "data": {
                "orders": [{
                    "coin": "ETH",
                    "oid": 42,
                    "cloid": "0x00000000000000000000000000000001",
                    "side": "B",
                    "sz": "1.5",
                    "limitPx": "3000",
                    "reduceOnly": false
                }]
            }
        });
        let orders = parse_ws_open_orders(&message).unwrap();
        assert_eq!(orders[0].order_id.0, "42");
        assert_eq!(
            orders[0].client_order_id.as_ref().unwrap().as_str(),
            "0x00000000000000000000000000000001"
        );
        assert_eq!(orders[0].side, Side::Buy);
        assert_eq!(orders[0].remaining_size, "1.5".parse().unwrap());
        assert_eq!(orders[0].limit_price, "3000".parse().unwrap());
    }
}
