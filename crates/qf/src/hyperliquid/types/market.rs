use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::core::{Decimal, Timestamp};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct HlCoin(pub String);

impl HlCoin {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlMarkPrice {
    pub coin: HlCoin,
    pub price: Decimal,
    pub timestamp: Timestamp,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HlAssetMeta {
    pub coin: HlCoin,
    pub asset_id: super::HlAssetId,
    pub size_decimals: u32,
    pub max_leverage: Option<u32>,
    pub only_isolated: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct HlMetadataSnapshot {
    pub assets: HashMap<HlCoin, HlAssetMeta>,
    pub updated_at: Option<DateTime<Utc>>,
}

impl HlMetadataSnapshot {
    pub fn asset(&self, coin: &HlCoin) -> Option<&HlAssetMeta> {
        self.assets.get(coin)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct HlMidSnapshot {
    pub mids: HashMap<HlCoin, Decimal>,
    pub updated_at: Option<DateTime<Utc>>,
}

impl HlMidSnapshot {
    pub fn apply_ws_message(&mut self, message: &serde_json::Value) -> anyhow::Result<()> {
        let mids = message
            .pointer("/data/mids")
            .ok_or_else(|| anyhow::anyhow!("allMids message is missing data.mids"))?
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("allMids data.mids must be an object"))?;

        for (coin, price) in mids {
            let price = price
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("allMids price must be a string"))?
                .parse()?;
            self.mids.insert(HlCoin::new(coin), price);
        }
        self.updated_at = Some(Utc::now());
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct HlMetaResponse {
    pub universe: Vec<HlMetaUniverseEntry>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct HlMetaUniverseEntry {
    pub name: String,
    #[serde(rename = "szDecimals")]
    pub size_decimals: u32,
    #[serde(rename = "maxLeverage")]
    pub max_leverage: Option<u32>,
    #[serde(rename = "onlyIsolated")]
    #[serde(default)]
    pub only_isolated: bool,
}

impl HlMetaResponse {
    pub fn into_snapshot(self, updated_at: DateTime<Utc>) -> HlMetadataSnapshot {
        let assets = self
            .universe
            .into_iter()
            .enumerate()
            .map(|(asset_id, entry)| {
                let coin = HlCoin::new(entry.name);
                let meta = HlAssetMeta {
                    coin: coin.clone(),
                    asset_id: super::HlAssetId(asset_id as u32),
                    size_decimals: entry.size_decimals,
                    max_leverage: entry.max_leverage,
                    only_isolated: entry.only_isolated,
                };
                (coin, meta)
            })
            .collect();

        HlMetadataSnapshot {
            assets,
            updated_at: Some(updated_at),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_meta_universe_to_asset_ids() {
        let snapshot = HlMetaResponse {
            universe: vec![
                HlMetaUniverseEntry {
                    name: "BTC".to_string(),
                    size_decimals: 5,
                    max_leverage: Some(40),
                    only_isolated: false,
                },
                HlMetaUniverseEntry {
                    name: "ETH".to_string(),
                    size_decimals: 4,
                    max_leverage: Some(25),
                    only_isolated: true,
                },
            ],
        }
        .into_snapshot(Utc::now());

        assert_eq!(snapshot.asset(&HlCoin::new("BTC")).unwrap().asset_id.0, 0);
        assert_eq!(snapshot.asset(&HlCoin::new("ETH")).unwrap().asset_id.0, 1);
        assert_eq!(
            snapshot.asset(&HlCoin::new("ETH")).unwrap().size_decimals,
            4
        );
    }

    #[test]
    fn applies_all_mids_updates() {
        let mut snapshot = HlMidSnapshot::default();
        snapshot
            .apply_ws_message(&serde_json::json!({
                "channel": "allMids",
                "data": {"mids": {"BTC": "100.5", "ETH": "3.25"}}
            }))
            .unwrap();

        assert_eq!(snapshot.mids[&HlCoin::new("BTC")], "100.5".parse().unwrap());
        assert_eq!(snapshot.mids[&HlCoin::new("ETH")], "3.25".parse().unwrap());
    }
}
