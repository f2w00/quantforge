use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use qf::core::{Decimal, Side};
use qf::hyperliquid::broker::HyperliquidBroker;
use qf::hyperliquid::client::HlUserFill;
use qf::hyperliquid::types::{
    HlCloseRequest, HlCloseSize, HlCoin, HlOrderRequest, HlOrderSize, HlOrderType,
};
use qf::performance::PerformanceReport;
use qf::storage::JsonlLedgerReader;
use serde::Deserialize;

pub const COIN: &str = "HYPE";
pub const MAX_ACTIVE_OIDS: usize = 5;
pub const TIERS: [Decimal; 5] = [
    Decimal::from_parts(400, 0, 0, false, 0),
    Decimal::from_parts(800, 0, 0, false, 0),
    Decimal::from_parts(1600, 0, 0, false, 0),
    Decimal::from_parts(2000, 0, 0, false, 0),
    Decimal::from_parts(2400, 0, 0, false, 0),
];
pub const POS_PCTS: [Decimal; 5] = [
    Decimal::from_parts(15, 0, 0, false, 2),
    Decimal::from_parts(20, 0, 0, false, 2),
    Decimal::from_parts(25, 0, 0, false, 2),
    Decimal::from_parts(30, 0, 0, false, 2),
    Decimal::from_parts(35, 0, 0, false, 2),
];

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    #[serde(default = "default_network")]
    pub network: String,
    pub leader: String,
    #[serde(default = "default_leverage")]
    pub leverage: u32,
    #[serde(default = "default_initial_equity")]
    pub initial_equity: Decimal,
    #[serde(default)]
    pub taker_fee_bps: u32,
    #[serde(default)]
    pub market_slippage_bps: u32,
    #[serde(default = "default_close_slippage")]
    pub close_slippage_bps: u32,
}

fn default_network() -> String {
    "testnet".to_string()
}
fn default_leverage() -> u32 {
    1
}
fn default_initial_equity() -> Decimal {
    Decimal::from(600)
}
fn default_close_slippage() -> u32 {
    100
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Direction {
    Long,
    Short,
}

#[derive(Clone, Debug)]
struct ActiveOid {
    direction: Direction,
    cumulative_size: Decimal,
    tier: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct CopyState {
    pub leader_size: Decimal,
    active: HashMap<String, ActiveOid>,
    processed_fills: HashSet<String>,
    pub desired_size: Decimal,
}

impl Default for CopyState {
    fn default() -> Self {
        Self {
            leader_size: Decimal::ZERO,
            active: HashMap::new(),
            processed_fills: HashSet::new(),
            desired_size: Decimal::ZERO,
        }
    }
}

impl CopyState {
    pub fn on_fill(&mut self, fill: &HlUserFill) {
        let (Some(oid), Some(size), Some(direction)) =
            (fill.order_id.clone(), fill.size, fill.direction.as_deref())
        else {
            return;
        };
        if fill.coin.as_deref() != Some(COIN)
            || size <= Decimal::ZERO
            || !matches!(direction, "Open Long" | "Open Short")
        {
            return;
        }
        let fill_id = fill.trade_id.clone().unwrap_or_else(|| {
            format!(
                "{oid}:{}:{}",
                fill.timestamp
                    .map(|value| value.timestamp_millis())
                    .unwrap_or_default(),
                size,
            )
        });
        if !self.processed_fills.insert(fill_id) {
            return;
        }
        let direction = if direction == "Open Long" {
            Direction::Long
        } else {
            Direction::Short
        };
        let existing = self.active.get(&oid);
        let existing_tier = existing.and_then(|value| value.tier);
        let existing_size = existing
            .map(|value| value.cumulative_size)
            .unwrap_or(Decimal::ZERO);
        let next_tier = tier_for_size(existing_size + size, existing_tier);
        if existing.is_none()
            && next_tier.is_some()
            && self
                .active
                .values()
                .filter(|value| value.tier.is_some())
                .count()
                >= MAX_ACTIVE_OIDS
        {
            return;
        }
        let entry = self.active.entry(oid).or_insert(ActiveOid {
            direction,
            cumulative_size: Decimal::ZERO,
            tier: None,
        });
        if entry.direction != direction {
            return;
        }
        entry.cumulative_size += size;
        if let Some(new_tier) = next_tier {
            entry.tier = new_tier;
        }
    }

    pub fn on_snapshot(&mut self, current_size: Decimal, equity: Decimal, price: Decimal) {
        if price <= Decimal::ZERO {
            return;
        }
        let previous = self.leader_size;
        self.leader_size = current_size;
        if current_size == previous {
            return;
        }
        let reversal = previous != Decimal::ZERO
            && current_size != Decimal::ZERO
            && previous.is_sign_positive() != current_size.is_sign_positive();
        if reversal || current_size == Decimal::ZERO {
            if current_size == Decimal::ZERO {
                self.active.clear();
            } else {
                self.active.retain(|_, value| {
                    (current_size.is_sign_positive() && value.direction == Direction::Long)
                        || (current_size.is_sign_negative() && value.direction == Direction::Short)
                });
            }
            self.desired_size = Decimal::ZERO;
        }
        if current_size == Decimal::ZERO {
            return;
        }
        if previous != Decimal::ZERO
            && previous.is_sign_positive() == current_size.is_sign_positive()
            && current_size.abs() < previous.abs()
        {
            self.desired_size = (self.desired_size * current_size.abs() / previous.abs())
                .round_dp_with_strategy(8, rust_decimal::RoundingStrategy::ToZero);
        } else {
            self.desired_size = self.target_size(equity, price, current_size);
        }
    }

    pub fn refresh_target(&mut self, equity: Decimal, price: Decimal) {
        if self.leader_size != Decimal::ZERO && price > Decimal::ZERO {
            self.desired_size = self.target_size(equity, price, self.leader_size);
        }
    }

    fn target_size(&self, equity: Decimal, price: Decimal, leader_size: Decimal) -> Decimal {
        let direction = if leader_size.is_sign_positive() {
            Direction::Long
        } else {
            Direction::Short
        };
        let fraction: Decimal = self
            .active
            .values()
            .filter(|value| value.direction == direction)
            .filter_map(|value| value.tier.map(|tier| POS_PCTS[tier]))
            .sum();
        let size = (equity * fraction / price)
            .round_dp_with_strategy(8, rust_decimal::RoundingStrategy::ToZero);
        if direction == Direction::Long {
            size
        } else {
            -size
        }
    }
}

fn tier_for_size(size: Decimal, current_tier: Option<usize>) -> Option<Option<usize>> {
    let next_tier = TIERS
        .iter()
        .enumerate()
        .rev()
        .find_map(|(index, threshold)| (size >= *threshold).then_some(index));
    (next_tier != current_tier).then_some(next_tier)
}

pub fn parse_config(path: &str) -> Result<Config> {
    let config: Config = toml::from_str(&std::fs::read_to_string(path)?)?;
    if config.leader.trim().is_empty() {
        bail!("leader is required")
    }
    if config.leverage == 0 {
        bail!("leverage must be positive")
    }
    Ok(config)
}

pub fn parse_network(value: &str) -> Result<qf::hyperliquid::HlNetwork> {
    match value {
        "mainnet" => Ok(qf::hyperliquid::HlNetwork::Mainnet),
        "testnet" => Ok(qf::hyperliquid::HlNetwork::Testnet),
        _ => bail!("network must be mainnet or testnet"),
    }
}

pub fn write_performance_report(
    ledger_path: &std::path::Path,
    report_path: &std::path::Path,
) -> Result<()> {
    if !ledger_path.exists() {
        return Ok(());
    }
    let report = PerformanceReport::from_events(JsonlLedgerReader::read_all(ledger_path)?);
    let temp_path = report_path.with_extension("md.tmp");
    std::fs::write(&temp_path, report.to_markdown())?;
    std::fs::rename(temp_path, report_path)?;
    Ok(())
}

pub async fn rebalance<B: HyperliquidBroker>(
    broker: &B,
    coin: &HlCoin,
    desired_size: Decimal,
    leverage: u32,
    max_slippage_bps: u32,
) -> Result<()> {
    let current = broker
        .position(coin)
        .await?
        .map(|p| p.size)
        .unwrap_or(Decimal::ZERO);
    if current == desired_size {
        return Ok(());
    }
    if current != Decimal::ZERO
        && desired_size != Decimal::ZERO
        && current.is_sign_positive() != desired_size.is_sign_positive()
    {
        broker
            .close_position(HlCloseRequest {
                coin: coin.clone(),
                size: HlCloseSize::Full,
                max_slippage_bps: Some(max_slippage_bps),
                client_order_id: None,
                expires_after: None,
            })
            .await
            .context("failed to close opposite follower position")?;
    }
    let now = broker
        .position(coin)
        .await?
        .map(|p| p.size)
        .unwrap_or(Decimal::ZERO);
    let delta = desired_size - now;
    if delta == Decimal::ZERO {
        return Ok(());
    }
    let reduce_only = now != Decimal::ZERO && delta.is_sign_positive() != now.is_sign_positive();
    broker
        .place_order(HlOrderRequest {
            coin: coin.clone(),
            side: if delta.is_sign_positive() {
                Side::Buy
            } else {
                Side::Sell
            },
            size: HlOrderSize::Exact(delta.abs()),
            leverage: Some(leverage),
            reduce_only,
            order_type: HlOrderType::Market {
                max_slippage_bps: Some(max_slippage_bps),
            },
            client_order_id: None,
            expires_after: None,
        })
        .await
        .context("failed to rebalance follower position")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use qf::hyperliquid::client::HlUserFill;
    use serde_json::Value;
    #[test]
    fn tiers_trigger_only_when_threshold_is_crossed() {
        assert_eq!(tier_for_size(Decimal::from(400), None), Some(Some(0)));
        assert_eq!(tier_for_size(Decimal::from(1_400), Some(0)), Some(Some(1)));
        assert_eq!(tier_for_size(Decimal::from(1_400), Some(1)), None);
    }

    #[test]
    fn duplicate_fill_does_not_increase_target_position() {
        let mut state = CopyState::default();
        state.leader_size = Decimal::from(400);
        let fill = HlUserFill {
            order_id: Some("1".to_string()),
            client_order_id: None,
            coin: Some(COIN.to_string()),
            size: Some(Decimal::from(400)),
            price: Some(Decimal::ONE),
            fee: None,
            side: Some(Side::Buy),
            direction: Some("Open Long".to_string()),
            timestamp: None,
            trade_id: Some("fill-1".to_string()),
            raw: Value::Null,
        };
        state.on_fill(&fill);
        state.refresh_target(Decimal::from(600), Decimal::from(100));
        assert_eq!(state.desired_size, Decimal::new(9, 1));
        state.on_fill(&fill);
        state.refresh_target(Decimal::from(600), Decimal::from(100));
        assert_eq!(state.desired_size, Decimal::new(9, 1));
    }

    #[test]
    fn leader_reduction_scales_follower_position() {
        let mut state = CopyState {
            desired_size: Decimal::from(10),
            ..CopyState::default()
        };
        state.leader_size = Decimal::from(1_000);
        state.on_snapshot(Decimal::from(600), Decimal::from(600), Decimal::from(100));
        assert_eq!(state.desired_size, Decimal::from(6));
    }
}
