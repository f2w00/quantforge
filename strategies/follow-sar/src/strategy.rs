use qf::core::{Decimal, Side};
use qf::hyperliquid::HyperliquidBroker;
use qf::hyperliquid::types::{
    HlCloseRequest, HlCloseSize, HlCoin, HlOrderRequest, HlOrderSize, HlOrderType, HlPosition,
};

use crate::{Candle, SarIndicator, SarTrend};

const ENTRY_POSITION_SIZE: Decimal = Decimal::from_parts(1215, 0, 0, false, 3);
const ENTRY_THRESHOLD: Decimal = Decimal::from_parts(31, 0, 0, false, 0);
const EMERGENCY_THRESHOLD: Decimal = Decimal::from_parts(3, 0, 0, false, 0);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SarProfile {
    Scalp,
    Intraday,
    Swing,
    Position,
}

impl SarProfile {
    fn from_position_size(size: Decimal) -> Self {
        match size {
            size if size < Decimal::from(35) => Self::Scalp,
            size if size < Decimal::from(40) => Self::Intraday,
            size if size <= Decimal::from(50) => Self::Swing,
            _ => Self::Position,
        }
    }

    pub fn parameters(self) -> (f64, f64) {
        match self {
            Self::Scalp => (0.04, 0.4),
            Self::Intraday => (0.03, 0.3),
            Self::Swing => (0.02, 0.2),
            Self::Position => (0.01, 0.1),
        }
    }
}

#[derive(Clone, Debug)]
pub enum FollowSarEvent {
    TargetPosition(Option<HlPosition>),
    BarClose(Candle),
}

pub struct FollowSarStrategy {
    coin: HlCoin,
    max_slippage_bps: u32,
    sar: Option<SarIndicator>,
    active_profile: Option<SarProfile>,
    target_above_threshold: bool,
}

impl FollowSarStrategy {
    pub fn new(coin: HlCoin, max_slippage_bps: u32) -> Self {
        Self {
            coin,
            max_slippage_bps,
            sar: None,
            active_profile: None,
            target_above_threshold: false,
        }
    }

    pub fn coin(&self) -> &HlCoin {
        &self.coin
    }

    pub fn active_profile(&self) -> Option<SarProfile> {
        self.active_profile
    }

    pub async fn on_event(
        &mut self,
        broker: &dyn HyperliquidBroker,
        event: FollowSarEvent,
    ) -> anyhow::Result<()> {
        match event {
            FollowSarEvent::TargetPosition(position) => {
                self.on_target_position(broker, position.as_ref()).await
            }
            FollowSarEvent::BarClose(candle) => self.on_bar_close(broker, &candle).await,
        }
    }

    async fn on_target_position(
        &mut self,
        broker: &dyn HyperliquidBroker,
        target: Option<&HlPosition>,
    ) -> anyhow::Result<()> {
        let target_size = target
            .filter(|position| position.coin == self.coin)
            .map(|position| position.size)
            .unwrap_or(Decimal::ZERO);
        let above_threshold = target_size.abs() > ENTRY_THRESHOLD;
        let emergency = target_size.abs() < EMERGENCY_THRESHOLD;
        let own_position = broker
            .position(&self.coin)
            .filter(|position| position.size != Decimal::ZERO);
        if emergency {
            if own_position.is_some() {
                self.close(broker).await?;
            } else {
                self.sar = None;
                self.active_profile = None;
            }
            self.target_above_threshold = false;
            return Ok(());
        }
        if !above_threshold && own_position.is_none() {
            self.sar = None;
            self.active_profile = None;
        }
        let crossed_up = above_threshold && !self.target_above_threshold;
        self.target_above_threshold = above_threshold;
        if !crossed_up || own_position.is_some() {
            return Ok(());
        }

        let target = target.expect("crossed threshold requires a target position");
        let profile = SarProfile::from_position_size(target.size.abs());
        let (step, maximum) = profile.parameters();
        let sar = SarIndicator::new(step, maximum)?;
        let side = if target.size.is_sign_positive() {
            Side::Buy
        } else if target.size.is_sign_negative() {
            Side::Sell
        } else {
            return Ok(());
        };
        if let Err(error) = broker
            .place_order(HlOrderRequest {
                coin: self.coin.clone(),
                side,
                size: HlOrderSize::Exact(ENTRY_POSITION_SIZE),
                leverage: Some(1),
                reduce_only: false,
                order_type: HlOrderType::Market {
                    max_slippage_bps: Some(self.max_slippage_bps),
                },
                client_order_id: None,
                expires_after: None,
            })
            .await
        {
            self.target_above_threshold = false;
            return Err(error.into());
        }
        self.sar = Some(sar);
        self.active_profile = Some(profile);
        Ok(())
    }

    async fn on_bar_close(
        &mut self,
        broker: &dyn HyperliquidBroker,
        candle: &Candle,
    ) -> anyhow::Result<()> {
        let Some(sar_indicator) = &mut self.sar else {
            return Ok(());
        };
        let sar = sar_indicator.next(candle)?;
        let Some(position) = broker
            .position(&self.coin)
            .filter(|position| position.size != Decimal::ZERO)
        else {
            return Ok(());
        };
        let should_close = match sar.reversal {
            Some(SarTrend::Falling) => position.size.is_sign_positive(),
            Some(SarTrend::Rising) => position.size.is_sign_negative(),
            None => false,
        };
        if should_close {
            self.close(broker).await?;
        }
        Ok(())
    }

    async fn close(&mut self, broker: &dyn HyperliquidBroker) -> anyhow::Result<()> {
        broker
            .close_position(HlCloseRequest {
                coin: self.coin.clone(),
                size: HlCloseSize::Full,
                max_slippage_bps: Some(self.max_slippage_bps),
                client_order_id: None,
                expires_after: None,
            })
            .await?;
        self.sar = None;
        self.active_profile = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use chrono::Utc;
    use qf::audit::RunJournal;
    use qf::core::{JournalId, StrategyId};
    use qf::hyperliquid::broker::state::HlBrokerState;
    use qf::hyperliquid::types::HlAccountState;
    use qf::hyperliquid::{HyperliquidBacktestBroker, HyperliquidBroker};
    use qf::risk::{RiskGuard, RiskLimits};
    use qf::storage::MemoryLedgerSink;

    use super::*;

    fn broker() -> HyperliquidBacktestBroker {
        HyperliquidBacktestBroker::new(
            StrategyId::new("follow-sar-test"),
            HlBrokerState {
                account: HlAccountState {
                    equity: Decimal::from(10_000),
                    margin_used: Decimal::ZERO,
                    positions: HashMap::new(),
                    updated_at: Utc::now(),
                },
                open_orders: Vec::new(),
            },
            RiskGuard::new(RiskLimits::default()),
            Arc::new(RunJournal::new(
                JournalId::new("follow-sar-test"),
                MemoryLedgerSink::new(),
            )),
        )
    }

    fn target(coin: &HlCoin, size: &str, notional: &str) -> HlPosition {
        HlPosition {
            coin: coin.clone(),
            size: size.parse().unwrap(),
            entry_price: Some(Decimal::from(100)),
            notional: notional.parse().unwrap(),
            unrealized_pnl: Decimal::ZERO,
            return_on_equity: Decimal::ZERO,
            leverage: Decimal::ONE,
            liquidation_price: None,
        }
    }

    fn candle(index: i64, open: i64, high: i64, low: i64, close: i64) -> Candle {
        Candle {
            opened_at: index,
            open: open as f64,
            high: high as f64,
            low: low as f64,
            close: close as f64,
            volume: 1.0,
        }
    }

    #[tokio::test]
    async fn opens_once_when_target_crosses_position_threshold() {
        let coin = HlCoin::new("ETH");
        let broker = broker();
        broker
            .set_mark_price(coin.clone(), Decimal::from(100))
            .unwrap();
        let mut strategy = FollowSarStrategy::new(coin.clone(), 100);

        strategy
            .on_event(
                &broker,
                FollowSarEvent::TargetPosition(Some(target(&coin, "32", "3200"))),
            )
            .await
            .unwrap();
        strategy
            .on_event(
                &broker,
                FollowSarEvent::TargetPosition(Some(target(&coin, "32", "3200"))),
            )
            .await
            .unwrap();

        assert_eq!(
            broker.position(&coin).unwrap().size,
            "1.215".parse().unwrap()
        );
        assert_eq!(strategy.active_profile(), Some(SarProfile::Scalp));

        strategy
            .on_event(
                &broker,
                FollowSarEvent::TargetPosition(Some(target(&coin, "45", "4500"))),
            )
            .await
            .unwrap();
        assert_eq!(strategy.active_profile(), Some(SarProfile::Scalp));
    }

    #[tokio::test]
    async fn does_not_open_below_threshold_or_reenter_until_new_crossing() {
        let coin = HlCoin::new("ETH");
        let broker = broker();
        broker
            .set_mark_price(coin.clone(), Decimal::from(100))
            .unwrap();
        let mut strategy = FollowSarStrategy::new(coin.clone(), 100);

        strategy
            .on_event(
                &broker,
                FollowSarEvent::TargetPosition(Some(target(&coin, "31", "3100"))),
            )
            .await
            .unwrap();
        assert!(broker.position(&coin).is_none());

        strategy
            .on_event(
                &broker,
                FollowSarEvent::TargetPosition(Some(target(&coin, "32", "3200"))),
            )
            .await
            .unwrap();
        strategy
            .on_event(
                &broker,
                FollowSarEvent::BarClose(candle(0, 100, 105, 99, 104)),
            )
            .await
            .unwrap();
        strategy
            .on_event(
                &broker,
                FollowSarEvent::BarClose(candle(1, 104, 110, 103, 109)),
            )
            .await
            .unwrap();
        strategy
            .on_event(
                &broker,
                FollowSarEvent::BarClose(candle(2, 109, 111, 108, 110)),
            )
            .await
            .unwrap();
        strategy
            .on_event(
                &broker,
                FollowSarEvent::BarClose(candle(3, 110, 110, 90, 92)),
            )
            .await
            .unwrap();
        assert!(broker.position(&coin).is_none());

        strategy
            .on_event(&broker, FollowSarEvent::TargetPosition(None))
            .await
            .unwrap();
        strategy
            .on_event(
                &broker,
                FollowSarEvent::TargetPosition(Some(target(&coin, "32", "3200"))),
            )
            .await
            .unwrap();
        assert_eq!(
            broker.position(&coin).unwrap().size,
            "1.215".parse().unwrap()
        );
    }

    #[tokio::test]
    async fn closes_immediately_when_target_position_is_nearly_flat() {
        let coin = HlCoin::new("ETH");
        let broker = broker();
        broker
            .set_mark_price(coin.clone(), Decimal::from(100))
            .unwrap();
        let mut strategy = FollowSarStrategy::new(coin.clone(), 100);
        strategy
            .on_event(
                &broker,
                FollowSarEvent::TargetPosition(Some(target(&coin, "36", "3600"))),
            )
            .await
            .unwrap();

        strategy
            .on_event(
                &broker,
                FollowSarEvent::TargetPosition(Some(target(&coin, "2.9", "290"))),
            )
            .await
            .unwrap();

        assert!(broker.position(&coin).is_none());
        assert!(strategy.sar.is_none());
        assert_eq!(strategy.active_profile(), None);
        assert!(!strategy.target_above_threshold);
    }

    #[test]
    fn selects_sar_profile_from_entry_position_size() {
        assert_eq!(
            SarProfile::from_position_size("32".parse().unwrap()),
            SarProfile::Scalp
        );
        assert_eq!(
            SarProfile::from_position_size("35".parse().unwrap()),
            SarProfile::Intraday
        );
        assert_eq!(
            SarProfile::from_position_size("40".parse().unwrap()),
            SarProfile::Swing
        );
        assert_eq!(
            SarProfile::from_position_size("51".parse().unwrap()),
            SarProfile::Position
        );
    }
}
