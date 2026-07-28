use qf::core::{Decimal, Side};
use qf::hyperliquid::HyperliquidBroker;
use qf::hyperliquid::types::{
    HlCloseRequest, HlCloseSize, HlCoin, HlOrderRequest, HlOrderSize, HlOrderType,
};

/// 最小策略：空仓开多，固定持有若干有效价格事件后平仓并循环。
pub struct ExampleStrategy {
    coin: HlCoin,
    entry_size: Decimal,
    max_slippage_bps: u32,
    hold_ticks: u32,
    held_ticks: u32,
}

impl ExampleStrategy {
    pub fn new(coin: HlCoin, entry_size: Decimal, max_slippage_bps: u32) -> Self {
        Self::with_hold_ticks(coin, entry_size, max_slippage_bps, 3)
    }

    pub fn with_hold_ticks(
        coin: HlCoin,
        entry_size: Decimal,
        max_slippage_bps: u32,
        hold_ticks: u32,
    ) -> Self {
        Self {
            coin,
            entry_size,
            max_slippage_bps,
            hold_ticks: hold_ticks.max(1),
            held_ticks: 0,
        }
    }

    pub fn coin(&self) -> &HlCoin {
        &self.coin
    }

    pub async fn on_price(
        &mut self,
        broker: &dyn HyperliquidBroker,
        price: Decimal,
    ) -> anyhow::Result<()> {
        if price <= Decimal::ZERO {
            return Ok(());
        }

        let position = broker.position(&self.coin);
        if position.is_some_and(|position| position.size != Decimal::ZERO) {
            self.held_ticks += 1;
            if self.held_ticks >= self.hold_ticks {
                broker
                    .close_position(HlCloseRequest {
                        coin: self.coin.clone(),
                        size: HlCloseSize::Full,
                        max_slippage_bps: Some(self.max_slippage_bps),
                        client_order_id: None,
                        expires_after: None,
                    })
                    .await?;
                self.held_ticks = 0;
            }
            return Ok(());
        }

        broker
            .place_order(HlOrderRequest {
                coin: self.coin.clone(),
                side: Side::Buy,
                size: HlOrderSize::Exact(self.entry_size),
                reduce_only: false,
                order_type: HlOrderType::Market {
                    max_slippage_bps: Some(self.max_slippage_bps),
                },
                client_order_id: None,
                expires_after: None,
            })
            .await?;
        self.held_ticks = 0;
        Ok(())
    }
}
