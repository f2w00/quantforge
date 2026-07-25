# Handoff: qf::hyperliquid

## 当前定位

`qf::hyperliquid` 是 QuantForge 库里的 Hyperliquid 专属基础设施模块。

它不是跨交易所抽象层，也不是策略运行时。当前设计目标是：

- 暴露 Hyperliquid 原生语义，避免过早做通用 Broker。
- 给策略提供可复用的 Hyperliquid Broker。
- 在 Broker 内部强制接入账户级 `RiskGuard`。
- 后续接入 `audit`，记录原始请求、风控结果、原始响应。
- 支持 live / paper / backtest 三种 broker 形态，但第一版先保留接口骨架。

策略未来可以直接使用 `qf::hyperliquid::HyperliquidBroker`，也可以包一层策略专属 broker。

示例：

```text
strategy
  -> StrategySpecificBroker 可选
  -> qf::hyperliquid::HyperliquidBroker
  -> qf::risk::RiskGuard
  -> Hyperliquid live/paper/backtest backend
```

## 当前文件结构

```text
crates/qf/src/hyperliquid/
  mod.rs

  types/
    mod.rs
    account.rs
    market.rs
    order.rs
    position.rs
    response.rs

  client/
    mod.rs
    rest.rs
    ws.rs
    signer.rs

  broker/
    mod.rs
    traits.rs
    state.rs
    risk_adapter.rs
    live.rs
    backtest.rs
```

## 对外导出

当前 `qf::hyperliquid` 导出：

```rust
pub use broker::{
    HyperliquidBacktestBroker,
    HyperliquidBroker,
    HyperliquidLiveBroker,
};
```

Hyperliquid 类型通过：

```rust
qf::hyperliquid::types::*
```

访问。

## 已有核心接口

### HyperliquidBroker

位置：`crates/qf/src/hyperliquid/broker/traits.rs`

```rust
pub trait HyperliquidBroker {
    fn account_state(&self) -> HlAccountState;

    fn position(&self, coin: &HlCoin) -> Option<HlPosition>;

    fn open_orders(&self, coin: &HlCoin) -> Vec<HlOpenOrder>;

    fn place_order(&mut self, request: HlOrderRequest) -> anyhow::Result<HlOrderResponse>;

    fn cancel_order(&mut self, request: HlCancelRequest) -> anyhow::Result<HlCancelResponse>;

    fn close_position(
        &mut self,
        coin: &HlCoin,
        options: HlCloseOptions,
    ) -> anyhow::Result<HlOrderResponse>;
}
```

这个 trait 是 Hyperliquid 专属，不要求兼容其他交易所。

### HlBrokerState

位置：`crates/qf/src/hyperliquid/broker/state.rs`

当前保存：

- `HlAccountState`
- open orders

并提供：

- `position(&HlCoin)`
- `open_orders(&HlCoin)`

后续 live broker 应该通过 exchange reconciliation 更新这个状态。

### Risk Adapter

位置：`crates/qf/src/hyperliquid/broker/risk_adapter.rs`

当前函数：

```rust
pub fn order_risk_input(
    strategy_id: StrategyId,
    account: &HlAccountState,
    request: &HlOrderRequest,
    open_order_count: usize,
) -> RiskCheckInput
```

它负责把 Hyperliquid 原生订单请求转换成通用风控输入。

当前实现很粗糙：

- `order_notional = limit_price.unwrap_or(0) * abs(size)`
- `post_trade_notional = order_notional`
- `post_trade_leverage = 0`

后续必须基于当前 mark price、现有仓位、账户 equity、订单方向重算。

## 当前 Broker 状态

### HyperliquidLiveBroker

位置：`crates/qf/src/hyperliquid/broker/live.rs`

当前行为：

- `account_state` / `position` / `open_orders` 读取本地 `HlBrokerState`
- `place_order` 会先经过 `RiskGuard`
- 风控通过后返回 `not_implemented` 占位响应
- `cancel_order` / `close_position` 也是占位响应

尚未实现：

- REST 下单
- REST 撤单
- close position 的订单构造
- user state sync
- websocket user event 更新
- 审计记录
- rate limit
- retry / idempotency
- client order id 规则

### HyperliquidBacktestBroker

位置：`crates/qf/src/hyperliquid/broker/backtest.rs`

当前行为：

- 使用本地 `HlBrokerState`
- `place_order` 会先经过 `RiskGuard`
- 风控通过后生成 `bt-<n>` 占位订单 ID
- 不更新仓位
- 不生成成交
- 不处理手续费、滑点、资金费率

尚未实现：

- 模拟订单簿或简化成交模型
- market / limit / trigger 订单处理
- open order 状态更新
- fill 生成
- position / equity / margin 更新
- funding 结算
- liquidation 检查

## 重要设计约束

### 不做通用跨交易所 Broker

不要把 `HyperliquidBroker` 抽成类似：

```rust
trait Broker {
    fn place_order(...);
}
```

原因：Hyperliquid 的返回、订单语义、账户状态、风控字段都有交易所特性。

当前原则是：

```text
不统一交易接口，只统一安全边界。
```

安全边界包括：

- `qf::risk::RiskGuard`
- `qf::audit::AuditEvent`
- `qf::storage::JsonlWriter`

### 策略不直接拿 API key

策略可以拿 `HyperliquidBroker`，但不应该直接拿 signer 或裸 REST client。

真实执行路径应该保持：

```text
strategy
  -> broker.place_order(...)
  -> risk_guard.check(...)
  -> audit record attempt
  -> rest client submit
  -> state update / reconcile
  -> audit record result
```

### qf 不是 runtime

当前没有 `qf-runtime`。

策略自己实现：

- event loop
- backtest replay
- live subscriptions
- timer
- shutdown
- metrics

`qf::hyperliquid` 只提供 Hyperliquid 能力，不规定策略怎么跑。

## 下一步建议顺序

### 1. 完善 Hyperliquid 原生类型

优先补齐：

- order request / response 的真实字段
- cancel request / response
- user state / clearinghouse state
- asset position
- open order
- fill
- funding
- mark / oracle price
- websocket event enum

要求：保留 `raw: serde_json::Value`，避免丢失交易所原始信息。

### 2. 实现 REST client 骨架

优先接口：

- info endpoint
- exchange endpoint
- user state
- open orders
- place order
- cancel order

先不要急着优化签名和错误分类，先让接口边界跑通。

### 3. 实现 signer

`HyperliquidSigner` 当前只有 `wallet_address`。

后续需要补：

- private key 管理
- request signing
- nonce
- action hash
- vault / subaccount 可选字段

注意不要把 secret 打进 audit log。

### 4. 接入 AuditRecorder

Broker 每个交易动作至少记录两类事件：

```text
attempt:
  raw_request
  risk_decision

result:
  raw_response 或 error
```

`AuditEvent.raw_request` 和 `raw_response` 用 JSON 保存交易所原生结构。

### 5. 完善 Risk Adapter

当前 risk adapter 只是占位。

应补充：

- market order 的 notional 估算
- post-trade position
- post-trade notional
- post-trade leverage
- reduce-only 判断
- open order count
- liquidation buffer 后续可加

### 6. 实现 LiveBroker 真实调用

顺序建议：

1. `sync_state`
2. `place_order`
3. `cancel_order`
4. `close_position`
5. user websocket event 更新本地 state
6. 周期性 reconciliation

### 7. 实现 BacktestBroker 简化成交

第一版建议只做保守简化：

- market order：用当前 mark price 加滑点成交
- limit order：价格触达才成交
- reduce-only：禁止扩大仓位
- fee：固定 maker/taker fee
- funding：后置

先不要实现复杂盘口队列。

## 第一个策略命名建议

用户计划的第一个策略是：观察其他账户持仓，如果目标账户重仓则跟随进场。

推荐策略名：

```text
hl-whale-follow
```

备选：

- `hl-position-copy`
- `hl-smart-money-follow`
- `hl-account-follow`

我建议用 `hl-whale-follow`，因为它表达明确且不限定必须完全复制交易。

当前不要实现策略，只保留这个命名决策。

## 当前验证状态

最近一次验证：

```bash
cargo fmt --all
cargo check --workspace
```

结果：通过。

## 当前风险点

- LiveBroker 未真实下单，所有 live 交易方法仍是占位。
- BacktestBroker 不会更新仓位或生成成交。
- RiskAdapter 的 notional / leverage 计算不可信，只是接口占位。
- Audit 尚未接入 broker 调用链。
- Hyperliquid REST/WS/signer 尚未实现真实协议。
