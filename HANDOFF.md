# Handoff: qf::hyperliquid

## 当前目标

`qf::hyperliquid` 是 QuantForge 中的 Hyperliquid 专属基础设施模块。

核心原则：

- 不做跨交易所通用 Broker。
- `HyperliquidBroker` 是 live / paper / backtest 的统一 Hyperliquid 接口。
- 策略只依赖 broker，不直接接触 REST/WS client、signer 或 API key。
- 所有 broker 都应经过账户级 `RiskGuard`。
- `qf` 不是策略 runtime，不负责 event loop、timer、shutdown 和回测 replay。
- 不引入第三方 Hyperliquid SDK，协议和 transport 由项目自己实现。

预期结构：

```text
strategy
  -> dyn HyperliquidBroker
      -> HyperliquidLiveBroker
      -> HyperliquidPaperBroker（尚未实现）
      -> HyperliquidBacktestBroker
```

## Git 状态

仓库目前只有一个提交：

```text
b875934 feat(hyperliquid): 初始化交易基础能力
```

这个提交是仓库的 root commit，包含当前完整工程基础。

工作区有未提交改动：

```text
M crates/qf/src/hyperliquid/broker/backtest.rs
M crates/qf/src/hyperliquid/broker/risk_adapter.rs
M crates/qf/src/hyperliquid/types/order.rs
M HANDOFF.md
```

不要丢弃这些改动。

## 当前目录

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

## 已提交的基础能力

### Hyperliquid 下单类型

`crates/qf/src/hyperliquid/types/order.rs` 已包含：

- `HlAssetId`
- `HlTimeInForce`
- `HlTriggerKind`
- `HlOrderType`
- `HlOrderGrouping`
- `HlOrderRequest`
- `HlCancelRequest`
- `HlCancelTarget`
- `HlOrderAction`
- `HlCancelAction`
- `HlCancelByCloidAction`
- `HlExchangeAction`
- `HlSignature`
- `HlSignedAction`
- REST exchange payload 构造
- WS `method=post` payload 构造

支持生成 Hyperliquid wire JSON 字段：

```text
a / b / p / s / r / t / c
grouping
f（仅 true 时编码）
nonce / signature / vaultAddress / expiresAfter
```

### 响应类型

`crates/qf/src/hyperliquid/types/response.rs` 已包含：

- `HlOrderResponse`
- `HlOrderStatus::{Accepted, Resting, Filled, Error}`
- `HlCancelResponse`
- `HlCancelStatus::{Success, Error}`
- `raw: serde_json::Value`

### 当前 Broker trait

位置：`crates/qf/src/hyperliquid/broker/traits.rs`

当前仍然是同步接口：

```rust
pub trait HyperliquidBroker {
    fn account_state(&self) -> HlAccountState;

    fn position(&self, coin: &HlCoin) -> Option<HlPosition>;

    fn open_orders(&self, coin: &HlCoin) -> Vec<HlOpenOrder>;

    fn place_order(&mut self, request: HlOrderRequest)
        -> anyhow::Result<HlOrderResponse>;

    fn cancel_order(&mut self, request: HlCancelRequest)
        -> anyhow::Result<HlCancelResponse>;

    fn close_position(
        &mut self,
        coin: &HlCoin,
        options: HlCloseOptions,
    ) -> anyhow::Result<HlOrderResponse>;
}
```

接口文档已明确：

- 这是 Hyperliquid 专属统一接口。
- live / paper / backtest 都实现它。
- 状态 getter 读取 broker 本地快照，不主动访问远端。

## 未提交：简单回测 Broker

用户先要求实现简单回测 broker，之后又要求从 LiveBroker 实际需求调整公共接口。

当前工作区的回测实现已经完成，但尚未提交。

### 支持能力

`HyperliquidBacktestBroker` 当前支持：

- `set_mark_price(coin, price)` 设置当前 mark price。
- 只支持 market order。
- market order 按当前 mark price 立即成交。
- 多头/空头开仓。
- 同向加仓和加权 entry price。
- 部分减仓。
- 完全平仓。
- 反向开仓。
- reduce-only 方向检查。
- reduce-only 超量时截断到当前仓位，避免反向开仓。
- realized PnL。
- unrealized PnL。
- equity 重估。
- 基础 margin used 计算。
- `close_position` 构造反向 market reduce-only 订单并复用 `place_order`。

### 不支持能力

- limit order
- trigger / TP / SL
- open orders
- cancel 的实际效果
- 手续费
- 滑点
- funding
- liquidation
- fill history
- 回测报告和持久化

`cancel_order` 会返回明确的 not-supported 响应。

### Market 类型

未提交改动给 `HlOrderType` 增加了：

```rust
HlOrderType::Market
```

Hyperliquid 实盘没有独立的 market wire 类型，因此 `to_hyperliquid_json()` 暂时将其编码为 IOC limit 语义。真实 LiveBroker 实现时仍需根据 mark/mid 和最大滑点生成保护价格，不能发送 price=0。

### 风控适配

未提交改动新增：

```rust
order_risk_input_at_price(..., price, ...)
```

回测 market order 使用 mark price 计算 order notional，避免原实现因 `limit_price=None` 得到 0。

原 `order_risk_input` 仍保留，并委托给新函数。

### 测试状态

最近验证：

```bash
cargo fmt --all
cargo test --workspace
cargo check --workspace
```

结果：全部通过。

回测 broker 当前有 5 个测试：

- market 成交和仓位更新
- 加权 entry price、realized/unrealized PnL
- close position 和 equity
- reduce-only 禁止扩大仓位
- 拒绝非 market order

## LiveBroker 当前状态

位置：`crates/qf/src/hyperliquid/broker/live.rs`

当前仍是占位实现：

- `account_state` / `position` / `open_orders` 读取 `HlBrokerState`。
- `place_order` 会先经过 `RiskGuard`。
- 下单通过风控后只返回 `not_implemented`。
- `cancel_order` 返回 not implemented。
- `close_position` 返回 not implemented。

尚未实现：

- private key / API wallet signer
- nonce 管理
- action hash 和签名
- WS 长连接
- WS post request / response correlation
- REST exchange fallback
- REST info snapshot
- user WS subscriptions
- 本地状态 reconciliation
- audit
- timeout / retry / idempotency
- client order id 规则

## 已确认的 Hyperliquid API 事实

### 可以通过 WS 下单

Hyperliquid WebSocket 支持发送原本通过 HTTP API 提交的 signed action：

```json
{
  "method": "post",
  "id": 256,
  "request": {
    "type": "action",
    "payload": {
      "action": {},
      "nonce": 1713825891591,
      "signature": {
        "r": "...",
        "s": "...",
        "v": "..."
      }
    }
  }
}
```

响应通过 `channel=post` 和相同 `id` 关联。

推荐 live 交易路径：

```text
WS post action 为主
REST /exchange 为 fallback
```

### WS 订阅

账户状态需要关注：

- `clearinghouseState`
- `openOrders`
- `orderUpdates`
- `userEvents`
- `userFills`

行情/风控需要关注：

- `allMids`
- `activeAssetCtx`
- `bbo` 或 `l2Book`（后续需要时）

启动仍应使用 REST snapshot，运行中使用 WS 增量更新，并周期性 REST reconciliation。

### 签名和 nonce

- WS 下单不会绕过签名。
- order/cancel 属于 signed L1 action。
- msgpack 字段顺序、数字尾零、地址大小写都会影响签名。
- 推荐每个 trading process 使用独立 API wallet。
- nonce 应由 atomic counter 管理，并至少推进到当前毫秒时间。
- 同一 signer 对多个 subaccount/vault 的请求共享 nonce 空间。

## 最新讨论：从 LiveBroker 反推接口

以下是讨论结论，尚未实施。

### 当前接口的问题

1. 下单/撤单/平仓是同步方法，不能自然承载 WS/REST I/O。
2. 交易方法使用 `&mut self`，不利于 WS reader、状态任务和策略任务并发。
3. `anyhow::Result` 无法区分风控拒绝、交易所拒绝、transport 错误和结果未知。
4. `open_orders(coin)` 只提供按 coin 查询，但 live 风控需要账户级 open order 总表。
5. `position` 和按 coin 过滤 open orders 可以作为默认便利方法。

### 推荐的新接口

建议采用 `async-trait`，以支持 `Box<dyn HyperliquidBroker>`：

```rust
#[async_trait::async_trait]
pub trait HyperliquidBroker: Send + Sync {
    /// 返回当前本地账户快照，不访问远端。
    fn account_state(&self) -> HlAccountState;

    /// 返回账户级 open orders 本地快照。
    fn open_orders(&self) -> Vec<HlOpenOrder>;

    async fn place_order(
        &self,
        request: HlOrderRequest,
    ) -> Result<HlOrderResponse, HlBrokerError>;

    async fn cancel_order(
        &self,
        request: HlCancelRequest,
    ) -> Result<HlCancelResponse, HlBrokerError>;

    async fn close_position(
        &self,
        coin: &HlCoin,
        options: HlCloseOptions,
    ) -> Result<HlOrderResponse, HlBrokerError>;

    fn position(&self, coin: &HlCoin) -> Option<HlPosition> {
        self.account_state()
            .positions
            .into_iter()
            .find(|position| &position.coin == coin)
    }

    fn open_orders_for(&self, coin: &HlCoin) -> Vec<HlOpenOrder> {
        self.open_orders()
            .into_iter()
            .filter(|order| &order.coin == coin)
            .collect()
    }
}
```

### 为什么交易方法使用 `&self`

LiveBroker 预期内部结构：

```text
Arc<RwLock<HlBrokerState>>
atomic nonce
WS writer task/channel
WS reader task
pending request map keyed by request id
REST fallback client
```

交易方法在 await 期间不应独占整个 broker，因此使用 `&self` + 内部可变性。

BacktestBroker 需要迁移为内部 `Mutex`/`RwLock` 才能实现相同接口。

### 推荐错误类型

建议新增：

```rust
#[derive(Debug, thiserror::Error)]
pub enum HlBrokerError {
    #[error("risk rejected: {violations:?}")]
    RiskRejected {
        violations: Vec<RiskViolation>,
    },

    #[error("invalid request: {message}")]
    InvalidRequest {
        message: String,
    },

    #[error("exchange rejected: {message}")]
    ExchangeRejected {
        message: String,
        raw: serde_json::Value,
    },

    #[error("transport failed: {message}")]
    Transport {
        message: String,
    },

    #[error("request timed out; exchange outcome is unknown")]
    OutcomeUnknown,

    #[error("local broker state is unavailable or stale")]
    StateUnavailable,
}
```

`OutcomeUnknown` 很重要：请求可能已经发送成功，但在响应前断线。此时不能直接重试，否则可能重复下单；应使用 client order id 查询交易所状态。

### 保留 close_position

建议保留在统一 trait：

- Live：根据本地仓位构造带保护价格的 IOC reduce-only 订单。
- Backtest：按 mark price 立即成交。
- Paper：按模拟执行模型成交。

策略不应自己处理仓位方向、reduce-only、asset ID 和保护价格。

### 暂不加入 sync_state

延续之前决定，不把 `sync_state` 放入公共 trait。

LiveBroker 的连接过程应负责：

```text
REST 初始快照
建立 WS
启动后台状态任务
返回可用 broker handle
```

状态过旧时，交易方法应返回 `HlBrokerError::StateUnavailable`。

## 下一线程建议顺序

这是公共 API 变更，先确认接口方向，再实施。

### 1. 先处理当前未提交回测实现

建议先 review 当前 diff，然后提交简单回测 broker：

```bash
git diff
cargo fmt --all
cargo test --workspace
cargo check --workspace
```

提交 message 需使用中文 subject + body，遵守全局 `AGENTS.md`。

### 2. 调整 HyperliquidBroker trait

如果确认采用上述 live-driven 设计：

- workspace 增加 `async-trait`。
- 新增 `HlBrokerError`。
- trait 增加 `Send + Sync`。
- 交易方法改 async + `&self`。
- `open_orders()` 改账户级。
- `position()` / `open_orders_for()` 改默认方法。
- 不加 `sync_state`。
- 保留 `close_position`。

### 3. 迁移 BacktestBroker

- 使用内部锁保存可变状态和 PnL 账本。
- 适配 async trait。
- 保持当前简单 market 成交规则不变。
- 测试改为 async test。

### 4. 再实现 LiveBroker 基础

优先级：

1. nonce manager
2. signer
3. WS post request correlation
4. REST exchange fallback
5. place_order
6. cancel_order
7. close_position
8. REST snapshot + WS 状态更新
9. audit

## 当前风险点

- LiveBroker 仍然完全不能真实下单。
- signer 只有 wallet address，占位不可用。
- REST/WS client 只有 base URL，占位不可用。
- 当前公共 trait 还是同步 `&mut self`，尚未适配 live 并发模型。
- 当前 `HlOrderRequest` 同时包含 broker 语义和 wire 相关字段，后续可能需要进一步分层，但不要现在过早重构。
- `HlOrderType::Market` 的 wire 价格保护尚未实现，不能直接用于 live 下单。
- RiskAdapter 的 post-trade notional/leverage 仍是粗糙占位。
- Audit 尚未接入 broker 调用链。
- 未提交回测实现不含手续费、滑点、funding 和 liquidation。
