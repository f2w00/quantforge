# Hyperliquid Broker 设计

本文说明 QuantForge 当前对 `qf::hyperliquid` Broker 的设计目标、接口语义和
LiveBroker 的实现边界。

## 设计目标

`qf::hyperliquid` 是 Hyperliquid 专属基础设施模块，不抽象成跨交易所通用
Broker。

核心原则：

- `HyperliquidBroker` 是 live、paper、backtest 的统一 Hyperliquid 接口。
- 策略只依赖 Broker，不直接接触 REST/WS client、signer 或 API key。
- 所有 Broker 的交易请求都经过账户级 `RiskGuard`。
- `qf` 不负责策略 runtime 的 event loop、timer、shutdown 或回测 replay。
- 不引入第三方 Hyperliquid SDK；Hyperliquid 协议和 transport 由项目自己实现。

目标结构：

```text
strategy
  -> dyn HyperliquidBroker
      -> HyperliquidLiveBroker
      -> HyperliquidPaperBroker
      -> HyperliquidBacktestBroker
```

## Broker 接口

当前公共接口采用 `async-trait`，并要求实现满足 `Send + Sync`：

```rust
pub trait HyperliquidBroker: Send + Sync {
    fn account_state(&self) -> HlAccountState;
    fn open_orders(&self) -> Vec<HlOpenOrder>;

    async fn place_order(
        &self,
        request: HlOrderRequest,
    ) -> Result<HlOrderResult, HlBrokerError>;

    async fn cancel_order(
        &self,
        request: HlCancelRequest,
    ) -> Result<HlCancelResponse, HlBrokerError>;

    async fn close_position(
        &self,
        request: HlCloseRequest,
    ) -> Result<HlOrderResult, HlBrokerError>;
}
```

交易方法使用 `&self`，Broker 通过内部锁、原子计数器和后台任务管理可变状态。
这样策略任务、WS reader 和 WS writer 不需要竞争整个 Broker 的独占借用。

`position(coin)` 和 `open_orders_for(coin)` 是基于账户级本地快照的便利方法，
不会主动访问远端。

公共接口暂不加入 `sync_state`。LiveBroker 的初始化和连接管理负责初始同步，
运行中的状态同步由其内部后台任务完成。

## 订单请求

`HlOrderRequest` 表达策略的订单意图，不直接暴露 Hyperliquid wire 或 envelope
字段：

```rust
pub struct HlOrderRequest {
    pub coin: HlCoin,
    pub side: Side,
    pub size: Decimal,
    pub reduce_only: bool,
    pub order_type: HlOrderType,
    pub client_order_id: Option<HlClientOrderId>,
    pub expires_after: Option<Timestamp>,
}
```

以下信息由 LiveBroker 根据 metadata 或运行时配置补全，不由策略传入：

- asset ID
- nonce
- signature
- vault address
- WS request ID
- REST/WS transport 选择
- 价格和数量的 Hyperliquid 精度处理

订单类型如下：

```rust
pub enum HlOrderType {
    Market {
        max_slippage_bps: u32,
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
```

Hyperliquid 没有独立的 market wire order。Market order 由 Broker 根据实时参考
价格计算保护价格，然后编码为 IOC limit order。

`HlTriggerExecution` 区分 trigger market 和 trigger limit：

```rust
pub enum HlTriggerExecution {
    Market {
        max_slippage_bps: u32,
    },
    Limit {
        limit_price: Decimal,
    },
}
```

订单请求会在进入风控前校验：

- size 必须大于零；
- limit price 和 trigger price 必须大于零；
- 最大滑点必须小于 10000 bps；
- TP/SL trigger 必须是 reduce-only。

## Client Order ID

`HlClientOrderId` 表示 Hyperliquid 的 128-bit cloid，格式为：

```text
0x + 32 个十六进制字符
```

LiveBroker 在调用方未提供 cloid 时自动生成。每个 live order 都应该有 cloid，
因为 WS 请求可能已经发送，但响应可能在断线前没有返回。此时可以通过 cloid
查询订单状态，而不会因为盲目重试导致重复下单。

## 平仓接口

Hyperliquid 没有独立的 `closePosition` action。平仓实际是一个反向的、
`reduceOnly=true` 的 IOC order。

当前使用完整的平仓请求：

```rust
pub struct HlCloseRequest {
    pub coin: HlCoin,
    pub size: HlCloseSize,
    pub max_slippage_bps: u32,
    pub client_order_id: Option<HlClientOrderId>,
    pub expires_after: Option<Timestamp>,
}

pub enum HlCloseSize {
    Full,
    Exact(Decimal),
}
```

`Full` 使用 Broker 本地仓位快照中的绝对仓位数量，`Exact` 只请求减少指定数量。

`close_position` 的返回不承诺调用结束时仓位已经归零，原因包括：

- 本地仓位是一个时间点的快照；
- IOC 可能部分成交；
- 保护价格范围内可能没有足够流动性；
- 请求响应可能丢失。

调用方需要结合返回的成交数量和后续账户状态判断是否完全平仓。

## 返回值和错误

单订单的 `place_order` 和 `close_position` 使用同一个结果类型：

```rust
pub struct HlOrderResult {
    pub submitted: HlSubmittedOrder,
    pub outcome: HlOrderOutcome,
    pub raw: serde_json::Value,
}
```

`submitted` 记录 Broker 实际提交的 coin、方向、数量、保护价格、reduce-only
和 cloid。`outcome` 只表达交易所明确返回的成功结果：

```rust
pub enum HlOrderOutcome {
    Resting {
        order_id: OrderId,
    },
    Filled {
        order_id: OrderId,
        total_size: Decimal,
        avg_price: Decimal,
    },
}
```

Hyperliquid 外层 `status=ok` 只表示 action 被处理，不能直接视为订单成功。必须
继续解析 `statuses` 中的 `resting`、`filled` 或 `error`。

错误分类如下：

| 情况 | 错误 |
| --- | --- |
| 风控拒绝 | `RiskRejected` |
| 请求参数非法 | `InvalidRequest` |
| 交易所明确拒绝 | `ExchangeRejected` |
| transport 明确失败 | `Transport` |
| 请求已发送但结果未知 | `OutcomeUnknown` |
| 本地状态不可用或过旧 | `StateUnavailable` |
| 平仓时没有目标仓位 | `PositionUnavailable` |

`OutcomeUnknown` 必须携带 cloid。出现该错误时，应先通过 order status 查询确认，
不能直接重试相同订单。

## LiveBroker 连接模型

LiveBroker 计划维护一条 Hyperliquid WS 长连接，同时承载交易请求和订阅：

```text
一条 WS 连接
├── post action：下单、撤单
├── allMids：实时市场参考价格
├── clearinghouseState：账户和仓位
├── openOrders：挂单快照
├── orderUpdates：订单状态更新
├── userFills：成交更新
└── userEvents：用户事件
```

同一条连接上的消息由 reader task 按 channel 分发：

```text
channel=post
  -> 根据 request id 唤醒等待中的交易请求

channel=allMids
  -> 更新本地市场价格

账户相关 channel
  -> 更新本地账户、仓位、挂单和成交状态
```

所有 WS 写操作经过单独的 writer task。交易调用只向 writer 发送请求，并通过
`request_id -> oneshot` 等待对应的 post response，不能让多个调用方直接并发写
同一个 WebSocket sink。

计划中的内部组件：

```text
HyperliquidLiveBroker
├── Arc<RwLock<HlBrokerState>>
├── Arc<RwLock<HlMarketState>>
├── Arc<RwLock<HlMetadataSnapshot>>
├── HyperliquidSigner
├── account-level nonce manager
├── WS writer channel
├── pending request map
├── REST client
└── RiskGuard
```

WS 断线时：

- 尚未写入 WS 的请求可以安全地走 REST fallback；
- 已经写入但响应未知的请求返回 `OutcomeUnknown`；
- 不允许直接使用 REST 重发相同订单；
- 重连后重新订阅并执行 REST reconciliation。

## allMids

`allMids` 是一个 WS 订阅频道，不是单个币种的价格。一次消息包含多个市场的
参考 mid price：

```json
{
  "channel": "allMids",
  "data": {
    "mids": {
      "BTC": "118500.0",
      "ETH": "3650.25"
    }
  }
}
```

LiveBroker 维护：

```text
HlCoin -> 最新 mid price -> 更新时间
```

Market/Close 请求只读取目标 coin 的价格。当前设计选择 `allMids`，而不是为每个
币种单独订阅 `bbo`，原因是连接和订阅管理更简单，而且 `allMids` 数据量相对较小。

`allMids` 只提供参考价格，不是：

- best bid/ask；
- mark price；
- oracle price；
- 成交保证。

Market/Close 的保护价格由 Broker 结合方向和 `max_slippage_bps` 计算，并在发送
前进行价格精度处理。

## Metadata

实时 `allMids` 与静态交易规则必须分开维护。Metadata 通过 REST `/info` 的 `meta`
请求获取，至少包含：

```rust
pub struct HlAssetMeta {
    pub coin: HlCoin,
    pub asset_id: HlAssetId,
    pub size_decimals: u32,
    pub max_leverage: u32,
    pub only_isolated: bool,
}
```

Metadata 用于：

- `coin -> asset ID` 解析；
- size 精度和数量步长；
- price 精度规则；
- leverage 和 isolated trading 约束；
- 订单和撤单的 wire 字段构造。

`size_decimals` 表示数量精度。例如 `size_decimals=3` 时，数量步长为 `0.001`。
它不等于最低订单名义金额。最低订单金额是另一个交易所规则，通常通过订单
预检查和交易所返回的错误共同处理。

Hyperliquid 的价格规则包括：

- 最多 5 位有效数字；
- perp 价格小数位不超过 `6 - szDecimals`；
- size 小数位不超过 `szDecimals`；
- 签名前去除数字尾零。

Metadata 不只在启动时加载一次。计划中的更新策略：

```text
首次连接：REST meta 成功后才允许 Broker ready
定时刷新：默认每 5 分钟
WS 重连：连接恢复后立即刷新
刷新失败：保留最后成功快照并记录失败
最大年龄：默认 30 分钟，过期后禁止 place/close
未知 allMids coin：触发一次受限的提前刷新
```

每次刷新先构造临时完整快照，校验完成后整体替换，避免下单读到半更新的 metadata。

新增币种可以进入可交易集合；下架币种禁止新下单，但可以保留旧 metadata 用于已有
订单的撤单、延迟 WS 消息解析和 reconciliation。

## 杠杆与数量

Hyperliquid 的杠杆是按 perp asset 设置，而不是账户全局设置。LiveBroker 的目标市场
配置在 `connect()` 阶段发送 `updateLeverage` 并等待明确成功；任一配置失败时 Broker
不会进入 ready 状态。当前实现仅支持 Cross，isolated 因需要独立保证金管理而明确拒绝。

`calculate_order_size()` 使用以下保守计算：

```text
available_margin = max(equity - margin_used - reserve_margin, 0)
margin = available_margin * margin_fraction
notional = margin * leverage
size = floor(notional / reference_price, size_decimals)
```

该 helper 只产生建议数量。下单时 Broker 仍会把现有仓位、非 reduce-only 挂单和
并发中的 pending order notional 纳入 post-trade 风控。

## 初始化和状态同步

LiveBroker 的连接初始化计划为：

```text
1. REST 获取 meta
2. REST 获取 allMids 初始快照
3. REST 获取账户状态和 open orders
4. 建立 WS 长连接
5. 订阅 allMids 和账户相关频道
6. 确认必要状态可用
7. 返回 ready 的 Broker
```

运行中由 WS 增量更新本地状态，并周期性使用 REST reconciliation 修复消息丢失、
重连窗口和本地状态漂移。

当前的 `HlBrokerState` 仍是基础账户和挂单快照；LiveBroker 后续需要增加状态时间戳、
行情快照、metadata 快照、连接状态和 pending request 管理。

## 当前实现状态

已完成：

- async `HyperliquidBroker` 公共接口；
- `Send + Sync` Broker 约束；
- 账户级 open orders getter；
- 强类型订单请求；
- Market 的滑点参数；
- Full/Exact 平仓请求；
- 强类型 cloid 和自动生成 cloid；
- 结构化订单结果和 Broker 错误；
- BacktestBroker 的异步接口迁移和基础成交测试。
- REST `meta` 和 `allMids` 查询模型；
- metadata 快照和 allMids 增量解析模型；
- WS 单连接 writer/reader、订阅发送和 post response correlation 基础设施；
- LiveBroker metadata/mids 快照刷新入口和 metadata 定时刷新任务。
- Alloy + `rmp-serde` 的 L1 action 签名基础设施；
- 官方 Rust SDK limit order 签名兼容性测试。
- `updateLeverage` action 的签名、WS post 与 Cross target-market 初始化配置。
- 基于可用保证金比例、目标杠杆和价格的向下量化 size helper。
- 现有仓位、挂单与 pending order notional 的 post-trade 风控计算。
- LiveBroker 主账户 `connect().await` 初始化；
- `allMids`、`clearinghouseState`、`openOrders`、`orderUpdates` 和 `userFills` 订阅；
- 账户和挂单快照的 REST/WS 解析及权威状态更新。

尚未完成：

- WS 断线恢复的完整请求状态语义和 REST reconciliation 失败诊断；
- nonce manager；
- 安全的 REST exchange fallback（需要区分未发送和结果未知）；
- WS 自动重连、断线恢复和 LiveBroker 事件消费；
- 结构化事件的持久化审计和成交账本；
- 账户 WS 订阅和本地状态 reconciliation；
- OutcomeUnknown 在进程重启后的自动 cloid orderStatus 对账；
- 完整 timeout、幂等查询和 audit 测试；
- PaperBroker。

因此当前 `HyperliquidLiveBroker` 仍是接口适配层，尚不能真实下单。目标是先实现
testnet 上的完整闭环，再扩展 mainnet、spot、HIP-3 和批量 TP/SL action。
