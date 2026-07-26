Objective
- 完成 `qf::hyperliquid` 的生产级 `HyperliquidLiveBroker`：主账户异步初始化、WS 长连接及四类账户订阅、签名下单/撤单、状态同步、重连和 reconciliation。
- 不引入 `hyperliquid_rust_sdk`，使用 Alloy + `rmp-serde` 自行实现 Hyperliquid 协议；subaccount/vault 暂不实现。

## Important Details
- 核心接口：`HyperliquidBroker: Send + Sync`，交易方法使用 `async fn + &self`；保留 `place_order`、`cancel_order`、`close_position`。
- 主账户模型：
  - `signer.wallet_address()` 用于签名。
  - `HlLiveBrokerConfig.account_address` 用于 REST 查询和账户 WS 订阅。
  - 暂不支持 `vaultAddress`、subaccount。
- 推荐初始化入口：
  ```rust
  HyperliquidLiveBroker::connect(config, signer, risk_guard).await
  ```
  `connect()` 按顺序完成 REST snapshot、WS 建连、订阅、ACK 等待和后台任务启动。
- 订阅全部复用一条 WS：
  - `allMids`
  - `clearinghouseState`
  - `openOrders`
  - `orderUpdates`
  - `userFills`
- 权威状态来源：
  - `clearinghouseState`：账户、仓位
  - `openOrders`：活动订单集合
  - `orderUpdates`：订单生命周期事件
  - `userFills`：成交事件/明细
  - `allMids`：市场 mid
- `orderUpdates` / `userFills` 当前暂存为最多 1000 条原始 `serde_json::Value`，尚未结构化为审计/成交账本。
- 使用官方 Hyperliquid Python/Rust SDK 作为协议参考；官方 Rust SDK 自身使用 Alloy，但不直接引入。
- 当前已提交 commit：
  ```text
  a80d8db feat(hyperliquid): 完成实盘 broker 基础链路
  ```
- 不要丢弃仓库原有 `HANDOFF.md` 改动；提交前工作区曾包含大量用户已有和本次实现改动。

## Work State
### Completed
- 已提交基础阶段：
  - 异步 `HyperliquidBroker` trait。
  - BacktestBroker 迁移、market 成交、平仓、PnL、RiskGuard。
  - `HlOrderRequest` 强类型订单模型。
  - `HlCloseRequest` / `HlCloseSize::{Full, Exact}`。
  - `HlOrderResult` / `HlOrderOutcome` / `HlBrokerError`。
  - `HlClientOrderId` 强类型和自动生成。
  - `HyperliquidRestClient` 的 `meta()`、`all_mids()`、`clearinghouse_state()`、`open_orders()`。
  - `HyperliquidWsClient` 单连接 writer/reader、post correlation、订阅发送。
  - Alloy + `rmp-serde` L1 action 签名。
  - 官方 Rust SDK limit-order 签名向量验证。
  - LiveBroker place/cancel 的签名 WS post 和 response parsing。
  - 主账户 `HlLiveBrokerConfig`、`HlNetwork`、`connect().await`。
  - REST 初始 metadata/allMids/account/openOrders snapshot。
  - 五个 WS 订阅和订阅 ACK 等待。
  - `clearinghouseState` / `openOrders` 状态更新。
  - metadata 定时刷新。
  - 设计文档 `doc/hyperliquid-broker-design.md`。
- 最近未提交改动中已实现：
  - WS 基础自动重连循环，重连后重发已记录的 subscribe 消息。
  - WS post 30 秒 timeout。
  - LiveBroker `HlFreshness` 及 metadata/mids/account/open_orders freshness 更新。
  - 定时 REST reconciliation：刷新 account 和 open orders。
  - place/close 前 `ensure_trading_state_fresh()`。
  - 下单数量 `size_decimals` 校验。
  - Trigger Market/Limit 的 wire price 计算：
    - Market：mid/trigger price + 方向性滑点保护。
    - Limit：使用指定 limit price。
- 最近验证通过：
  ```text
  cargo fmt --all
  cargo check --workspace
  cargo test --workspace
  ```
  测试结果：
  ```text
  15 passed
  0 failed
  ```
- 最近修改后仍未重新提交。

### Active
- 继续完成用户要求的“都实现”，重点剩余：
  - 审查并收紧 WS supervisor/reconnect 设计。
  - 确保断线时 pending request 正确结束为 `Transport` 或 `OutcomeUnknown`。
  - 完善订阅 ACK 的错误解析和超时。
  - 完善 REST reconciliation 的 freshness 标记和失败策略。
  - 实现完整 price/size precision normalization：
    - size decimals 严格校验或明确量化策略。
    - Hyperliquid 5 位有效数字。
    - perp price decimals `<= 6 - szDecimals`。
    - 去除尾零。
  - 结构化 `orderUpdates` / `userFills` 事件。
  - 实现 REST `/exchange` fallback。
  - 实现 `OutcomeUnknown` 后基于 cloid 的 order status reconciliation。
  - 继续补充测试和更新文档。
- 当前代码中新增的 `HlLiveBrokerConfig` 字段：
  ```rust
  pub metadata_refresh_interval: Duration
  pub connect_timeout: Duration
  pub freshness_max_age: Duration
  pub reconciliation_interval: Duration
  ```
- 当前 LiveBroker 已不再使用旧的可选 signer/ws transport；`from_parts()` 内部持有：
  ```rust
  Arc<HyperliquidSigner>
  HyperliquidWsClient
  ```
- 当前 `HlNetwork`：
  ```rust
  Mainnet -> https://api.hyperliquid.xyz / wss://api.hyperliquid.xyz/ws
  Testnet -> https://api.hyperliquid-testnet.xyz / wss://api.hyperliquid-testnet.xyz/ws
  ```

### Blocked
- 没有当前编译或测试失败。
- 尚未有真实 testnet 集成测试；不能宣称真实下单链路已在交易所验证。
- WS 自动重连实现仍需审查：当前 `HyperliquidWsClient::connect()` 内部 supervisor 同时处理 outbound、inbound 和订阅重发，断线 pending request 的语义需要进一步确认。
- 当前订阅 ACK 只按 `/data/subscription/type` 计数，没有严格判断 ACK 是否表示错误。
- REST reconciliation 失败目前静默保留旧状态，尚未完整实现 stale/不可用状态传播。
- `orderUpdates` / `userFills` 仍是原始事件，不支持结构化成交账本、手续费、realized PnL 审计。
- REST `/exchange`、`orderStatus` 查询尚未实现，因此 `OutcomeUnknown` 还不能自动对账。
- subaccount/vault 明确暂不实现。

## Next Move
1. 审查并修正 `crates/qf/src/hyperliquid/client/ws.rs` 的 reconnect/pending request/订阅 ACK 语义，补断线和 timeout 测试。
2. 实现 REST reconciliation freshness 失败处理、结构化 `orderUpdates/userFills`、精度规范化和 `orderStatus`/REST fallback，然后运行 `cargo fmt --all`、`cargo test --workspace`、`cargo check --workspace` 并提交本轮改动。

## Relevant Files
- `crates/qf/src/hyperliquid/broker/live.rs`: `HyperliquidLiveBroker`、`HlLiveBrokerConfig`、`HlNetwork`、`connect()`、freshness、reconciliation、WS 事件消费、place/cancel。
- `crates/qf/src/hyperliquid/client/ws.rs`: WS 长连接、writer/reader、post correlation、订阅、重连。
- `crates/qf/src/hyperliquid/client/rest.rs`: REST `meta`、`allMids`、账户快照、open orders、wire 解析。
- `crates/qf/src/hyperliquid/client/signer.rs`: Alloy private-key signer、L1 action hash、EIP-712 Agent、MsgPack DTO、nonce。
- `crates/qf/src/hyperliquid/types/order.rs`: `HlOrderRequest`、`HlOrderType`、`HlCloseRequest`、wire action、signature payload。
- `crates/qf/src/hyperliquid/types/market.rs`: `HlAssetMeta`、`HlMetadataSnapshot`、`HlMidSnapshot`、allMids 解析。
- `crates/qf/src/hyperliquid/types/account.rs`: `HlAccountState`。
- `crates/qf/src/hyperliquid/types/position.rs`: `HlPosition`。
- `crates/qf/src/hyperliquid/broker/state.rs`: 本地账户和 open orders snapshot。
- `crates/qf/src/hyperliquid/broker/error.rs`: `HlBrokerError`，包括 `OutcomeUnknown`、`StateUnavailable`。
- `crates/qf/src/hyperliquid/broker/traits.rs`: `HyperliquidBroker` 公共接口。
- `crates/qf/src/hyperliquid/broker/backtest.rs`: 已迁移的异步回测 Broker及测试。
- `doc/hyperliquid-broker-design.md`: 当前设计和实现状态文档。
- `HANDOFF.md`: 原有工作交接文档，已有用户改动，不要丢弃。
- `Cargo.toml`: workspace 依赖，包括 `alloy`、`rmp-serde`、`reqwest`、`tokio-tungstenite`。
- `crates/qf/Cargo.toml`: qf crate 依赖。
- `a80d8db`: 最近一次已提交的基础链路 commit。
