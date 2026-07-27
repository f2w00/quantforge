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
- `orderUpdates` / `userFills` 当前暂存为最多 1000 条带 raw payload 的结构化内存事件，尚未持久化为审计/成交账本。
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
- 最近一轮改动中已实现：
  - WS supervisor 自动重连，重连后重发已记录的 subscribe 消息。
  - WS 断线时结束 pending request，WS post 30 秒 timeout。
  - 订阅 ACK 频道校验、错误响应处理和超时。
  - LiveBroker `HlFreshness` 及 metadata/mids/account/open_orders freshness 更新。
  - 定时 REST reconciliation，失败时将对应账户/挂单 freshness 标记为不可用。
  - place/close 前 `ensure_trading_state_fresh()`。
  - 下单数量 `size_decimals` 严格校验。
  - Hyperliquid 5 位有效数字和 perp price decimals 规范化。
  - Trigger Market/Limit 的 wire price 计算。
  - REST `orderStatus` 和 `/exchange` client 原语。
  - `orderUpdates` / `userFills` 的基础结构化内存投影，并保留 raw payload。
  - `HlMarketConfig` 目标市场配置，`connect()` 中仅对 Cross 市场设置并确认杠杆。
  - `updateLeverage` action 的 Alloy/rmp-serde 签名和 WS post。
  - trait 级 `calculate_order_size()`：`margin_fraction=0.5` 表示可用保证金的 50%，
    `reserve_fraction=0.2` 表示账户 equity 的 20% 预留。
  - trait 级 `calculate_close_size()` 和 `HlCloseSize::Fraction(0.5)`，按最新仓位
    计算并向下量化 50% 的 reduce-only 平仓数量。
  - `HlCoin::new()` 自动 trim 并规范化为 ASCII 大写。
  - 基于已有仓位、挂单和 pending 请求的 post-trade notional/leverage 风控。
- 最近验证通过：
  ```text
  cargo fmt --all
  cargo check --workspace
  cargo test --workspace
  ```
  测试结果：
  ```text
  21 passed
  0 failed
  ```
- 最近已提交 `77dda3f feat(hyperliquid): 收紧实盘状态与请求语义`；当前本轮修改尚未提交。

### Active
- 继续完成用户要求的“都实现”，重点剩余：
  - `reconcile_order(cloid)` 基于 cloid 查询 orderStatus，并在查询成功后释放对应风险预留。
  - 为结构化事件增加持久化审计和成交账本。
  - 明确区分 WS 未发送失败与已发送但响应未知，之后再安全接入 REST `/exchange` fallback。
  - WS 重连完成后重新确认目标市场的杠杆配置。
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
- REST reconciliation 失败已传播为对应 freshness 不可用，但尚未记录失败原因和连续失败次数。
- `orderUpdates` / `userFills` 已有基础结构化投影，但尚不支持持久化成交账本、完整手续费和 realized PnL 审计。
- REST `/exchange`、`orderStatus` 原语已实现；`reconcile_order(cloid)` 可供调用方处理
  `OutcomeUnknown`，但重启后的自动恢复仍依赖后续订单意图持久化。
- subaccount/vault 明确暂不实现。
- isolated leverage/margin 明确暂不实现；当前 `set_leverage()` 会拒绝 `is_cross=false`。

## Next Move
1. 为 `OutcomeUnknown` 增加基于 cloid 的 orderStatus 查询和恢复状态机。
2. 增加 REST/WS 失败语义测试、精度规范化测试和结构化事件测试。
3. 设计并接入持久化订单事件/成交账本，然后运行 `cargo fmt --all`、`cargo test --workspace`、`cargo check --workspace` 并提交。

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
