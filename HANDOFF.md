# 跨模式账本与绩效报告交接

## 目标

`Backtest`、`Paper`、`Live` 产生同格式的账本事实；`performance` 仅消费账本事件或其持久化回放结果计算报告。不要创建 backtest 专用 report。

## 当前已实现

### 账本与审计边界

- `AuditEvent` 是可丢失的操作诊断日志；`RunJournal` 统一填充 `journal_id` 与本地 `record_at`。
- 调用方传入 `AuditRecord`：策略、模式、交易所、动作、可选标的和自由格式的脱敏 `data`。不得在 `data` 写入签名 payload、nonce 或授权材料。
- `LedgerEvent` 位于 `crates/qf/src/audit/ledger.rs`，是可移植的不可变经济事实，包含稳定 `event_id`、`strategy_id`、模式、交易所和事件时间；不携带存储归档用的 `journal_id`。
- `LedgerEventKind` 已实现：`Fill`、`Funding`、`Liquidation`、`EquitySnapshot`。
- `Fill.fee` 与 `Liquidation.fee` 为 `Option<Decimal>`；实盘字段缺失必须保留 `None`，不能伪造为零。
- `Funding`、`Liquidation` 与 `EquitySnapshot` 中实盘未可靠提供的派生字段同样使用 `Option`；`Some(Decimal::ZERO)` 仅表示来源明确确认该值为零。
- `event_id` 是来源经济事实的稳定 ID：实盘优先使用交易所事实 ID；回测没有来源 ID 时使用 `bt-<journal_id>-<sequence>`。
- 派生指标不写入账本。

### 运行级记录入口

- `RunJournal` 位于 `crates/qf/src/audit/recorder.rs`，包含 `JournalId`、必需账本 sink 和可选操作审计 sink。
- `JournalId` 标识持久化记录集合：回测通常每次创建一个，Live 可跨进程重启稳定地关联同一策略的全部记录。
- Broker 仅持有 `Arc<RunJournal>`，调用 `record_ledger` 或 `record_audit`，不依赖 JSONL、路径、队列或数据库。
- `audit` 不依赖 `storage`。

### 存储实现

- `storage::JsonlAuditSink` 与 `JsonlLedgerSink` 是同步 JSONL 实现。
- `storage::JsonlReader` 支持泛型 JSONL 回放。
- `storage::JsonlLedgerReader` 是 Ledger 的专用读取入口；`JournalPaths::ledger_path()`
  定位单个 journal 的 `ledger.jsonl`。
- `storage::AsyncAuditSink` 与 `AsyncLedgerSink` 是任意 sink 的有界异步包装：调用侧使用 `try_send`，队列满或后台退出时丢弃并计数；后台写入失败同样计数。
- `AsyncSinkStatus` 提供 `accepted`、`dropped`、`write_failures`；调用 `shutdown()` 可排空已入队事件。
- 异步不是强制默认。是否异步由运行装配层注入 `RunJournal` 的 sink 决定；裸 `Jsonl*Sink` 会同步写入。
- 当前没有 `StorageConfig`、CLI、`BacktestRunner` 或应用启动层，因此仓库内部尚无“选择 JSONL / 数据库 / 队列”的配置工厂。

### 回测 Broker

- `HyperliquidBacktestBroker::new` 已强制接收 `Arc<RunJournal>`，不再有无账本构造路径。
- 成功市价成交后发出 `Fill` 和 `EquitySnapshot`。
- `apply_funding` 后发出 `Funding`、可能的 `Liquidation` 和 `EquitySnapshot`。
- 标记价格、杠杆或维持保证金变更后发出可能的 `Liquidation` 与 `EquitySnapshot`。
- 清算按标的记录数量、价格、已实现损益和 `maintenance_margin_breach` 原因。
- 回测事件 ID 为 `bt-<journal_id>-<sequence>`；其 `journal_id` 仅参与生成模拟来源 ID，不写入 ledger 事件字段。
- 事件在账户状态锁释放后写入 `RunJournal`；若注入 `AsyncLedgerSink`，不会阻塞 Broker 调用路径。
- 回测快照大多仍使用墙钟 `Utc::now()`；仅 funding 使用调用方提供的结算时间。可复现回放仍需后续从行情/replay 层传入事件时间。

### 实盘 Broker

- `HyperliquidLiveBroker::connect` 接收共享 `Arc<RunJournal>`；`JournalId` 由运行装配层选择，而不应由交易所提供。
- 下单、撤单、平仓、杠杆设置与账户/挂单 reconciliation 失败会记录 audit。正常高频 WS 更新不会写入 audit。
- Live audit 只记录请求已接受、明确拒绝、失败或结果未知等本地诊断结果；不代表成交。
- 后续只可由交易所确认的 `userFills` 生成 `Fill`，不得以订单请求成功代替成交。
- 账户 WS 快照或 reconciliation 生成 `EquitySnapshot`；资金费与清算必须来自可靠交易所数据。

### 绩效模块

- `performance::PerformanceReport::from_events` 是只读账本投影：按事件时间排序、按
  `event_id` 去重，回放 `Fill`、`Funding`、`Liquidation` 与 `EquitySnapshot`。
- 报告包含权益收益、已实现/未实现 PnL、费用、资金费、完整持仓生命周期口径的胜率、
  Profit Factor、Expectancy、最大回撤、清算数和数据质量。
- `JsonlLedgerReader::read_all(paths.ledger_path())` 可读取单个 journal 的 JSONL 账本并传入
  `PerformanceReport::from_events`。Ledger 不分段，读取器当前一次性加载文件。
- `PerformanceReport::to_markdown()` 用于人类阅读；`to_pretty_json()` 保持数值类型并可反序列化
  回 `PerformanceReport`，适合后续计算输入。
- 详细设计见 `doc/performance-design.md`。

## 运行装配示例

运行层应选择具体存储实现，再创建共享记录入口：

```rust
let paths = JournalPaths::new(storage_root, journal_id.clone());
let writer = JsonlWriter::create(paths.ledger_path())?;
let ledger = AsyncLedgerSink::new(JsonlLedgerSink::new(writer), 4_096);
let journal = Arc::new(RunJournal::new(journal_id, ledger));

let broker = HyperliquidBacktestBroker::new(
    strategy_id,
    initial_state,
    risk_guard,
    Arc::clone(&journal),
);
```

运行结束时，装配层必须持有异步 sink 的生命周期并调用 `shutdown()`；当前 `RunJournal` 不提供该能力，未来应增加运行资源/运行器对象统一管理。

## 下一步

1. 新增运行装配层和 `StorageConfig`，统一创建 JSONL/异步 sink、保留 sink 状态和 shutdown 生命周期。
2. 新增 Live JournalId 的装配策略，明确它是否跨进程重启稳定，并与策略、账户、交易所和环境关联。
3. 接入 Live ledger：仅由交易所确认的 `userFills` 写入 `Fill`，并补充账户快照、资金费与清算事件。
4. 新增运行级 CLI/API 入口，串联 journal 读取、绩效计算和 Markdown/JSON 输出。
5. 增加 `ClosedTrade` 明细、按标的/方向/时间聚合，以及在明确重采样和年化规则后加入
   Sharpe/Sortino。

## 验证与工作区

- 最近一次验证：`cargo fmt`、`cargo test -p qf`、`git diff --check`，共 58 个测试通过。
- 已提交审计与存储基础设施：`0ce3e64 feat(audit): 增加可扩展审计账本存储`。
- 当前仍有未提交的账本接入与术语调整，包含 `RunJournal` 强制注入与回测账本事件接入。
