# Audit 与账本设计

## 目标

系统将操作诊断与经济事实拆分为两个独立的数据流：

- Audit 用于诊断本地操作、请求结果和状态协调问题。它可以丢失，不能用于计算绩效。
- Ledger 用于记录跨 `Backtest`、`Paper`、`Live` 一致的不可变经济事实。绩效报告仅消费
  Ledger 或其持久化回放结果。

该边界使策略、Broker 和绩效模块不依赖具体文件格式、路径、队列或数据库实现。

## 模型

### Audit

调用方创建 `AuditRecord`，提供以下上下文：

| 字段 | 含义 |
| --- | --- |
| `strategy_id` | 产生操作的策略 |
| `mode` | `Backtest`、`Paper` 或 `Live` |
| `exchange` | 交易所标识 |
| `symbol` | 可选标的 |
| `action` | 下单、撤单、平仓、设置杠杆或状态协调 |
| `data` | 脱敏后的自由格式诊断数据 |

`RunJournal::record_audit` 补充 `journal_id` 与本地 `record_at`，形成 `AuditEvent`。其中
`record_at` 是本地记录时间，不代表交易所成交或结算时间。

`data` 不得包含签名 payload、nonce、私钥、授权材料或其他秘密。Audit 中的“请求已接受”
仅说明本地或交易所接受请求，不能代表已成交。

### Ledger

`LedgerEvent` 表示经济事实，包含来源稳定的 `event_id`、策略、运行模式、交易所和实际发生
时间。它不携带用于归档的 `journal_id`。

当前事件类型为：

- `Fill`：确认成交。
- `Funding`：资金费现金流。
- `Liquidation`：清算事实。
- `EquitySnapshot`：账户权益快照。

来源未可靠提供的字段必须保留 `None`，特别是实盘手续费、资金费率、结算价格、清算价格和
派生损益。`Some(Decimal::ZERO)` 只表示来源明确确认数值为零。

## 记录入口

`RunJournal` 是一个运行记录集合的唯一业务入口：

```rust
let journal = Arc::new(
    RunJournal::new(journal_id, ledger_sink).with_audit_sink(audit_sink),
);
```

- `journal_id` 标识持久化记录集合。回测通常每次运行创建一个；Live 应由运行装配层决定是否
  跨进程重启稳定复用。
- Ledger sink 是必需的，因为账本事实是绩效回放的输入。
- Audit sink 是可选的，缺失时 Audit 不落盘。
- Broker 只持有 `Arc<RunJournal>`，只调用 `record_audit` 与 `record_ledger`。

业务层不得直接依赖 JSONL、目录、队列、数据库或日志留存参数。

## Broker 责任

### Backtest

`HyperliquidBacktestBroker` 必须接收 `RunJournal`。成功成交、资金费、可能的清算以及账户
状态变化后的权益快照都会记录为 Ledger 事件。回测没有外部来源 ID 时，使用
`bt-<journal_id>-<sequence>` 作为稳定模拟事实 ID。

### Live

`HyperliquidLiveBroker` 已接收共享的 `RunJournal`，并记录下单、撤单、平仓、杠杆设置和
reconciliation 失败等 Audit 诊断。正常高频 WebSocket 更新不记录 Audit。

Live Ledger 尚未完整接入。后续只能由交易所确认的 `userFills` 创建 `Fill`；不能将订单请求
成功当作成交。账户快照、资金费与清算也必须来自可靠的交易所数据后，才可记录为 Ledger。

## 存储与异步

`audit` 不依赖 `storage`。存储层通过以下 trait 承接事件：

```rust
pub trait AuditSink {
    fn record(&mut self, event: &AuditEvent) -> anyhow::Result<()>;
}

pub trait LedgerSink {
    fn record(&mut self, event: &LedgerEvent) -> anyhow::Result<()>;
}
```

当前 JSONL 实现为同步写入；`AsyncAuditSink` 和 `AsyncLedgerSink` 可以包装任意 sink，使用
有界队列在后台写入。队列已满、后台退出或写入失败会计数，但不会阻塞 Broker。运行装配层
持有异步 sink 的生命周期，并在结束时调用 `shutdown()` 排空已接受事件。

异步队列容量与 Audit 留存上限是不同概念：

- 队列容量限制内存中的待写事件数，满时允许丢弃 Audit。
- 留存上限限制已持久化 Audit 的数量，不能由通用 `AsyncAuditSink` 管理。

## Audit JSONL 留存

Audit 的目标语义是单个 `audit.jsonl` 文件最多保留最近的 `N` 条事件，不按时间或字节数
限制，也不保留多个分片文件。写入第 `N + 1` 条事件时，删除最旧事件并保留最新 `N` 条。

该策略属于 `storage::JsonlAuditSink`：

1. 未达到 `N` 条时直接追加。
2. 达到上限时，读取现有 JSONL，丢弃最旧事件，将剩余事件和新事件写入临时文件。
3. 成功写入并同步后，以原子替换更新 `audit.jsonl`。
4. 重启时根据单文件内容恢复当前事件数；若文件已超过 `N`，下一次整理时裁剪为最新 `N` 条。

JSONL 无法高效原地删除文件头，因此满额后的写入复杂度为 `O(N)`。Audit 是低频诊断流，
此代价可接受；高频经济事实必须写入 Ledger，而不是 Audit。

目前 `JsonlAuditSink` 仍使用事件数分片实现，尚未符合上述单文件留存目标；切换时应删除
分片扫描与轮转逻辑，避免遗留文件被误读。

## 回放与绩效

`JsonlLedgerReader` 负责回放持久化账本事件。`performance` 模块从 Ledger 重建报告，并按
`event_id` 去重；它不读取 Audit，也不依赖运行模式的专用报告结构。

Audit 可辅助排查为什么某个操作失败或结果未知，Ledger 则回答实际发生了哪些经济事件。两者
不得互相替代。
