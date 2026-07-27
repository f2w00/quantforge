# Performance 设计

## 目标与边界

`performance` 是基于不可变 `LedgerEvent` 的只读绩效投影模块。它为
`Backtest`、`Paper` 和 `Live` 使用同一套计算逻辑，不依赖 Broker 内部状态、Audit
事件、JSONL 路径或具体存储实现。

```text
LedgerEvent / ledger.jsonl
  -> JsonlLedgerReader
  -> PerformanceReport::from_events
  -> Markdown 或可重算 JSON
```

模块不负责：

- 下单、记账或修改账本。
- 将订单请求或 Audit 记录推断为成交。
- 选择运行目录、管理异步 sink 生命周期或执行 CLI。
- Sharpe、Sortino、年化收益等需要固定频率权益序列和年化约定的指标。

## 输入与存储

`LedgerEvent` 是唯一的经济事实输入，包含 `Fill`、`Funding`、`Liquidation` 和
`EquitySnapshot`。投影器按 `timestamp`、`event_id` 稳定排序，并通过 `event_id`
去重。重复事件不会参与指标计算，但会计入
`PerformanceDataQuality.duplicate_event_count`。

JSONL 的运行级读取路径如下：

```rust
let paths = JournalPaths::new(storage_root, journal_id);
let events = JsonlLedgerReader::read_all(paths.ledger_path())?;
let report = PerformanceReport::from_events(events);
```

`JournalPaths::ledger_path()` 固定返回
`<storage_root>/<journal_id>/ledger.jsonl`。Ledger 当前是单文件追加存储，不分段；
`JsonlLedgerReader::read_all` 会一次性加载该文件。运行规模导致单文件或内存占用不可接受时，
再单独设计流式读取，不在当前报告计算中引入存储复杂度。

`LedgerEvent` 不含显式 `journal_id`。单个 `ledger.jsonl` 对应一个 journal 时可可靠隔离；
合并多个运行的事件前，调用方必须自行确认筛选范围。若未来需要跨运行查询，应为账本事件增加
显式运行标识，不能从 `event_id` 前缀推断。

## 报告模型

`PerformanceReport` 是可序列化、可反序列化的原始计算结果。数值保持 `Decimal` 或
`Option<Decimal>`，不会被展示层替换为带逗号或百分号的字符串。

主要指标包括：

- 权益：期初权益、期末权益、总 PnL、总收益率。
- PnL：已实现 PnL、未实现 PnL、交易手续费、资金费。
- 交易：完整已平仓交易数、胜/负/平次数、胜率、平均盈亏、Profit Factor、Expectancy。
- 风险：最大回撤金额、最大回撤比例和清算次数。
- 数据质量：有效账本事件数、去重数量、缺失手续费数量和权益快照可用性。

`Option` 表示无法计算或来源未提供，不能用零替代。例如没有权益快照时，权益、总收益和回撤
相关指标为 `None`；没有输单时，`average_loss` 和 `profit_factor` 为 `None`。

## 账本回放与交易口径

### 成交与持仓生命周期

`Fill` 按标的维护一个连续净仓位及加权平均开仓价：

- 同方向成交：增加仓位，按数量更新加权平均开仓价。
- 反方向成交：先以较小数量平掉已有仓位，确认该部分已实现 PnL。
- 部分平仓：已实现 PnL 立即累积到报告；仓位仍开放，不产生新的 closed trade。
- 仓位归零：结束一笔 closed trade，用该完整仓位生命周期的净 PnL 统计胜负。
- 反手：先结束旧方向的 closed trade；超出平仓数量的部分从成交价开始创建新方向仓位。

该定义使胜率不受加仓或拆单频率影响。`closed_trades` 不等于 `Fill` 数量，也不等于部分平仓的
次数。

### 净交易 PnL 与账户 PnL

一笔 closed trade 的净 PnL 为：

```text
realized_pnl + funding_pnl - fees
```

其中成交手续费会随该生命周期累积；反手成交的手续费按平仓量与新开仓量分摊。资金费发生时归属到
当时同标的的活跃仓位。胜、负、平以该净值分别大于、少于、等于零判定。

账户级的 `realized_pnl`、`trading_fees` 和 `funding_pnl` 在 `EquitySnapshot` 提供时，以最后一个
快照中的累计值为准；没有快照时，以账本回放的累计值为准。`total_pnl` 和 `total_return` 始终由
首末权益快照计算：

```text
total_pnl = final_equity - initial_equity
total_return = total_pnl / initial_equity
```

初始权益为零时，收益率不可计算。

### 回撤

权益快照按账本事件时间回放。对每个快照维护此前最高权益：

```text
drawdown = historical_peak_equity - current_equity
drawdown_pct = drawdown / historical_peak_equity
```

最大值分别形成 `max_drawdown` 与 `max_drawdown_pct`。快照不是等频时间序列，因此当前不据此计算
Sharpe、Sortino 或年化指标。

## 数据质量

`Fill.fee = None` 或 `Liquidation.fee = None` 的含义是来源未提供手续费，而不是手续费为零。报告会
继续输出已知数据，但递增 `missing_fee_count`；费后胜率、Profit Factor 和 Expectancy 因此可能不完整。

实盘若使用 `AsyncLedgerSink`，队列满、后台退出或写入失败可能导致账本缺失。该 sink 的
`AsyncSinkStatus` 目前未写入 `PerformanceDataQuality`；生成实盘报告时，运行层应一并检查 sink
状态，不能把报告视为完整审计结论。

## 输出

### Markdown

`PerformanceReport::to_markdown()` 生成供终端、文档和评审阅读的 Markdown 表格，分为 Returns、
Trades、PnL、Risk 和 Data Quality 五个区块。

- 金额保留两位小数并使用千分位。
- PnL、收益和期望值等收益性指标的正数带 `+`。
- 比率转换为百分比。
- 无法计算的值显示为 `N/A`。

Markdown 是展示格式，不用于反序列化或下游计算。

### JSON

`PerformanceReport::to_pretty_json()` 使用缩进输出 JSON，但保持报告字段结构和原始数值类型：

```rust
let json = report.to_pretty_json()?;
let report: PerformanceReport = serde_json::from_str(&json)?;
```

该 JSON 同时适合人类检查与后续计算输入。为保持可重算性，金额不会编码为带千分位的字符串，比例
也不会编码为带 `%` 的字符串。

## 后续演进

1. 在运行层或 CLI 增加“读取 journal、计算报告、选择 Markdown/JSON 输出”的入口。
2. 增加 `ClosedTrade` 明细，以及按标的、方向和时间区间的聚合报告。
3. 定义权益重采样频率、无风险利率和年化规则后，再增加 Sharpe、Sortino、Calmar 和年化指标。
4. 为 `LedgerEvent` 增加显式运行标识，支持跨 journal 的可靠筛选与聚合。
5. 将异步 sink 丢失/失败状态纳入报告数据质量。
