# 跨模式绩效报告实现交接

## 目标

实现跨模式的交易账本与绩效报告能力，供 `Backtest`、`Paper`、`Live` 统一使用。

不要创建 backtest 专用的 report。回测和实盘应产生同格式的账本事件；报告层从
账本事件流或其 JSONL 持久化结果计算指标。

## 已确认的职责边界

```text
HyperliquidBacktestBroker / HyperliquidLiveBroker
  -> 产生结构化账本事件
  -> audit 持久化不可变事实（JSONL）

performance
  -> 消费账本事件
  -> 增量统计或回放历史 JSONL
  -> 输出 PerformanceReport
```

- `audit`：记录事实，不负责指标公式。
- `performance`：纯指标计算，不依赖 Hyperliquid Broker。
- Broker：成交、资金费、清算与权益变动发生时发出账本事件。
- 现有 `AuditEvent` 保持其操作审计用途，不应硬塞成交与权益统计字段。

## 当前代码状态

### 已有 audit

- `crates/qf/src/audit/event.rs`
  - `AuditEvent` 只记录操作请求、风控决定、原始响应、错误和时间。
  - `AuditAction` 当前包含下单、撤单、修改、平仓、同步状态。
- `crates/qf/src/audit/recorder.rs`
  - `AuditRecorder<S: AuditSink>` 仅将 `AuditEvent` 写入 sink。
- `crates/qf/src/audit/sink.rs`
  - `AuditSink` 当前只接收 `AuditEvent`。
  - `JsonlAuditSink` 用 `JsonlWriter` 持久化。
- `crates/qf/src/storage/jsonl.rs`
  - 只有 append writer，没有 JSONL reader。

### 已有回测 Broker

`crates/qf/src/hyperliquid/broker/backtest.rs` 已支持：

- market order 全额成交；
- 确定性不利滑点：`set_market_slippage_bps`；
- taker 手续费：`set_taker_fee_bps`；
- 杠杆、初始保证金校验、仓位/账户重估；
- funding：`apply_funding(coin, funding_rate, settlement_price, settled_at)`；
- 简化组合维持保证金清算：`set_maintenance_margin_bps`，默认 500 bps；
- 可读累计值：`realized_pnl`、`unrealized_pnl`、`trading_fees`、`funding_pnl`、
  `liquidation_count`。

回测 Broker 当前尚未输出逐笔成交、funding、清算或权益快照账本。

### 已有实盘 Broker

`crates/qf/src/hyperliquid/broker/live.rs` 已维护 WS 订单更新和 `userFills` 的有限内存历史。
后续应从交易所确认的 fills、账户快照和 funding 数据产生相同的账本事件。
不要将“下单请求已发送”错误地记录为成交。

### 核心类型

- `RunMode`：`crates/qf/src/core/mode.rs`，含 `Backtest`、`Paper`、`Live`。
- `RunId`、`StrategyId`：`crates/qf/src/core/id.rs`。
- `Timestamp`：`crates/qf/src/core/time.rs`，即 `chrono::DateTime<Utc>`。
- 公共 Decimal：`crate::core::Decimal`。

## 推荐实现范围

同时实现内存增量统计与 JSONL 回放：

1. 新增 `audit::LedgerEvent`，并持久化到 JSONL。
2. 新增独立 `performance` 模块，其输入仅为账本事件。
3. `PerformanceTracker` 支持增量消费事件并生成报告。
4. JSONL reader 支持读取历史账本并重建同一 `PerformanceReport`。
5. 回测 Broker 发出账本事件；实盘接入留出明确入口，至少不要阻碍后续接入。

## 推荐文件结构

```text
crates/qf/src/
├── audit/
│   ├── event.rs          # 既有操作审计，尽量不改语义
│   ├── ledger.rs         # 新增 LedgerEvent 及具体 payload
│   ├── recorder.rs       # 按需扩展账本记录入口
│   └── sink.rs           # 按需扩展或新增 LedgerSink
├── performance/
│   ├── mod.rs
│   ├── tracker.rs        # PerformanceTracker：增量状态机
│   ├── report.rs         # PerformanceReport 及公开数据结构
│   └── metrics.rs        # 无副作用的统计计算
└── storage/
    └── jsonl.rs          # 增加 reader，或新增专用 reader 文件
```

## LedgerEvent 建议

建议 event envelope 包含：

```rust
pub struct LedgerEvent {
    pub run_id: RunId,
    pub strategy_id: StrategyId,
    pub mode: RunMode,
    pub exchange: String,
    pub timestamp: Timestamp,
    pub event: LedgerEventKind,
}
```

`LedgerEventKind` 至少包括：

```rust
pub enum LedgerEventKind {
    Fill {
        order_id: Option<String>,
        client_order_id: Option<String>,
        symbol: String,
        side: Side,
        size: Decimal,
        price: Decimal,
        fee: Decimal,
        reduce_only: bool,
    },
    Funding {
        symbol: String,
        funding_rate: Decimal,
        settlement_price: Decimal,
        cashflow: Decimal,
    },
    Liquidation {
        symbol: Option<String>,
        realized_pnl: Decimal,
        reason: String,
    },
    EquitySnapshot {
        equity: Decimal,
        margin_used: Decimal,
        realized_pnl: Decimal,
        unrealized_pnl: Decimal,
        trading_fees: Decimal,
        funding_pnl: Decimal,
    },
}
```

字段可按现有类型和实际可获取数据微调，但应保留以下事实：

- fill 的价格、数量、方向、手续费、订单关联信息；
- funding 的费率、结算价、实际现金流；
- liquidation 的已实现损益与原因；
- equity snapshot 的时间序列。

不要把派生指标（胜率、最大回撤、Sharpe）写回 ledger。

## PerformanceReport 口径

报告必须跨模式通用，至少包含：

```text
initial_equity
final_equity
net_pnl
total_return
realized_pnl
unrealized_pnl
trading_fees
funding_pnl
closed_trade_count
win_rate
average_win
average_loss
profit_factor
max_drawdown
max_drawdown_duration
sharpe_ratio
sortino_ratio
liquidation_count
peak_margin_used
```

定义：

- `net_pnl = final_equity - initial_equity`。
- `total_return = final_equity / initial_equity - 1`；初始权益非正时需显式处理，不能除零。
- `win_rate` 只统计完整关闭仓位的净损益；不能用信号方向正确率替代。
- 单笔净损益必须包含对应开/平仓手续费和持仓期间 funding。
- `profit_factor = gross_profit / abs(gross_loss)`；无亏损需定义为 `None` 或明确的约定值，
  不要返回无意义的无穷大 Decimal。
- 最大回撤从 `EquitySnapshot` 的时间序列计算。
- Sharpe/Sortino 需要先确定权益采样频率与无风险利率；不能从无时间序列的期末权益计算。
- 实盘报告是“截至当前”，未平仓仓位计入 equity 与总收益，但不计入 closed-trade 胜率。

## 回测 Broker 接入建议

不要让 Broker 直接依赖 JSONL 或文件路径。推荐注入一个可选的、线程安全的 ledger
recorder/sink；默认关闭，保持现有 `new()` API 可用。

产生事件的时点：

- `fill_market_order` 成功后：`Fill`；
- 每次 `apply_funding`：`Funding`；
- `liquidate_if_needed` 触发时：每个被平仓市场或一条明确的组合清算事件；
- `set_mark_price`、成功成交、funding、清算后：`EquitySnapshot`。

回测的 timestamp 目前大量使用 `Utc::now()`。为可复现报告，后续 replay 应提供事件时间，
而不是依赖墙钟时间。可以先在 ledger API 中接受 timestamp，并在没有 replay 时回退至
`Utc::now()`，但要将该限制写入文档或 TODO。

## 实盘 Broker 接入建议

- 仅在 exchange 已确认 fill 时记录 `Fill`，以 `userFills` 为优先数据源。
- fee 应取交易所返回值；没有值时应显式标记缺失，不要伪造为零。
- 账户 WS 快照或 reconciliation 后记录 `EquitySnapshot`。
- funding 应来自交易所账单/用户资金费事件；当前客户端可能尚无该订阅或 REST 查询，需要
  单独补齐。
- 所有实盘事件都应带实际交易所时间和原始标识符，支持去重。

## 测试与验收

至少新增：

1. fill、fee、funding、liquidation、equity snapshot 的 JSON 序列化/反序列化测试。
2. Tracker 的增量结果与同一事件 JSONL 回放结果完全一致。
3. 长仓与短仓 funding 的现金流方向测试。
4. 一笔完整开平仓的净 PnL、胜率、盈亏比测试，含手续费与 funding。
5. 未平仓仓位不进入 `closed_trade_count` 或 `win_rate` 的测试。
6. 具有多个 equity snapshots 的最大回撤测试。
7. 空事件、零初始资金、无亏损、无快照等边界行为测试。
8. 现有回测 Broker 单测全绿；`cargo test -p qf` 全绿。

## 注意事项

- 这是跨 `audit`、`performance`、`storage`、`backtest`，后续还会涉及 `live` 的跨模块改动。
- 保持最小可验证实现，不先做复杂图表、数据库或 CLI。
- 不要修改或撤回当前工作区中其他人对 `live.rs` 的未提交变更。
- 当前分支已有提交 `bb49fcd`，其中包含回测滑点、手续费、funding、简化清算实现。
- 本会话后续又修改了 `backtest.rs`（funding/清算）；截至写此文件时未提交。提交前必须先
  检查工作区，且只 stage 本次实际修改的文件。
