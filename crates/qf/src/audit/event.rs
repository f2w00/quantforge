use serde::{Deserialize, Serialize};

use crate::core::{JournalId, RunMode, StrategyId, Timestamp};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum AuditAction {
    PlaceOrder,
    CancelOrder,
    AmendOrder,
    ClosePosition,
    SetLeverage,
    ReconcileState,
    Connect,
    WebSocket,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AuditRecord {
    pub strategy_id: StrategyId,
    pub mode: RunMode,
    pub exchange: String,
    pub symbol: Option<String>,
    pub action: AuditAction,
    /// 调用方提供的脱敏诊断内容。
    pub data: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AuditEvent {
    pub journal_id: JournalId,
    /// 本地投递审计记录的时间，不代表交易所经济事件时间。
    pub record_at: Timestamp,
    #[serde(flatten)]
    pub record: AuditRecord,
}
