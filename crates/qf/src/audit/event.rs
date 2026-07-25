use serde::{Deserialize, Serialize};

use crate::core::{RunId, RunMode, StrategyId, Timestamp};
use crate::risk::RiskDecision;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum AuditAction {
    PlaceOrder,
    CancelOrder,
    AmendOrder,
    ClosePosition,
    SyncState,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AuditEvent {
    pub run_id: RunId,
    pub strategy_id: StrategyId,
    pub mode: RunMode,
    pub exchange: String,
    pub symbol: Option<String>,
    pub action: AuditAction,
    pub raw_request: serde_json::Value,
    pub risk_decision: Option<RiskDecision>,
    pub raw_response: Option<serde_json::Value>,
    pub error: Option<String>,
    pub timestamp: Timestamp,
}
