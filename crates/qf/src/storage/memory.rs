use std::sync::{Arc, Mutex};

use crate::audit::{AuditEvent, AuditSink, LedgerEvent, LedgerSink};

/// 将审计事件保存在进程内存中，适用于回测和测试。
#[derive(Clone, Default)]
pub struct MemoryAuditSink {
    events: Arc<Mutex<Vec<AuditEvent>>>,
}

impl MemoryAuditSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn events(&self) -> Vec<AuditEvent> {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl AuditSink for MemoryAuditSink {
    fn record(&mut self, event: &AuditEvent) -> anyhow::Result<()> {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(event.clone());
        Ok(())
    }
}

/// 将账本事件保存在进程内存中，适用于回测和测试。
#[derive(Clone, Default)]
pub struct MemoryLedgerSink {
    events: Arc<Mutex<Vec<LedgerEvent>>>,
}

impl MemoryLedgerSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn events(&self) -> Vec<LedgerEvent> {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl LedgerSink for MemoryLedgerSink {
    fn record(&mut self, event: &LedgerEvent) -> anyhow::Result<()> {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(event.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::audit::{AuditAction, AuditRecord, LedgerEventKind};
    use crate::core::{Decimal, JournalId, RunMode, StrategyId};

    use super::*;

    #[test]
    fn stores_audit_events_in_shared_memory() {
        let mut sink = MemoryAuditSink::new();
        let reader = sink.clone();
        let event = AuditEvent {
            journal_id: JournalId::new("backtest-1"),
            record_at: chrono::Utc::now(),
            record: AuditRecord {
                strategy_id: StrategyId::new("strategy-1"),
                mode: RunMode::Backtest,
                exchange: "hyperliquid".to_string(),
                symbol: Some("BTC".to_string()),
                action: AuditAction::PlaceOrder,
                data: serde_json::Value::Null,
            },
        };

        sink.record(&event).unwrap();

        let events = reader.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].journal_id, event.journal_id);
    }

    #[test]
    fn stores_ledger_events_in_shared_memory() {
        let mut sink = MemoryLedgerSink::new();
        let reader = sink.clone();
        let event = LedgerEvent {
            event_id: "snapshot-1".to_string(),
            strategy_id: StrategyId::new("strategy-1"),
            mode: RunMode::Backtest,
            exchange: "hyperliquid".to_string(),
            timestamp: chrono::Utc::now(),
            event: LedgerEventKind::EquitySnapshot {
                equity: Decimal::new(100, 0),
                margin_used: Decimal::ZERO,
                realized_pnl: None,
                unrealized_pnl: None,
                trading_fees: None,
                funding_pnl: None,
            },
        };

        sink.record(&event).unwrap();

        let events = reader.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_id, event.event_id);
    }
}
