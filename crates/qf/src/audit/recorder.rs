use std::sync::Mutex;

use crate::audit::{AuditEvent, AuditRecord, AuditSink, LedgerEvent, LedgerSink};
use crate::core::JournalId;

pub struct AuditRecorder<S> {
    sink: S,
}

impl<S: AuditSink> AuditRecorder<S> {
    pub fn new(sink: S) -> Self {
        Self { sink }
    }

    pub fn record(&mut self, event: &AuditEvent) -> anyhow::Result<()> {
        self.sink.record(event)
    }
}

pub struct LedgerRecorder<S> {
    sink: S,
}

impl<S: LedgerSink> LedgerRecorder<S> {
    pub fn new(sink: S) -> Self {
        Self { sink }
    }

    pub fn record(&mut self, event: &LedgerEvent) -> anyhow::Result<()> {
        self.sink.record(event)
    }
}

/// 一个日志集合共享的记录入口，隔离业务代码与具体存储实现。
pub struct RunJournal {
    journal_id: JournalId,
    audit_sink: Option<Mutex<Box<dyn AuditSink + Send>>>,
    ledger_sink: Mutex<Box<dyn LedgerSink + Send>>,
}

impl RunJournal {
    pub fn new<S>(journal_id: JournalId, ledger_sink: S) -> Self
    where
        S: LedgerSink + Send + 'static,
    {
        Self {
            journal_id,
            audit_sink: None,
            ledger_sink: Mutex::new(Box::new(ledger_sink)),
        }
    }

    pub fn with_audit_sink<S>(mut self, audit_sink: S) -> Self
    where
        S: AuditSink + Send + 'static,
    {
        self.audit_sink = Some(Mutex::new(Box::new(audit_sink)));
        self
    }

    pub fn journal_id(&self) -> &JournalId {
        &self.journal_id
    }

    pub fn record_audit(&self, record: AuditRecord) {
        if let Some(audit_sink) = &self.audit_sink {
            if let Ok(mut audit_sink) = audit_sink.lock() {
                let _ = audit_sink.record(&AuditEvent {
                    journal_id: self.journal_id.clone(),
                    record_at: chrono::Utc::now(),
                    record,
                });
            }
        }
    }

    pub fn record_ledger(&self, event: &LedgerEvent) {
        if let Ok(mut ledger_sink) = self.ledger_sink.lock() {
            let _ = ledger_sink.record(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::audit::AuditAction;
    use crate::core::{RunMode, StrategyId};

    struct MemoryAuditSink(Arc<Mutex<Vec<AuditEvent>>>);

    impl AuditSink for MemoryAuditSink {
        fn record(&mut self, event: &AuditEvent) -> anyhow::Result<()> {
            self.0.lock().unwrap().push(event.clone());
            Ok(())
        }
    }

    struct NoopLedgerSink;

    impl LedgerSink for NoopLedgerSink {
        fn record(&mut self, _event: &LedgerEvent) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn adds_journal_context_and_record_time_to_audit_records() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let journal = RunJournal::new(JournalId::new("live-strategy-1"), NoopLedgerSink)
            .with_audit_sink(MemoryAuditSink(Arc::clone(&events)));
        let before = chrono::Utc::now();

        journal.record_audit(AuditRecord {
            strategy_id: StrategyId::new("strategy-1"),
            mode: RunMode::Live,
            exchange: "hyperliquid".to_string(),
            symbol: Some("BTC".to_string()),
            action: AuditAction::PlaceOrder,
            data: serde_json::json!({"outcome": "accepted"}),
        });

        let events = events.lock().unwrap();
        let event = events.first().unwrap();
        assert_eq!(event.journal_id, JournalId::new("live-strategy-1"));
        assert!(event.record_at >= before);
        assert_eq!(event.record.action, AuditAction::PlaceOrder);
    }
}
