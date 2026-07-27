use crate::audit::{AuditEvent, LedgerEvent};

pub trait AuditSink {
    fn record(&mut self, event: &AuditEvent) -> anyhow::Result<()>;
}

pub trait LedgerSink {
    fn record(&mut self, event: &LedgerEvent) -> anyhow::Result<()>;
}
