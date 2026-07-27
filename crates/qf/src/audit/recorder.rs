use crate::audit::{AuditEvent, AuditSink, LedgerEvent, LedgerSink};

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
