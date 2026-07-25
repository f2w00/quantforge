use crate::audit::{AuditEvent, AuditSink};

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
