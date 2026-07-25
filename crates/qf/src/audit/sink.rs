use crate::audit::AuditEvent;
use crate::storage::JsonlWriter;

pub trait AuditSink {
    fn record(&mut self, event: &AuditEvent) -> anyhow::Result<()>;
}

pub struct JsonlAuditSink {
    writer: JsonlWriter,
}

impl JsonlAuditSink {
    pub fn new(writer: JsonlWriter) -> Self {
        Self { writer }
    }
}

impl AuditSink for JsonlAuditSink {
    fn record(&mut self, event: &AuditEvent) -> anyhow::Result<()> {
        self.writer.write(event)
    }
}
