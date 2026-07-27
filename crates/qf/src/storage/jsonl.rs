use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::Path;

use anyhow::Context;
use serde::{Serialize, de::DeserializeOwned};

use crate::audit::{AuditEvent, AuditSink, LedgerEvent, LedgerSink};

pub struct JsonlWriter {
    writer: BufWriter<File>,
}

pub struct JsonlReader {
    reader: BufReader<File>,
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

pub struct JsonlLedgerSink {
    writer: JsonlWriter,
}

impl JsonlLedgerSink {
    pub fn new(writer: JsonlWriter) -> Self {
        Self { writer }
    }
}

impl LedgerSink for JsonlLedgerSink {
    fn record(&mut self, event: &LedgerEvent) -> anyhow::Result<()> {
        self.writer.write(event)
    }
}

impl JsonlReader {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let file = File::open(path)
            .with_context(|| format!("failed to open jsonl file: {}", path.display()))?;
        Ok(Self {
            reader: BufReader::new(file),
        })
    }

    pub fn read_all<T: DeserializeOwned>(&mut self) -> anyhow::Result<Vec<T>> {
        self.reader
            .by_ref()
            .lines()
            .enumerate()
            .filter_map(|(line_number, line)| match line {
                Ok(line) if line.trim().is_empty() => None,
                Ok(line) => Some(
                    serde_json::from_str(&line)
                        .with_context(|| format!("failed to parse jsonl line {}", line_number + 1)),
                ),
                Err(error) => Some(
                    Err(error).context(format!("failed to read jsonl line {}", line_number + 1)),
                ),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::audit::{
        AuditAction, AuditEvent, AuditSink, LedgerEvent, LedgerEventKind, LedgerSink,
    };
    use crate::core::{Decimal, RunId, RunMode, StrategyId};

    use super::*;

    static NEXT_TEMP_FILE: AtomicU64 = AtomicU64::new(0);

    fn temporary_path() -> PathBuf {
        std::env::temp_dir().join(format!(
            "qf-jsonl-{}-{}-{}.jsonl",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap(),
            NEXT_TEMP_FILE.fetch_add(1, Ordering::Relaxed),
        ))
    }

    #[test]
    fn reads_events_written_as_jsonl() {
        let path = temporary_path();
        let event = LedgerEvent {
            event_id: "snapshot-1".to_string(),
            run_id: RunId::new("run-1"),
            strategy_id: StrategyId::new("strategy-1"),
            mode: RunMode::Backtest,
            exchange: "hyperliquid".to_string(),
            timestamp: chrono::Utc::now(),
            event: LedgerEventKind::EquitySnapshot {
                equity: Decimal::new(100, 0),
                margin_used: Decimal::ZERO,
                realized_pnl: Decimal::ZERO,
                unrealized_pnl: Decimal::ZERO,
                trading_fees: Decimal::ZERO,
                funding_pnl: Decimal::ZERO,
            },
        };

        let writer = JsonlWriter::create(&path).unwrap();
        JsonlLedgerSink::new(writer).record(&event).unwrap();
        let events: Vec<LedgerEvent> = JsonlReader::open(&path).unwrap().read_all().unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_id, event.event_id);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn reads_audit_events_written_by_jsonl_sink() {
        let path = temporary_path();
        let event = AuditEvent {
            run_id: RunId::new("run-1"),
            strategy_id: StrategyId::new("strategy-1"),
            mode: RunMode::Backtest,
            exchange: "hyperliquid".to_string(),
            symbol: Some("BTC".to_string()),
            action: AuditAction::PlaceOrder,
            raw_request: serde_json::json!({"size": "1"}),
            risk_decision: None,
            raw_response: None,
            error: None,
            timestamp: chrono::Utc::now(),
        };

        let writer = JsonlWriter::create(&path).unwrap();
        JsonlAuditSink::new(writer).record(&event).unwrap();
        let events: Vec<AuditEvent> = JsonlReader::open(&path).unwrap().read_all().unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].symbol, event.symbol);
        std::fs::remove_file(path).unwrap();
    }
}

impl JsonlWriter {
    pub fn create(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("failed to create parent directory: {}", parent.display())
            })?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("failed to open jsonl file: {}", path.display()))?;

        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    pub fn write<T: Serialize>(&mut self, value: &T) -> anyhow::Result<()> {
        serde_json::to_writer(&mut self.writer, value)?;
        self.writer.write_all(b"\n")?;
        self.writer.flush()?;
        Ok(())
    }
}
