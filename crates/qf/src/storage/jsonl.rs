use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use serde::{Serialize, de::DeserializeOwned};

use crate::audit::{AuditEvent, AuditSink, LedgerEvent, LedgerSink};

pub struct JsonlWriter {
    path: PathBuf,
    writer: BufWriter<File>,
}

pub struct JsonlReader {
    reader: BufReader<File>,
}

/// JSONL 账本文件的专用读取入口。
pub struct JsonlLedgerReader;

pub struct JsonlAuditSink {
    path: PathBuf,
    writer: JsonlWriter,
    events_in_file: usize,
    max_events_per_file: usize,
    segment: usize,
}

impl JsonlAuditSink {
    pub fn new(writer: JsonlWriter, max_events_per_file: usize) -> anyhow::Result<Self> {
        if max_events_per_file == 0 {
            bail!("max_events_per_file must be greater than zero");
        }

        let (segment, events_in_file) = Self::latest_segment(&writer.path)?;
        let writer = if segment == 0 {
            writer
        } else {
            JsonlWriter::create(Self::segment_path(&writer.path, segment))?
        };

        Ok(Self {
            path: writer.path.clone(),
            writer,
            events_in_file,
            max_events_per_file,
            segment,
        })
    }

    fn latest_segment(path: &Path) -> anyhow::Result<(usize, usize)> {
        let mut latest_segment = 0;
        let mut latest_path = path.to_path_buf();

        if let Some(parent) = path.parent() {
            for entry in std::fs::read_dir(parent).with_context(|| {
                format!("failed to read audit log directory: {}", parent.display())
            })? {
                let entry = entry?;
                if let Some(segment) = Self::segment_number(path, &entry.path()) {
                    if segment > latest_segment {
                        latest_segment = segment;
                        latest_path = entry.path();
                    }
                }
            }
        }

        Ok((latest_segment, Self::event_count(&latest_path)?))
    }

    fn segment_number(base_path: &Path, path: &Path) -> Option<usize> {
        let stem = base_path.file_stem()?.to_str()?;
        let extension = base_path.extension()?.to_str()?;
        let file_name = path.file_name()?.to_str()?;
        let prefix = format!("{stem}-");
        let suffix = format!(".{extension}");
        let segment = file_name.strip_prefix(&prefix)?.strip_suffix(&suffix)?;

        segment.parse().ok()
    }

    fn segment_path(base_path: &Path, segment: usize) -> PathBuf {
        let stem = base_path.file_stem().unwrap_or_default().to_string_lossy();
        let extension = base_path.extension().unwrap_or_default().to_string_lossy();
        base_path.with_file_name(format!("{stem}-{segment:06}.{extension}"))
    }

    fn event_count(path: &Path) -> anyhow::Result<usize> {
        if !path.exists() {
            return Ok(0);
        }

        BufReader::new(File::open(path)?)
            .lines()
            .try_fold(0, |count, line| {
                line.map(|line| count + usize::from(!line.trim().is_empty()))
            })
            .with_context(|| format!("failed to count audit events in: {}", path.display()))
    }

    fn rotate(&mut self) -> anyhow::Result<()> {
        self.segment += 1;
        self.writer = JsonlWriter::create(Self::segment_path(&self.path, self.segment))?;
        self.events_in_file = 0;
        Ok(())
    }
}

impl AuditSink for JsonlAuditSink {
    fn record(&mut self, event: &AuditEvent) -> anyhow::Result<()> {
        if self.events_in_file == self.max_events_per_file {
            self.rotate()?;
        }
        self.writer.write(event)?;
        self.events_in_file += 1;
        Ok(())
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

impl JsonlLedgerReader {
    pub fn read_all(path: impl AsRef<Path>) -> anyhow::Result<Vec<LedgerEvent>> {
        JsonlReader::open(path)?.read_all()
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
        AuditAction, AuditEvent, AuditRecord, AuditSink, LedgerEvent, LedgerEventKind, LedgerSink,
    };
    use crate::core::{Decimal, JournalId, RunMode, StrategyId};

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

    fn audit_event() -> AuditEvent {
        AuditEvent {
            journal_id: JournalId::new("backtest-1"),
            record_at: chrono::Utc::now(),
            record: AuditRecord {
                strategy_id: StrategyId::new("strategy-1"),
                mode: RunMode::Backtest,
                exchange: "hyperliquid".to_string(),
                symbol: Some("BTC".to_string()),
                action: AuditAction::PlaceOrder,
                data: serde_json::json!({"size": "1"}),
            },
        }
    }

    #[test]
    fn reads_events_written_as_jsonl() {
        let path = temporary_path();
        let event = LedgerEvent {
            event_id: "snapshot-1".to_string(),
            strategy_id: StrategyId::new("strategy-1"),
            mode: RunMode::Backtest,
            exchange: "hyperliquid".to_string(),
            timestamp: chrono::Utc::now(),
            event: LedgerEventKind::EquitySnapshot {
                equity: Decimal::new(100, 0),
                margin_used: Decimal::ZERO,
                realized_pnl: Some(Decimal::ZERO),
                unrealized_pnl: Some(Decimal::ZERO),
                trading_fees: Some(Decimal::ZERO),
                funding_pnl: Some(Decimal::ZERO),
            },
        };

        let writer = JsonlWriter::create(&path).unwrap();
        JsonlLedgerSink::new(writer).record(&event).unwrap();
        let events = JsonlLedgerReader::read_all(&path).unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_id, event.event_id);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn reads_audit_events_written_by_jsonl_sink() {
        let path = temporary_path();
        let event = audit_event();

        let writer = JsonlWriter::create(&path).unwrap();
        JsonlAuditSink::new(writer, 10)
            .unwrap()
            .record(&event)
            .unwrap();
        let events: Vec<AuditEvent> = JsonlReader::open(&path).unwrap().read_all().unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].record.symbol, event.record.symbol);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn rotates_audit_logs_after_the_configured_event_count() {
        let path = temporary_path();
        let writer = JsonlWriter::create(&path).unwrap();
        let mut sink = JsonlAuditSink::new(writer, 2).unwrap();

        for _ in 0..5 {
            sink.record(&audit_event()).unwrap();
        }
        drop(sink);

        let first: Vec<AuditEvent> = JsonlReader::open(&path).unwrap().read_all().unwrap();
        let second_path = JsonlAuditSink::segment_path(&path, 1);
        let third_path = JsonlAuditSink::segment_path(&path, 2);
        let second: Vec<AuditEvent> = JsonlReader::open(&second_path).unwrap().read_all().unwrap();
        let third: Vec<AuditEvent> = JsonlReader::open(&third_path).unwrap().read_all().unwrap();

        assert_eq!([first.len(), second.len(), third.len()], [2, 2, 1]);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_file(second_path).unwrap();
        std::fs::remove_file(third_path).unwrap();
    }

    #[test]
    fn resumes_writing_to_the_latest_non_full_audit_segment() {
        let path = temporary_path();
        let writer = JsonlWriter::create(&path).unwrap();
        let mut sink = JsonlAuditSink::new(writer, 2).unwrap();
        for _ in 0..3 {
            sink.record(&audit_event()).unwrap();
        }
        drop(sink);

        let writer = JsonlWriter::create(&path).unwrap();
        let mut sink = JsonlAuditSink::new(writer, 2).unwrap();
        sink.record(&audit_event()).unwrap();
        drop(sink);

        let second_path = JsonlAuditSink::segment_path(&path, 1);
        let first: Vec<AuditEvent> = JsonlReader::open(&path).unwrap().read_all().unwrap();
        let second: Vec<AuditEvent> = JsonlReader::open(&second_path).unwrap().read_all().unwrap();

        assert_eq!([first.len(), second.len()], [2, 2]);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_file(second_path).unwrap();
    }

    #[test]
    fn rejects_zero_maximum_audit_events() {
        let path = temporary_path();
        let writer = JsonlWriter::create(&path).unwrap();

        assert!(JsonlAuditSink::new(writer, 0).is_err());
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
            path: path.to_path_buf(),
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
