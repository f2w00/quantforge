use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError, sync_channel};
use std::thread::{self, JoinHandle};

use crate::audit::{AuditEvent, AuditSink, LedgerEvent, LedgerSink};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AsyncSinkStatus {
    pub accepted: u64,
    pub dropped: u64,
    pub write_failures: u64,
}

#[derive(Default)]
struct AsyncSinkCounters {
    accepted: AtomicU64,
    dropped: AtomicU64,
    write_failures: AtomicU64,
}

impl AsyncSinkCounters {
    fn status(&self) -> AsyncSinkStatus {
        AsyncSinkStatus {
            accepted: self.accepted.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            write_failures: self.write_failures.load(Ordering::Relaxed),
        }
    }
}

/// 使用有界队列在后台写入操作审计事件。
pub struct AsyncAuditSink {
    sender: SyncSender<AuditEvent>,
    counters: Arc<AsyncSinkCounters>,
    worker: Option<JoinHandle<()>>,
}

impl AsyncAuditSink {
    pub fn new<S>(mut sink: S, capacity: usize) -> Self
    where
        S: AuditSink + Send + 'static,
    {
        let (sender, receiver) = sync_channel(capacity.max(1));
        let counters = Arc::new(AsyncSinkCounters::default());
        let worker_counters = Arc::clone(&counters);
        let worker = thread::spawn(move || {
            while let Ok(event) = receiver.recv() {
                if sink.record(&event).is_err() {
                    worker_counters
                        .write_failures
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        Self {
            sender,
            counters,
            worker: Some(worker),
        }
    }

    pub fn status(&self) -> AsyncSinkStatus {
        self.counters.status()
    }

    /// 关闭投递端并等待后台工作线程处理已入队事件。
    pub fn shutdown(mut self) -> std::thread::Result<()> {
        drop(self.sender);
        self.worker.take().map(JoinHandle::join).unwrap_or(Ok(()))
    }
}

impl AuditSink for AsyncAuditSink {
    fn record(&mut self, event: &AuditEvent) -> anyhow::Result<()> {
        match self.sender.try_send(event.clone()) {
            Ok(()) => {
                self.counters.accepted.fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                self.counters.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(())
    }
}

/// 使用有界队列在后台写入账本事件。
pub struct AsyncLedgerSink {
    sender: SyncSender<LedgerEvent>,
    counters: Arc<AsyncSinkCounters>,
    worker: Option<JoinHandle<()>>,
}

impl AsyncLedgerSink {
    pub fn new<S>(mut sink: S, capacity: usize) -> Self
    where
        S: LedgerSink + Send + 'static,
    {
        let (sender, receiver) = sync_channel(capacity.max(1));
        let counters = Arc::new(AsyncSinkCounters::default());
        let worker_counters = Arc::clone(&counters);
        let worker = thread::spawn(move || {
            while let Ok(event) = receiver.recv() {
                if sink.record(&event).is_err() {
                    worker_counters
                        .write_failures
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        Self {
            sender,
            counters,
            worker: Some(worker),
        }
    }

    pub fn status(&self) -> AsyncSinkStatus {
        self.counters.status()
    }

    /// 关闭投递端并等待后台工作线程处理已入队事件。
    pub fn shutdown(mut self) -> std::thread::Result<()> {
        drop(self.sender);
        self.worker.take().map(JoinHandle::join).unwrap_or(Ok(()))
    }
}

impl LedgerSink for AsyncLedgerSink {
    fn record(&mut self, event: &LedgerEvent) -> anyhow::Result<()> {
        match self.sender.try_send(event.clone()) {
            Ok(()) => {
                self.counters.accepted.fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                self.counters.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc::{Receiver, Sender, channel};
    use std::sync::{Arc, Mutex};

    use crate::audit::{AuditAction, AuditEvent, AuditRecord, LedgerEvent, LedgerEventKind};
    use crate::core::{Decimal, JournalId, RunMode, StrategyId};
    use crate::storage::{JsonlAuditSink, JsonlReader, JsonlWriter};

    use super::*;

    static NEXT_TEMP_FILE: AtomicU64 = AtomicU64::new(0);

    fn temporary_path() -> PathBuf {
        std::env::temp_dir().join(format!(
            "qf-async-jsonl-{}-{}-{}.jsonl",
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
                data: serde_json::Value::Null,
            },
        }
    }

    fn ledger_event() -> LedgerEvent {
        LedgerEvent {
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
        }
    }

    struct MemoryAuditSink(Arc<Mutex<Vec<AuditEvent>>>);

    impl AuditSink for MemoryAuditSink {
        fn record(&mut self, event: &AuditEvent) -> anyhow::Result<()> {
            self.0.lock().unwrap().push(event.clone());
            Ok(())
        }
    }

    struct BlockingLedgerSink {
        ready: Sender<()>,
        release: Receiver<()>,
        blocks_next_write: bool,
    }

    impl LedgerSink for BlockingLedgerSink {
        fn record(&mut self, _event: &LedgerEvent) -> anyhow::Result<()> {
            if self.blocks_next_write {
                self.blocks_next_write = false;
                self.ready.send(()).unwrap();
                self.release.recv().unwrap();
            }
            Ok(())
        }
    }

    #[test]
    fn writes_audit_events_on_the_background_worker() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut sink = AsyncAuditSink::new(MemoryAuditSink(Arc::clone(&events)), 2);

        sink.record(&audit_event()).unwrap();
        sink.shutdown().unwrap();

        assert_eq!(events.lock().unwrap().len(), 1);
    }

    #[test]
    fn drains_audit_events_into_rotated_jsonl_segments() {
        let path = temporary_path();
        let writer = JsonlWriter::create(&path).unwrap();
        let jsonl_sink = JsonlAuditSink::new(writer, 2).unwrap();
        let mut sink = AsyncAuditSink::new(jsonl_sink, 8);

        for _ in 0..3 {
            sink.record(&audit_event()).unwrap();
        }
        sink.shutdown().unwrap();

        let second_path = path.with_file_name(format!(
            "{}-000001.{}",
            path.file_stem().unwrap().to_string_lossy(),
            path.extension().unwrap().to_string_lossy(),
        ));
        let first: Vec<AuditEvent> = JsonlReader::open(&path).unwrap().read_all().unwrap();
        let second: Vec<AuditEvent> = JsonlReader::open(&second_path).unwrap().read_all().unwrap();

        assert_eq!([first.len(), second.len()], [2, 1]);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_file(second_path).unwrap();
    }

    #[test]
    fn drops_ledger_events_when_the_queue_is_full() {
        let (ready_sender, ready_receiver) = channel();
        let (release_sender, release_receiver) = channel();
        let mut sink = AsyncLedgerSink::new(
            BlockingLedgerSink {
                ready: ready_sender,
                release: release_receiver,
                blocks_next_write: true,
            },
            1,
        );
        let event = ledger_event();

        sink.record(&event).unwrap();
        ready_receiver.recv().unwrap();
        sink.record(&event).unwrap();
        sink.record(&event).unwrap();

        assert_eq!(
            sink.status(),
            AsyncSinkStatus {
                accepted: 2,
                dropped: 1,
                write_failures: 0,
            }
        );
        release_sender.send(()).unwrap();
        sink.shutdown().unwrap();
    }
}
