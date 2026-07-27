use std::path::{Path, PathBuf};

use crate::core::JournalId;

#[derive(Clone, Debug)]
pub struct JournalPaths {
    root: PathBuf,
    journal_id: JournalId,
}

impl JournalPaths {
    pub fn new(root: impl Into<PathBuf>, journal_id: JournalId) -> Self {
        Self {
            root: root.into(),
            journal_id,
        }
    }

    pub fn journal_dir(&self) -> PathBuf {
        self.root.join(&self.journal_id.0)
    }

    pub fn file(&self, name: impl AsRef<Path>) -> PathBuf {
        self.journal_dir().join(name)
    }

    pub fn ledger_path(&self) -> PathBuf {
        self.file("ledger.jsonl")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_ledger_path_inside_the_journal_directory() {
        let paths = JournalPaths::new("/var/lib/qf", JournalId::new("backtest-1"));

        assert_eq!(
            paths.ledger_path(),
            PathBuf::from("/var/lib/qf/backtest-1/ledger.jsonl")
        );
    }
}
