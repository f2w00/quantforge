use std::path::{Path, PathBuf};

use crate::core::RunId;

#[derive(Clone, Debug)]
pub struct RunPaths {
    root: PathBuf,
    run_id: RunId,
}

impl RunPaths {
    pub fn new(root: impl Into<PathBuf>, run_id: RunId) -> Self {
        Self {
            root: root.into(),
            run_id,
        }
    }

    pub fn run_dir(&self) -> PathBuf {
        self.root.join(&self.run_id.0)
    }

    pub fn file(&self, name: impl AsRef<Path>) -> PathBuf {
        self.run_dir().join(name)
    }
}
