use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug, Default)]
pub struct KillSwitch {
    enabled: AtomicBool,
}

impl KillSwitch {
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::SeqCst);
    }

    pub fn disable(&self) {
        self.enabled.store(false, Ordering::SeqCst);
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }
}
