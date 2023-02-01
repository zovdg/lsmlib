//! Stats Module.

use std::sync::atomic::AtomicU64;

pub struct WorkerStats {
    pub read_bytes: AtomicU64,
    pub written_bytes: AtomicU64,
}

impl WorkerStats {
    pub fn new() -> Self {
        Self {
            read_bytes: 0.into(),
            written_bytes: 0.into(),
        }
    }
}

#[derive(Debug, Copy, Clone, Default)]
pub struct Stats {
    pub resident_bytes: u64,
    pub on_disk_bytes: u64,
    pub logged_bytes: u64,
    pub read_bytes: u64,
    pub written_bytes: u64,
    pub space_amp: f64,
    pub write_amp: f64,
}
