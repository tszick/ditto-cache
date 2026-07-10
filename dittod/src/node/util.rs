use super::NodeHandle;
use std::{
    sync::atomic::Ordering,
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

pub(super) fn stable_node_uuid(node_id: &str) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("ditto-cache/node/{node_id}").as_bytes(),
    )
}

/// Returns the total size (in bytes) of all regular files in `path`.
/// Returns 0 if the directory does not exist or cannot be read.
pub(super) fn dir_size_bytes(path: &str) -> u64 {
    std::fs::read_dir(path)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter_map(|e| e.metadata().ok())
                .filter(|m| m.is_file())
                .map(|m| m.len())
                .sum()
        })
        .unwrap_or(0)
}

impl NodeHandle {
    pub fn record_snapshot_restore(&self, path: String, entries: u64, duration_ms: u64) {
        *self.snapshot_last_load_path.lock().unwrap() = Some(path);
        self.snapshot_last_load_entries
            .store(entries, Ordering::Relaxed);
        self.snapshot_last_load_duration_ms
            .store(duration_ms, Ordering::Relaxed);
        self.snapshot_last_load_completed_at_ms
            .store(Self::now_millis(), Ordering::Relaxed);
    }

    pub(super) fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}
