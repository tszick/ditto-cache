use bytes::Bytes;
use ditto_protocol::LogEntry;
use std::collections::VecDeque;

/// In-memory write log.
///
/// Entries are appended on every write and compacted once all active nodes
/// have confirmed they applied a given index.
pub struct WriteLog {
    entries:         VecDeque<WriteLogEntry>,
    next_index:      u64,
    committed_index: u64,
}

#[derive(Debug, Clone)]
pub struct WriteLogEntry {
    pub index:    u64,
    pub key:      String,
    pub value:    Option<Bytes>,
    pub ttl_secs: Option<u64>,
    pub ts_ms:    u64,
    pub status:   EntryStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryStatus {
    /// Sent to active nodes, awaiting all ACKs.
    Pending,
    /// All active nodes ACK'd; data is visible to reads.
    Committed,
}

impl WriteLog {
    pub fn new() -> Self {
        Self {
            entries:         VecDeque::new(),
            next_index:      1,
            committed_index: 0,
        }
    }

    /// Append a pending entry at the exact index given by the primary.
    ///
    /// Used by follower PREPARE handlers to keep indices in sync with the
    /// primary.  Idempotent: silently ignores duplicate indices.
    /// Advances `next_index` if needed so the next local `append()` won't
    /// collide.
    pub fn append_at(
        &mut self,
        index: u64,
        key: String,
        value: Option<Bytes>,
        ttl_secs: Option<u64>,
    ) {
        if self.entries.iter().any(|e| e.index == index) {
            return;
        }
        if index >= self.next_index {
            self.next_index = index + 1;
        }
        let ts_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.entries.push_back(WriteLogEntry {
            index,
            key,
            value,
            ttl_secs,
            ts_ms,
            status: EntryStatus::Pending,
        });
    }

    /// Append a new pending entry and return its log index.
    pub fn append(&mut self, key: String, value: Option<Bytes>, ttl_secs: Option<u64>) -> u64 {
        let index = self.next_index;
        self.next_index += 1;

        let ts_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        self.entries.push_back(WriteLogEntry {
            index,
            key,
            value,
            ttl_secs,
            ts_ms,
            status: EntryStatus::Pending,
        });
        index
    }

    /// Mark an entry as committed and update `committed_index`.
    pub fn commit(&mut self, index: u64) {
        if let Some(e) = self.entries.iter_mut().find(|e| e.index == index) {
            e.status = EntryStatus::Committed;
        }
        if index > self.committed_index {
            self.committed_index = index;
        }
    }

    pub fn committed_index(&self) -> u64 {
        self.committed_index
    }

    /// Reset the log after a manual flush.
    ///
    /// Clears all in-memory entries and sets `committed_index = 0` so that
    /// the next `run_resync()` will pull all available entries from peers.
    /// `next_index` is kept unchanged to prevent reuse of already-seen indices.
    pub fn reset(&mut self) {
        self.entries.clear();
        self.committed_index = 0;
        // next_index intentionally unchanged
    }

    /// Find any entry by index regardless of its committed status.
    /// Used by the COMMIT handler on follower nodes: the entry was stored as
    /// Pending on PREPARE, and we need to look it up before marking it committed.
    pub fn get_entry(&self, index: u64) -> Option<(String, Option<Bytes>, Option<u64>)> {
        self.entries
            .iter()
            .find(|e| e.index == index)
            .map(|e| (e.key.clone(), e.value.clone(), e.ttl_secs))
    }

    /// Entries after `from` (exclusive) up to `committed_index`.
    pub fn entries_since(&self, from: u64) -> Vec<LogEntry> {
        self.entries
            .iter()
            .filter(|e| e.index > from && e.status == EntryStatus::Committed)
            .map(|e| LogEntry {
                index:    e.index,
                key:      e.key.clone(),
                value:    e.value.clone(),
                ttl_secs: e.ttl_secs,
                ts_ms:    e.ts_ms,
            })
            .collect()
    }

    /// Remove all committed entries that have been applied by every active
    /// node (call this periodically to keep memory usage bounded).
    pub fn compact(&mut self, safe_index: u64) {
        while let Some(front) = self.entries.front() {
            if front.index <= safe_index && front.status == EntryStatus::Committed {
                self.entries.pop_front();
            } else {
                break;
            }
        }
    }
}
