use crate::store::capacity::{entry_size, remove_entry_size, replace_entry_size};
use crate::store::compression::{
    compress_value, decompress_value, export_portable_value, read_decompressed_value,
    sanitize_for_log, validate_manual_decompression, ReadDecompression,
    MAX_COMPRESS_INPUT_BYTES, MAX_DECOMPRESSED_FALLBACK_BYTES,
};
use crate::store::lfu::LfuTracker;
use crate::store::retention::{
    expires_at_from_ttl_secs, export_expires_at_ms, glob_match, restore_expires_at,
    ttl_remaining_secs,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Debug, Clone)]
pub struct Entry {
    /// Raw stored bytes (may be LZ4-compressed; see `compressed` flag).
    pub value: Bytes,
    pub version: u64,
    pub expires_at: Option<Instant>,
    pub freq_count: u64,
    /// Whether `value` is stored in LZ4-compressed form.
    pub compressed: bool,
}

impl Entry {
    pub fn is_expired(&self) -> bool {
        self.expires_at
            .map(|e| Instant::now() >= e)
            .unwrap_or(false)
    }

    /// Remaining TTL in seconds, if set.
    pub fn ttl_remaining_secs(&self) -> Option<u64> {
        ttl_remaining_secs(self.expires_at)
    }
}

/// Serialisable snapshot of a single entry (no Instant).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportEntry {
    pub value: Vec<u8>,
    pub version: u64,
    /// Unix timestamp (ms) of expiry, if set.
    pub expires_at_ms: Option<u64>,
}

impl From<&Entry> for ExportEntry {
    fn from(e: &Entry) -> Self {
        // Always export decompressed values for portability.
        // Guard against decompression bombs: check the declared decompressed size
        // stored in the LZ4 size-prepended header (first 4 bytes, little-endian u32)
        // before allocating.  Use MAX_DECOMPRESSED_FALLBACK_BYTES as the hard cap
        // because ExportEntry::from() has no access to the per-store limit.
        ExportEntry {
            value: export_portable_value(&e.value, e.compressed),
            version: e.version,
            expires_at_ms: export_expires_at_ms(e.expires_at),
        }
    }
}

pub struct KvStore {
    inner: Arc<Mutex<StoreInner>>,
}

struct StoreInner {
    data: HashMap<String, Entry>,
    lfu: LfuTracker,
    max_bytes: u64,
    used_bytes: u64,
    default_ttl: Option<Duration>,
    evictions: u64,
    hit_count: u64,
    miss_count: u64,
    /// Maximum allowed value size in bytes; 0 = unlimited.
    max_value_bytes: u64,
    /// Maximum number of keys; 0 = unlimited.
    max_keys_limit: usize,
    /// LZ4 compression enabled (logic added in Feature 3).
    compression_enabled: bool,
    /// Values larger than this threshold are compressed.
    compression_threshold_bytes: u64,
}

impl KvStore {
    pub fn new(
        max_memory_mb: u64,
        default_ttl_secs: u64,
        max_value_bytes: u64,
        max_keys_limit: usize,
        compression_enabled: bool,
        compression_threshold_bytes: u64,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(StoreInner {
                data: HashMap::new(),
                lfu: LfuTracker::new(),
                max_bytes: max_memory_mb * 1024 * 1024,
                used_bytes: 0,
                default_ttl: if default_ttl_secs > 0 {
                    Some(Duration::from_secs(default_ttl_secs))
                } else {
                    None
                },
                evictions: 0,
                hit_count: 0,
                miss_count: 0,
                max_value_bytes,
                max_keys_limit,
                compression_enabled,
                compression_threshold_bytes,
            })),
        }
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Returns the entry with the value DECOMPRESSED (transparent to caller).
    pub fn get(&self, key: &str) -> Option<Entry> {
        let mut inner = self.inner.lock().unwrap();
        // Clone first to release the immutable borrow before any mutation.
        let maybe = inner.data.get(key).cloned();
        match maybe {
            Some(entry) if entry.is_expired() => {
                inner.data.remove(key);
                inner.lfu.remove(key);
                inner.used_bytes = remove_entry_size(inner.used_bytes, key, &entry.value);
                inner.miss_count += 1;
                None
            }
            Some(mut entry) => {
                inner.lfu.touch(key);
                inner.hit_count += 1;
                // Transparently decompress before returning.
                if entry.compressed {
                    match read_decompressed_value(&entry.value, inner.max_value_bytes) {
                        ReadDecompression::SkippedDeclaredTooLarge {
                            declared_size,
                            limit,
                        } => {
                            tracing::warn!(
                                key = sanitize_for_log(key).as_str(),
                                declared_decompressed_bytes = declared_size,
                                decomp_limit = limit,
                                "Refusing LZ4 decompression: declared output size \
                                 exceeds limit; returning compressed bytes"
                            );
                            return Some(entry);
                        }
                        ReadDecompression::Decompressed(decompressed) => {
                            entry.value = decompressed;
                            entry.compressed = false;
                        }
                        ReadDecompression::Failed(error) => {
                            tracing::error!(
                                key = sanitize_for_log(key).as_str(),
                                error = %error,
                                "LZ4 decompression failed for key; returning raw bytes"
                            );
                        }
                    }
                }
                Some(entry)
            }
            None => {
                inner.miss_count += 1;
                None
            }
        }
    }

    /// Returns `true` if the stored value for `key` is LZ4-compressed.
    pub fn is_compressed(&self, key: &str) -> Option<bool> {
        let inner = self.inner.lock().unwrap();
        inner.data.get(key).map(|e| e.compressed)
    }

    /// Compress or decompress the stored value for a specific key.
    /// Returns `Err` if the key is not found or decompression fails.
    pub fn set_key_compressed(&self, key: &str, compress: bool) -> Result<(), &'static str> {
        let mut inner = self.inner.lock().unwrap();

        // Read current state without holding a long-lived mutable borrow.
        let (current_compressed, current_value) = {
            let e = inner.data.get(key).ok_or("key not found")?;
            (e.compressed, e.value.clone())
        };

        match (compress, current_compressed) {
            (true, false) => {
                let new_value = compress_value(&current_value);
                inner.used_bytes =
                    replace_entry_size(inner.used_bytes, key, &current_value, &new_value);
                let e = inner.data.get_mut(key).unwrap();
                e.value = new_value;
                e.compressed = true;
            }
            (false, true) => {
                validate_manual_decompression(&current_value, inner.max_value_bytes)?;
                let new_value = decompress_value(&current_value)?;
                inner.used_bytes =
                    replace_entry_size(inner.used_bytes, key, &current_value, &new_value);
                let e = inner.data.get_mut(key).unwrap();
                e.value = new_value;
                e.compressed = false;
            }
            _ => {} // already in the desired state
        }
        Ok(())
    }

    /// Insert or update a key. Returns the new version.
    pub fn set(&self, key: String, value: Bytes, version: u64, ttl_secs: Option<u64>) -> u64 {
        let mut inner = self.inner.lock().unwrap();

        // Hard limit on value size to prevent unbounded memory allocation
        // (CWE-770, CWE-789 / CodeQL rust/uncontrolled-allocation-size).
        // The primary check (check_limits) lives in the caller, but enforcing
        // the limit here ensures that no call path — including cluster
        // replication — can cause compress_prepend_size() to allocate arbitrary
        // amounts of memory. When max_value_bytes == 0 (unlimited), fall back to
        // MAX_DECOMPRESSED_FALLBACK_BYTES so the protection is always active.
        let alloc_limit = if inner.max_value_bytes > 0 {
            inner.max_value_bytes
        } else {
            MAX_DECOMPRESSED_FALLBACK_BYTES
        };
        if value.len() as u64 > alloc_limit {
            return version;
        }
        // Explicitly rebind `value` to a slice bounded by alloc_limit so that
        // static analysis can verify all downstream allocations are bounded.
        // At this point value.len() <= alloc_limit is guaranteed by the guard
        // above, so this slice is always the full original value.
        let value = value.slice(..value.len().min(alloc_limit as usize));

        // Clamp user-provided TTL to a reasonable maximum to avoid unbounded lifetimes.
        let expires_at = expires_at_from_ttl_secs(ttl_secs, inner.default_ttl);

        // Compress if eligible.
        let (stored_value, is_compressed) = if inner.compression_enabled
            && value.len() as u64 > inner.compression_threshold_bytes
        {
            // Guard directly before the allocation with a compile-time
            // constant so static analysis can verify the bound (CWE-789).
            if value.len() > MAX_COMPRESS_INPUT_BYTES {
                return version;
            }
            let compressed = compress_value(&value);
            (compressed, true)
        } else {
            (value, false)
        };

        let sz = entry_size(&key, &stored_value);
        let entry = Entry {
            value: stored_value,
            version,
            expires_at,
            freq_count: 1,
            compressed: is_compressed,
        };

        inner.upsert_entry(key, entry, sz);

        version
    }

    pub fn delete(&self, key: &str) -> bool {
        let mut inner = self.inner.lock().unwrap();
        inner.remove_key(key).is_some()
    }

    pub fn flush(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.data.clear();
        inner.lfu = LfuTracker::new();
        inner.used_bytes = 0;
    }

    /// Return all keys matching an optional glob-style `*` pattern.
    pub fn keys(&self, pattern: Option<&str>) -> Vec<String> {
        let inner = self.inner.lock().unwrap();
        inner
            .data
            .keys()
            .filter(|k| match pattern {
                None => true,
                Some(p) => glob_match(p, k),
            })
            .cloned()
            .collect()
    }

    /// Sweep and remove all expired entries. Called by the TTL background task.
    pub fn sweep_expired(&self) -> usize {
        let mut inner = self.inner.lock().unwrap();
        let expired: Vec<String> = inner
            .data
            .iter()
            .filter(|(_, e)| e.is_expired())
            .map(|(k, _)| k.clone())
            .collect();
        let count = expired.len();
        for k in &expired {
            inner.remove_key(k);
        }
        count
    }

    // -----------------------------------------------------------------------
    // Export / Import
    // -----------------------------------------------------------------------

    /// Serialisable snapshot (converts Instant → unix timestamp ms).
    pub fn snapshot(&self) -> Vec<(String, ExportEntry)> {
        let inner = self.inner.lock().unwrap();
        inner
            .data
            .iter()
            .map(|(k, v)| (k.clone(), ExportEntry::from(v)))
            .collect()
    }

    /// Restore entries from an exported snapshot.
    pub fn restore(&self, entries: Vec<(String, ExportEntry)>) {
        for (key, exp) in entries {
            let expires_at = restore_expires_at(exp.expires_at_ms);
            let value = Bytes::from(exp.value);
            let sz = entry_size(&key, &value);
            let entry = Entry {
                value,
                version: exp.version,
                expires_at,
                freq_count: 1,
                compressed: false, // exported values are always decompressed
            };
            let mut inner = self.inner.lock().unwrap();
            inner.restore_entry(key, entry, sz);
        }
    }

    // -----------------------------------------------------------------------
    // Stats
    // -----------------------------------------------------------------------

    pub fn stats(&self) -> StoreStats {
        let inner = self.inner.lock().unwrap();
        StoreStats {
            key_count: inner.data.len() as u64,
            memory_used_bytes: inner.used_bytes,
            memory_max_bytes: inner.max_bytes,
            evictions: inner.evictions,
            hit_count: inner.hit_count,
            miss_count: inner.miss_count,
            value_size_limit_bytes: inner.max_value_bytes,
            max_keys_limit: inner.max_keys_limit as u64,
            compression_enabled: inner.compression_enabled,
            compression_threshold_bytes: inner.compression_threshold_bytes,
        }
    }

    /// Validate value-size and key-count limits before a write.
    /// Returns `Err(ErrorKind)` if a limit is violated; `Ok(())` otherwise.
    pub fn check_limits(&self, key: &str, value: &Bytes) -> Result<(), LimitError> {
        let inner = self.inner.lock().unwrap();
        if inner.max_value_bytes > 0 && value.len() as u64 > inner.max_value_bytes {
            return Err(LimitError::ValueTooLarge);
        }
        if inner.max_keys_limit > 0
            && !inner.data.contains_key(key)
            && inner.data.len() >= inner.max_keys_limit
        {
            return Err(LimitError::KeyLimitReached);
        }
        Ok(())
    }

    /// Update the memory cap at runtime (e.g. via `dittoctl node set max-memory`).
    /// If the new limit is smaller than the current usage, eviction runs on the
    /// next `set()` call; no immediate eviction is triggered here.
    pub fn set_max_memory_mb(&self, mb: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.max_bytes = mb * 1024 * 1024;
    }

    /// Update the default TTL at runtime (e.g. via `dittoctl node set default-ttl`).
    /// `secs == 0` means no TTL (keys live forever unless explicitly given one).
    pub fn set_default_ttl_secs(&self, secs: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.default_ttl = if secs > 0 {
            Some(Duration::from_secs(secs))
        } else {
            None
        };
    }

    pub fn set_value_size_limit(&self, bytes: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.max_value_bytes = bytes;
    }

    pub fn set_max_keys(&self, limit: usize) {
        let mut inner = self.inner.lock().unwrap();
        inner.max_keys_limit = limit;
    }

    pub fn set_compression_enabled(&self, enabled: bool) {
        let mut inner = self.inner.lock().unwrap();
        inner.compression_enabled = enabled;
    }

    /// Update `expires_at` for every key that matches `pattern` (glob `*` wildcard).
    /// `ttl_secs = None` removes the TTL (key lives forever).
    /// Returns the number of keys updated.
    pub fn set_ttl_by_pattern(&self, pattern: &str, ttl_secs: Option<u64>) -> usize {
        let mut inner = self.inner.lock().unwrap();
        let expires_at = expires_at_from_ttl_secs(ttl_secs, None);
        let mut count = 0usize;
        for (key, entry) in inner.data.iter_mut() {
            if !entry.is_expired() && glob_match(pattern, key) {
                entry.expires_at = expires_at;
                count += 1;
            }
        }
        count
    }

    /// Update compression threshold. Enforces minimum of 4096 bytes and
    /// only allows increasing (never decreasing) the threshold at runtime.
    pub fn set_compression_threshold(&self, bytes: u64) -> Result<u64, &'static str> {
        let mut inner = self.inner.lock().unwrap();
        let min_allowed = inner.compression_threshold_bytes.max(4096);
        if bytes < min_allowed {
            return Err("threshold can only be increased; minimum is 4096 bytes");
        }
        inner.compression_threshold_bytes = bytes;
        Ok(bytes)
    }
}

/// Errors returned by `KvStore::check_limits()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitError {
    ValueTooLarge,
    KeyLimitReached,
}

pub struct StoreStats {
    pub key_count: u64,
    pub memory_used_bytes: u64,
    pub memory_max_bytes: u64,
    pub evictions: u64,
    pub hit_count: u64,
    pub miss_count: u64,
    pub value_size_limit_bytes: u64,
    pub max_keys_limit: u64,
    pub compression_enabled: bool,
    pub compression_threshold_bytes: u64,
}

impl StoreInner {
    fn upsert_entry(&mut self, key: String, entry: Entry, size: u64) {
        let existed = self.data.contains_key(&key);
        self.remove_key(&key);
        self.ensure_capacity(size);

        if existed {
            self.lfu.touch(&key);
        } else {
            self.lfu.insert(&key);
        }
        self.data.insert(key, entry);
        self.used_bytes += size;
    }

    fn restore_entry(&mut self, key: String, entry: Entry, size: u64) {
        self.remove_key(&key);
        self.ensure_capacity(size);
        self.lfu.insert(&key);
        self.data.insert(key, entry);
        self.used_bytes += size;
    }

    fn remove_key(&mut self, key: &str) -> Option<Entry> {
        let removed = self.data.remove(key);
        if let Some(entry) = &removed {
            self.used_bytes = remove_entry_size(self.used_bytes, key, &entry.value);
        }
        self.lfu.remove(key);
        removed
    }

    fn ensure_capacity(&mut self, needed: u64) {
        while self.used_bytes + needed > self.max_bytes && !self.lfu.is_empty() {
            if let Some(victim) = self.lfu.evict() {
                if let Some(entry) = self.data.remove(&victim) {
                    self.used_bytes = remove_entry_size(self.used_bytes, &victim, &entry.value);
                    self.evictions += 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Entry, ExportEntry, KvStore, LimitError};
    use crate::store::capacity::entry_size;
    use crate::store::compression::sanitize_for_log;
    use crate::store::retention::MAX_TTL_SECS;
    use bytes::Bytes;
    use lz4_flex::compress_prepend_size;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    #[test]
    fn set_ttl_by_pattern_clamps_extreme_ttl() {
        let store = KvStore::new(1, 0, 1024, 10, false, 4096);
        store.set(
            "tenant::key".to_string(),
            Bytes::from_static(b"value"),
            1,
            None,
        );

        assert_eq!(store.set_ttl_by_pattern("tenant::*", Some(u64::MAX)), 1);

        let ttl = store
            .get("tenant::key")
            .and_then(|entry| entry.ttl_remaining_secs())
            .expect("ttl should be set");
        assert!(ttl <= MAX_TTL_SECS);
        assert!(ttl >= MAX_TTL_SECS.saturating_sub(1));
    }

    #[test]
    fn limits_runtime_setters_and_stats_track_hits_misses_and_limits() {
        let store = KvStore::new(1, 0, 4, 1, false, 4096);

        assert_eq!(
            store.check_limits("oversized", &Bytes::from_static(b"12345")),
            Err(LimitError::ValueTooLarge)
        );
        store.set("alpha".to_string(), Bytes::from_static(b"one"), 1, None);
        assert_eq!(
            store.check_limits("beta", &Bytes::from_static(b"two")),
            Err(LimitError::KeyLimitReached)
        );
        assert!(store
            .check_limits("alpha", &Bytes::from_static(b"two"))
            .is_ok());

        assert!(store.get("alpha").is_some());
        assert!(store.get("missing").is_none());
        let stats = store.stats();
        assert_eq!(stats.key_count, 1);
        assert_eq!(stats.hit_count, 1);
        assert_eq!(stats.miss_count, 1);
        assert_eq!(stats.value_size_limit_bytes, 4);
        assert_eq!(stats.max_keys_limit, 1);

        store.set_value_size_limit(8);
        store.set_max_keys(2);
        store.set_compression_enabled(true);
        store.set_compression_threshold(4096).unwrap();
        let stats = store.stats();
        assert_eq!(stats.value_size_limit_bytes, 8);
        assert_eq!(stats.max_keys_limit, 2);
        assert!(stats.compression_enabled);
        assert_eq!(stats.compression_threshold_bytes, 4096);
    }

    #[test]
    fn compression_paths_decompress_for_read_and_manual_toggles_are_idempotent() {
        let store = KvStore::new(1, 0, 1024, 10, true, 4);
        let value = Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        store.set("compressed".to_string(), value.clone(), 7, None);

        assert_eq!(store.is_compressed("compressed"), Some(true));
        let read = store
            .get("compressed")
            .expect("compressed key should exist");
        assert_eq!(read.value, value);
        assert!(!read.compressed);
        assert_eq!(store.is_compressed("compressed"), Some(true));

        store
            .set_key_compressed("compressed", false)
            .expect("manual decompression should work");
        assert_eq!(store.is_compressed("compressed"), Some(false));
        store
            .set_key_compressed("compressed", false)
            .expect("already decompressed is ok");

        store
            .set_key_compressed("compressed", true)
            .expect("manual compression should work");
        assert_eq!(store.is_compressed("compressed"), Some(true));
        store
            .set_key_compressed("compressed", true)
            .expect("already compressed is ok");
        assert_eq!(
            store.set_key_compressed("missing", true),
            Err("key not found")
        );
    }

    #[test]
    fn decompression_guards_return_raw_bytes_or_errors_for_invalid_payloads() {
        let store = KvStore::new(1, 0, 8, 10, false, 4096);
        let valid_large = Bytes::from(compress_prepend_size(b"this is too large"));
        {
            let mut inner = store.inner.lock().unwrap();
            inner.data.insert(
                "bomb".to_string(),
                Entry {
                    value: valid_large.clone(),
                    version: 1,
                    expires_at: None,
                    freq_count: 1,
                    compressed: true,
                },
            );
            inner.lfu.insert("bomb");
            inner.used_bytes += entry_size("bomb", &valid_large);
        }

        let guarded = store.get("bomb").expect("bomb key should exist");
        assert_eq!(guarded.value, valid_large);
        assert!(guarded.compressed);
        assert_eq!(
            store.set_key_compressed("bomb", false),
            Err("decompressed size exceeds limit")
        );

        let mut invalid_payload = 3u32.to_le_bytes().to_vec();
        invalid_payload.extend_from_slice(b"bad-lz4");
        let invalid = Bytes::from(invalid_payload);
        {
            let mut inner = store.inner.lock().unwrap();
            inner.data.insert(
                "invalid\nkey".to_string(),
                Entry {
                    value: invalid.clone(),
                    version: 2,
                    expires_at: None,
                    freq_count: 1,
                    compressed: true,
                },
            );
            inner.lfu.insert("invalid\nkey");
            inner.used_bytes += entry_size("invalid\nkey", &invalid);
        }

        let raw = store.get("invalid\nkey").expect("invalid key should exist");
        assert_eq!(raw.value, invalid);
        assert!(raw.compressed);
        assert_eq!(
            store.set_key_compressed("invalid\nkey", false),
            Err("decompression failed")
        );
        assert_eq!(sanitize_for_log("bad\nkey\rname"), "badkeyname");
    }

    #[test]
    fn keys_delete_flush_expiry_and_glob_matching_cover_store_lifecycle() {
        let store = KvStore::new(1, 0, 1024, 10, false, 4096);
        store.set("tenant:a:1".to_string(), Bytes::from_static(b"a"), 1, None);
        store.set("tenant:a:2".to_string(), Bytes::from_static(b"b"), 2, None);
        store.set(
            "tenant:b:1".to_string(),
            Bytes::from_static(b"c"),
            3,
            Some(1),
        );

        assert_eq!(
            sorted(store.keys(Some("tenant:a:*"))),
            vec!["tenant:a:1".to_string(), "tenant:a:2".to_string()]
        );
        assert_eq!(
            sorted(store.keys(Some("*:b:*"))),
            vec!["tenant:b:1".to_string()]
        );
        assert_eq!(
            sorted(store.keys(Some("tenant:*:2"))),
            vec!["tenant:a:2".to_string()]
        );
        assert!(store.keys(Some("tenant:c:*")).is_empty());

        assert!(store.delete("tenant:a:1"));
        assert!(!store.delete("tenant:a:1"));
        assert!(store.get("tenant:a:1").is_none());

        std::thread::sleep(Duration::from_millis(1100));
        assert!(store.get("tenant:b:1").is_none());
        let stats = store.stats();
        assert_eq!(stats.key_count, 1);
        assert!(stats.miss_count >= 2);

        store.flush();
        let stats = store.stats();
        assert_eq!(stats.key_count, 0);
        assert_eq!(stats.memory_used_bytes, 0);
    }

    #[test]
    fn snapshot_restore_default_ttl_and_pattern_ttl_skip_expired_entries() {
        let store = KvStore::new(1, 1, 1024, 10, false, 4096);
        store.set("default-ttl".to_string(), Bytes::from_static(b"v"), 1, None);
        let ttl = store
            .get("default-ttl")
            .and_then(|entry| entry.ttl_remaining_secs())
            .expect("default ttl should apply");
        assert!(ttl <= 1);

        store.set_default_ttl_secs(0);
        store.set("no-ttl".to_string(), Bytes::from_static(b"v"), 2, None);
        assert!(store
            .get("no-ttl")
            .expect("key should exist")
            .ttl_remaining_secs()
            .is_none());

        let future_ms = (SystemTime::now() + Duration::from_secs(60))
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let past_ms = (SystemTime::now() - Duration::from_secs(60))
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let restored = KvStore::new(1, 0, 1024, 10, false, 4096);
        restored.restore(vec![
            (
                "future".to_string(),
                ExportEntry {
                    value: b"future".to_vec(),
                    version: 3,
                    expires_at_ms: Some(future_ms),
                },
            ),
            (
                "past".to_string(),
                ExportEntry {
                    value: b"past".to_vec(),
                    version: 4,
                    expires_at_ms: Some(past_ms),
                },
            ),
        ]);

        assert_eq!(
            restored.get("future").unwrap().value,
            Bytes::from_static(b"future")
        );
        assert!(restored
            .get("past")
            .expect("past restore timestamp becomes non-expiring")
            .ttl_remaining_secs()
            .is_none());

        restored.set(
            "expired".to_string(),
            Bytes::from_static(b"expired"),
            5,
            Some(1),
        );
        std::thread::sleep(Duration::from_millis(1100));
        assert_eq!(restored.set_ttl_by_pattern("*", Some(5)), 2);
        assert!(restored.get("expired").is_none());

        let snapshot = restored.snapshot();
        let future = snapshot
            .iter()
            .find(|(key, _)| key == "future")
            .expect("future entry should be exported");
        assert_eq!(future.1.value, b"future");
        assert_eq!(future.1.version, 3);
        assert!(future.1.expires_at_ms.is_some());
    }

    #[test]
    fn memory_cap_eviction_prefers_least_frequently_used_existing_key() {
        let store = KvStore::new(1, 0, 1024, 10, false, 4096);
        store.set("hot".to_string(), Bytes::from_static(b"hot"), 1, None);
        store.set("cold".to_string(), Bytes::from_static(b"cold"), 2, None);
        assert!(store.get("hot").is_some());

        {
            let mut inner = store.inner.lock().unwrap();
            inner.max_bytes = entry_size("hot", &Bytes::from_static(b"hot"))
                + entry_size("new", &Bytes::from_static(b"new"))
                + 5;
        }

        assert!(store.get("hot").is_some());
        assert!(store.get("cold").is_some());
        store.set("new".to_string(), Bytes::from_static(b"new"), 3, None);

        assert!(store.get("hot").is_some());
        assert!(store.get("cold").is_none());
        assert!(store.get("new").is_some());
        assert!(store.stats().evictions >= 1);
    }

    #[test]
    fn compression_threshold_only_increases_from_minimum() {
        let store = KvStore::new(1, 0, 1024, 10, false, 1024);

        assert_eq!(
            store.set_compression_threshold(2048),
            Err("threshold can only be increased; minimum is 4096 bytes")
        );
        assert_eq!(store.set_compression_threshold(4096), Ok(4096));
        assert_eq!(
            store.set_compression_threshold(4095),
            Err("threshold can only be increased; minimum is 4096 bytes")
        );
        assert_eq!(store.set_compression_threshold(8192), Ok(8192));
        assert_eq!(store.stats().compression_threshold_bytes, 8192);
    }

    #[test]
    fn export_entry_handles_corrupt_or_oversized_compressed_payloads() {
        let corrupt = Entry {
            value: Bytes::from_static(b"bad-lz4"),
            version: 1,
            expires_at: Some(Instant::now() - Duration::from_secs(1)),
            freq_count: 9,
            compressed: true,
        };
        let exported = ExportEntry::from(&corrupt);
        assert_eq!(exported.value, b"bad-lz4");
        assert_eq!(exported.version, 1);
        assert!(exported.expires_at_ms.is_some());

        let mut oversized = (u32::MAX).to_le_bytes().to_vec();
        oversized.extend_from_slice(b"payload");
        let entry = Entry {
            value: Bytes::from(oversized.clone()),
            version: 2,
            expires_at: None,
            freq_count: 1,
            compressed: true,
        };
        let exported = ExportEntry::from(&entry);
        assert_eq!(exported.value, oversized);
        assert_eq!(entry.freq_count, 1);
    }

    fn sorted(keys: Vec<String>) -> Vec<String> {
        let mut keys = keys;
        keys.sort();
        keys
    }
}
