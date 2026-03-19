use crate::store::lfu::LfuTracker;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use lz4_flex::{compress_prepend_size, decompress_size_prepended};

/// Maximum allowed TTL for any entry, in seconds (e.g. 30 days).
const MAX_TTL_SECS: u64 = 30 * 24 * 60 * 60;

#[derive(Debug, Clone)]
pub struct Entry {
    /// Raw stored bytes (may be LZ4-compressed; see `compressed` flag).
    pub value:      Bytes,
    pub version:    u64,
    pub expires_at: Option<Instant>,
    pub freq_count: u64,
    /// Whether `value` is stored in LZ4-compressed form.
    pub compressed: bool,
}

impl Entry {
    pub fn is_expired(&self) -> bool {
        self.expires_at.map(|e| Instant::now() >= e).unwrap_or(false)
    }

    /// Remaining TTL in seconds, if set.
    pub fn ttl_remaining_secs(&self) -> Option<u64> {
        self.expires_at.map(|e| {
            let now = Instant::now();
            if now >= e { 0 } else { (e - now).as_secs() }
        })
    }
}

/// Serialisable snapshot of a single entry (no Instant).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportEntry {
    pub value:           Vec<u8>,
    pub version:         u64,
    /// Unix timestamp (ms) of expiry, if set.
    pub expires_at_ms:   Option<u64>,
}

impl From<&Entry> for ExportEntry {
    fn from(e: &Entry) -> Self {
        let expires_at_ms = e.expires_at.map(|instant| {
            let now_instant = Instant::now();
            let now_system  = SystemTime::now();
            let remaining   = if instant > now_instant { instant - now_instant } else { Duration::ZERO };
            let expiry_sys  = now_system + remaining;
            expiry_sys
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64
        });
        // Always export decompressed values for portability.
        let raw_value = if e.compressed {
            decompress_size_prepended(&e.value)
                .unwrap_or_else(|_| e.value.to_vec())
        } else {
            e.value.to_vec()
        };
        ExportEntry {
            value:         raw_value,
            version:       e.version,
            expires_at_ms,
        }
    }
}

pub struct KvStore {
    inner: Arc<Mutex<StoreInner>>,
}

struct StoreInner {
    data:          HashMap<String, Entry>,
    lfu:           LfuTracker,
    max_bytes:     u64,
    used_bytes:    u64,
    default_ttl:   Option<Duration>,
    evictions:     u64,
    hit_count:     u64,
    miss_count:    u64,
    /// Maximum allowed value size in bytes; 0 = unlimited.
    max_value_bytes:         u64,
    /// Maximum number of keys; 0 = unlimited.
    max_keys_limit:          usize,
    /// LZ4 compression enabled (logic added in Feature 3).
    compression_enabled:     bool,
    /// Values larger than this threshold are compressed.
    compression_threshold_bytes: u64,
}

impl KvStore {
    pub fn new(
        max_memory_mb:           u64,
        default_ttl_secs:        u64,
        max_value_bytes:         u64,
        max_keys_limit:          usize,
        compression_enabled:     bool,
        compression_threshold_bytes: u64,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(StoreInner {
                data:        HashMap::new(),
                lfu:         LfuTracker::new(),
                max_bytes:   max_memory_mb * 1024 * 1024,
                used_bytes:  0,
                default_ttl: if default_ttl_secs > 0 {
                    Some(Duration::from_secs(default_ttl_secs))
                } else {
                    None
                },
                evictions:   0,
                hit_count:   0,
                miss_count:  0,
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
                let sz = entry_size(key, &entry.value);
                inner.data.remove(key);
                inner.lfu.remove(key);
                inner.used_bytes = inner.used_bytes.saturating_sub(sz);
                inner.miss_count += 1;
                None
            }
            Some(mut entry) => {
                inner.lfu.touch(key);
                inner.hit_count += 1;
                // Transparently decompress before returning.
                if entry.compressed {
                    match decompress_size_prepended(&entry.value) {
                        Ok(decompressed) => {
                            entry.value      = Bytes::from(decompressed);
                            entry.compressed = false;
                        }
                        Err(e) => {
                            tracing::error!(
                                "LZ4 decompression failed for key '{}': {}; returning raw bytes",
                                key, e
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
                let new_value = Bytes::from(compress_prepend_size(&current_value));
                let old_sz    = entry_size(key, &current_value);
                let new_sz    = entry_size(key, &new_value);
                inner.used_bytes = inner.used_bytes.saturating_sub(old_sz).saturating_add(new_sz);
                let e = inner.data.get_mut(key).unwrap();
                e.value      = new_value;
                e.compressed = true;
            }
            (false, true) => {
                let decompressed = decompress_size_prepended(&current_value)
                    .map_err(|_| "decompression failed")?;
                let new_value = Bytes::from(decompressed);
                let old_sz    = entry_size(key, &current_value);
                let new_sz    = entry_size(key, &new_value);
                inner.used_bytes = inner.used_bytes.saturating_sub(old_sz).saturating_add(new_sz);
                let e = inner.data.get_mut(key).unwrap();
                e.value      = new_value;
                e.compressed = false;
            }
            _ => {} // already in the desired state
        }
        Ok(())
    }

    /// Insert or update a key. Returns the new version.
    pub fn set(&self, key: String, value: Bytes, version: u64, ttl_secs: Option<u64>) -> u64 {
        let mut inner = self.inner.lock().unwrap();

        // Remove old size if key exists.
        if let Some(old) = inner.data.get(&key) {
            let old_sz = entry_size(&key, &old.value);
            inner.used_bytes = inner.used_bytes.saturating_sub(old_sz);
        }

        // Clamp user-provided TTL to a reasonable maximum to avoid unbounded lifetimes.
        let ttl_secs = ttl_secs.map(|t| t.min(MAX_TTL_SECS));

        let expires_at = ttl_secs
            .map(Duration::from_secs)
            .or(inner.default_ttl)
            .map(|d| Instant::now() + d);

        // Compress if eligible.
        let (stored_value, is_compressed) =
            if inner.compression_enabled
                && value.len() as u64 > inner.compression_threshold_bytes
            {
                let compressed = Bytes::from(compress_prepend_size(&value));
                (compressed, true)
            } else {
                (value, false)
            };

        let sz    = entry_size(&key, &stored_value);
        let entry = Entry {
            value:      stored_value,
            version,
            expires_at,
            freq_count: 1,
            compressed: is_compressed,
        };

        // Evict if needed.
        inner.ensure_capacity(sz);

        if inner.data.contains_key(&key) {
            inner.lfu.touch(&key);
        } else {
            inner.lfu.insert(&key);
        }
        inner.data.insert(key, entry);
        inner.used_bytes += sz;

        version
    }

    pub fn delete(&self, key: &str) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if let Some(entry) = inner.data.remove(key) {
            let sz = entry_size(key, &entry.value);
            inner.used_bytes = inner.used_bytes.saturating_sub(sz);
            inner.lfu.remove(key);
            true
        } else {
            false
        }
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
                None    => true,
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
            if let Some(e) = inner.data.remove(k) {
                inner.used_bytes = inner.used_bytes.saturating_sub(entry_size(k, &e.value));
            }
            inner.lfu.remove(k);
        }
        count
    }

    // -----------------------------------------------------------------------
    // Export / Import
    // -----------------------------------------------------------------------

    /// Serialisable snapshot (converts Instant → unix timestamp ms).
    pub fn snapshot(&self) -> Vec<(String, ExportEntry)> {
        let inner = self.inner.lock().unwrap();
        inner.data.iter()
            .map(|(k, v)| (k.clone(), ExportEntry::from(v)))
            .collect()
    }

    /// Restore entries from an exported snapshot.
    pub fn restore(&self, entries: Vec<(String, ExportEntry)>) {
        let now_sys  = SystemTime::now();
        let now_inst = Instant::now();
        for (key, exp) in entries {
            let expires_at = exp.expires_at_ms.and_then(|ms| {
                let expiry = UNIX_EPOCH + Duration::from_millis(ms);
                expiry.duration_since(now_sys).ok().map(|d| now_inst + d)
            });
            let value = Bytes::from(exp.value);
            let sz    = entry_size(&key, &value);
            let mut inner = self.inner.lock().unwrap();
            inner.ensure_capacity(sz);
            inner.lfu.insert(&key);
            inner.data.insert(key, Entry {
                value,
                version:    exp.version,
                expires_at,
                freq_count: 1,
                compressed: false, // exported values are always decompressed
            });
            inner.used_bytes += sz;
        }
    }

    // -----------------------------------------------------------------------
    // Stats
    // -----------------------------------------------------------------------

    pub fn stats(&self) -> StoreStats {
        let inner = self.inner.lock().unwrap();
        StoreStats {
            key_count:               inner.data.len() as u64,
            memory_used_bytes:       inner.used_bytes,
            memory_max_bytes:        inner.max_bytes,
            evictions:               inner.evictions,
            hit_count:               inner.hit_count,
            miss_count:              inner.miss_count,
            value_size_limit_bytes:  inner.max_value_bytes,
            max_keys_limit:          inner.max_keys_limit as u64,
            compression_enabled:     inner.compression_enabled,
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
        inner.default_ttl = if secs > 0 { Some(Duration::from_secs(secs)) } else { None };
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
        let expires_at = ttl_secs.map(|s| Instant::now() + Duration::from_secs(s));
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
    pub key_count:               u64,
    pub memory_used_bytes:       u64,
    pub memory_max_bytes:        u64,
    pub evictions:               u64,
    pub hit_count:               u64,
    pub miss_count:              u64,
    pub value_size_limit_bytes:  u64,
    pub max_keys_limit:          u64,
    pub compression_enabled:     bool,
    pub compression_threshold_bytes: u64,
}

// -----------------------------------------------------------------------
// StoreInner helpers
// -----------------------------------------------------------------------

impl StoreInner {
    fn ensure_capacity(&mut self, needed: u64) {
        while self.used_bytes + needed > self.max_bytes && !self.lfu.is_empty() {
            if let Some(victim) = self.lfu.evict() {
                if let Some(e) = self.data.remove(&victim) {
                    self.used_bytes = self.used_bytes.saturating_sub(entry_size(&victim, &e.value));
                    self.evictions += 1;
                }
            }
        }
    }
}

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

fn entry_size(key: &str, value: &Bytes) -> u64 {
    (key.len() + value.len() + 64) as u64
}

/// Minimal glob matching: supports `*` as wildcard.
fn glob_match(pattern: &str, s: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == s;
    }
    let mut pos = 0usize;
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if i == 0 {
            if !s.starts_with(part) {
                return false;
            }
            pos = part.len();
        } else if i == parts.len() - 1 {
            if !s[pos..].ends_with(part) {
                return false;
            }
        } else {
            match s[pos..].find(part) {
                Some(idx) => pos += idx + part.len(),
                None      => return false,
            }
        }
    }
    true
}
