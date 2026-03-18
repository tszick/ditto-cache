use std::collections::HashMap;

/// O(1) LFU eviction tracker.
///
/// Tracks access frequency per key and lets the caller evict the
/// least-frequently-used key in O(1).  When multiple keys share the
/// minimum frequency the *oldest* one (first inserted at that frequency)
/// is chosen.
pub struct LfuTracker {
    /// key → frequency
    freq:     HashMap<String, u64>,
    /// frequency → ordered set of keys (insertion order via Vec)
    buckets:  HashMap<u64, Vec<String>>,
    min_freq: u64,
}

impl LfuTracker {
    pub fn new() -> Self {
        Self {
            freq:     HashMap::new(),
            buckets:  HashMap::new(),
            min_freq: 0,
        }
    }

    /// Register a new key (freq = 1).
    pub fn insert(&mut self, key: &str) {
        self.freq.insert(key.to_string(), 1);
        self.buckets.entry(1).or_default().push(key.to_string());
        // A new key may be the new minimum if the store was non-empty.
        self.min_freq = 1;
    }

    /// Record an access; increments the key's frequency.
    pub fn touch(&mut self, key: &str) {
        if let Some(f) = self.freq.get_mut(key) {
            let old = *f;
            *f = old + 1;

            // Remove from old bucket.
            if let Some(bucket) = self.buckets.get_mut(&old) {
                bucket.retain(|k| k != key);
                if bucket.is_empty() {
                    self.buckets.remove(&old);
                    if self.min_freq == old {
                        self.min_freq = old + 1;
                    }
                }
            }

            // Add to new bucket.
            self.buckets.entry(old + 1).or_default().push(key.to_string());
        }
    }

    /// Remove a key entirely (e.g. on DELETE).
    pub fn remove(&mut self, key: &str) {
        if let Some(f) = self.freq.remove(key) {
            if let Some(bucket) = self.buckets.get_mut(&f) {
                bucket.retain(|k| k != key);
                if bucket.is_empty() {
                    self.buckets.remove(&f);
                    // Recalculate min_freq if necessary.
                    if self.min_freq == f {
                        self.min_freq = self.freq.values().copied().min().unwrap_or(0);
                    }
                }
            }
        }
    }

    /// Return the key to evict (least frequent, oldest at that frequency).
    pub fn evict_candidate(&self) -> Option<&str> {
        self.buckets
            .get(&self.min_freq)
            .and_then(|b| b.first())
            .map(|s| s.as_str())
    }

    /// Evict and return the evicted key.
    pub fn evict(&mut self) -> Option<String> {
        let candidate = self.evict_candidate()?.to_string();
        self.remove(&candidate);
        Some(candidate)
    }

    pub fn len(&self) -> usize {
        self.freq.len()
    }

    pub fn is_empty(&self) -> bool {
        self.freq.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evicts_least_frequent() {
        let mut lfu = LfuTracker::new();
        lfu.insert("a");
        lfu.insert("b");
        lfu.insert("c");
        lfu.touch("b");
        lfu.touch("b");
        lfu.touch("c");

        // "a" has freq=1, should be evicted first.
        assert_eq!(lfu.evict(), Some("a".to_string()));
        // "c" has freq=2, "b" has freq=3 → evict "c".
        assert_eq!(lfu.evict(), Some("c".to_string()));
        assert_eq!(lfu.evict(), Some("b".to_string()));
        assert_eq!(lfu.evict(), None);
    }
}
