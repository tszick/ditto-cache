use super::NodeHandle;
use crate::store::kv_store::sanitize_for_log;
use bytes::Bytes;

impl NodeHandle {
    pub(super) fn apply_locally(
        &self,
        key: &str,
        value: Option<Bytes>,
        ttl_secs: Option<u64>,
        log_index: u64,
    ) -> u64 {
        match value {
            Some(value) => {
                // Replication traffic must obey the same store limits as client writes.
                if let Err(error) = self.store.check_limits(key, &value) {
                    tracing::warn!(
                        key = sanitize_for_log(key).as_str(),
                        value_bytes = value.len(),
                        log_index,
                        error = ?error,
                        "apply_locally: skipping entry that violates store limits received via cluster replication"
                    );
                    return log_index;
                }
                let watch_value = value.clone();
                let version = self.store.set(key.to_string(), value, log_index, ttl_secs);
                let _ = self
                    .watch_tx
                    .send((key.to_string(), Some(watch_value), version));
                version
            }
            None => {
                self.store.delete(key);
                let _ = self.watch_tx.send((key.to_string(), None, log_index));
                log_index
            }
        }
    }
}
