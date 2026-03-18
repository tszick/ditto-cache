use crate::store::kv_store::KvStore;
use std::{sync::Arc, time::Duration};
use tracing::debug;

/// Spawns a background task that periodically sweeps expired keys.
pub fn start_ttl_sweep(store: Arc<KvStore>, interval_secs: u64) {
    let interval = Duration::from_secs(interval_secs.max(1));
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            let removed = store.sweep_expired();
            if removed > 0 {
                debug!("TTL sweep removed {} expired keys", removed);
            }
        }
    });
}
