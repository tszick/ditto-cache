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

#[cfg(test)]
mod tests {
    use super::start_ttl_sweep;
    use crate::store::kv_store::KvStore;
    use bytes::Bytes;
    use std::{sync::Arc, time::Duration};

    #[tokio::test]
    async fn ttl_sweep_removes_expired_keys_and_clamps_zero_interval() {
        let store = Arc::new(KvStore::new(1, 0, 1024, 10, false, 4096));
        store.set(
            "expired".to_string(),
            Bytes::from_static(b"value"),
            1,
            Some(1),
        );
        store.set("live".to_string(), Bytes::from_static(b"value"), 2, None);

        tokio::time::sleep(Duration::from_millis(1100)).await;
        start_ttl_sweep(store.clone(), 0);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let keys = store.keys(None);
                if !keys.contains(&"expired".to_string()) && keys.contains(&"live".to_string()) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("ttl sweep should remove expired key");
    }
}
