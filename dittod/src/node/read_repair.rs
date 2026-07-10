use super::NodeHandle;
use crate::store::kv_store::sanitize_for_log;
use ditto_protocol::{ClientRequest, ClientResponse, NodeStatus};
use std::sync::{atomic::Ordering, Arc};

impl NodeHandle {
    pub(super) async fn maybe_read_repair_on_miss(
        self: &Arc<Self>,
        namespace: Option<String>,
        key: String,
    ) -> Option<ClientResponse> {
        let (enabled, min_interval_ms, max_per_minute) = {
            let cfg = self.config.lock().unwrap();
            (
                cfg.replication.read_repair_on_miss_enabled,
                cfg.replication.read_repair_min_interval_ms.max(1),
                cfg.replication.read_repair_max_per_minute,
            )
        };
        if !enabled {
            return None;
        }

        let (status, am_primary) = {
            let set = self.active_set.lock().await;
            (set.local_status(), set.is_primary())
        };
        if status != NodeStatus::Active || am_primary {
            return None;
        }

        let now_ms = Self::now_millis();
        if !self.try_consume_read_repair_budget(now_ms, max_per_minute) {
            self.read_repair_budget_exhausted_total
                .fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let last_ms = self.last_read_repair_trigger_ms.load(Ordering::Relaxed);
        if now_ms < last_ms.saturating_add(min_interval_ms) {
            self.read_repair_throttled_total
                .fetch_add(1, Ordering::Relaxed);
            return None;
        }
        self.last_read_repair_trigger_ms
            .store(now_ms, Ordering::Relaxed);
        self.read_repair_trigger_total
            .fetch_add(1, Ordering::Relaxed);

        match self
            .forward_request_to_primary(ClientRequest::Get {
                key: key.clone(),
                namespace,
            })
            .await
        {
            v @ ClientResponse::Value { .. } => {
                self.read_repair_success_total
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    "Read-repair: key '{}' found on primary but missing locally; scheduling resync",
                    sanitize_for_log(&key),
                );
                tokio::spawn(Arc::clone(self).run_resync());
                Some(v)
            }
            ClientResponse::NotFound => Some(ClientResponse::NotFound),
            other => {
                tracing::warn!(
                    "Read-repair: primary query for '{}' returned non-repairable response: {:?}",
                    sanitize_for_log(&key),
                    other
                );
                None
            }
        }
    }

    pub(super) fn try_consume_read_repair_budget(&self, now_ms: u64, max_per_minute: u64) -> bool {
        if max_per_minute == 0 {
            return true;
        }

        let window_ms = 60_000;
        let start_ms = self
            .read_repair_budget_window_start_ms
            .load(Ordering::Relaxed);
        if start_ms == 0 || now_ms >= start_ms.saturating_add(window_ms) {
            self.read_repair_budget_window_start_ms
                .store(now_ms, Ordering::Relaxed);
            self.read_repair_budget_window_count
                .store(1, Ordering::Relaxed);
            return true;
        }

        let prev = self
            .read_repair_budget_window_count
            .fetch_add(1, Ordering::Relaxed);
        prev < max_per_minute
    }
}
