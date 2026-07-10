use super::{ClientRequestSource, NodeHandle};
use ditto_protocol::{ClusterMessage, NodeStatus};
use std::sync::Arc;

impl NodeHandle {
    /// Handles inbound cluster protocol messages and returns replies when required.
    pub async fn handle_cluster(self: Arc<Self>, msg: ClusterMessage) -> Option<ClusterMessage> {
        match msg {
            ClusterMessage::Prepare {
                log_index,
                key,
                value,
                ttl_secs,
            } => {
                self.write_log
                    .lock()
                    .await
                    .append_at(log_index, key, value, ttl_secs);
                Some(ClusterMessage::PrepareAck {
                    log_index,
                    node_id: self.id,
                })
            }
            ClusterMessage::Commit { log_index } => {
                let entry_data = self.write_log.lock().await.get_entry(log_index);
                if let Some((key, value, ttl)) = entry_data {
                    self.apply_locally(&key, value, ttl, log_index);
                    self.write_log.lock().await.commit(log_index);
                }
                self.active_set.lock().await.set_local_applied(log_index);
                Some(ClusterMessage::CommitAck {
                    log_index,
                    node_id: self.id,
                })
            }
            ClusterMessage::Forward { request, .. } => {
                let response = self
                    .handle_client_with_source(request, ClientRequestSource::Internal)
                    .await;
                Some(ClusterMessage::ForwardResponse(response))
            }
            ClusterMessage::RequestLog { from_index } => {
                let entries = self.write_log.lock().await.entries_since(from_index);
                Some(ClusterMessage::LogEntries { entries })
            }
            ClusterMessage::LogEntries { entries } => {
                for entry in entries {
                    self.apply_locally(
                        &entry.key,
                        entry.value.clone(),
                        entry.ttl_secs,
                        entry.index,
                    );
                    self.write_log.lock().await.commit(entry.index);
                    self.active_set.lock().await.set_local_applied(entry.index);
                }
                None
            }
            ClusterMessage::Synced {
                node_id,
                last_applied,
            } => {
                let mut set = self.active_set.lock().await;
                if let Some(mut info) = set.snapshot().into_iter().find(|node| node.id == node_id) {
                    info.status = NodeStatus::Active;
                    info.last_applied = last_applied;
                    set.upsert(info);
                }
                None
            }
            ClusterMessage::ForcePrimary { node_id } => {
                self.active_set.lock().await.set_pinned_primary(node_id);
                tracing::info!("Primary pinned to {} by ForcePrimary broadcast", node_id);
                None
            }
            ClusterMessage::Admin(admin_request) => {
                let response = Arc::clone(&self).handle_admin(admin_request).await;
                Some(ClusterMessage::AdminResponse(Box::new(response)))
            }
            _ => None,
        }
    }
}
