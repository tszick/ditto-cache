mod anti_entropy;
mod recovery;

use super::{NodeHandle, PROTOCOL_VERSION};
use crate::network::cluster_server::send_cluster;
use ditto_protocol::{AdminRequest, ClusterMessage, NodeStatus};
use std::{
    net::SocketAddr,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

pub use anti_entropy::start_anti_entropy;
pub use recovery::{run_recovery, run_resync};

pub(super) async fn committed_index(node: &Arc<NodeHandle>) -> u64 {
    node.write_log.lock().await.committed_index()
}

pub(super) async fn mark_active_and_get_index(node: &Arc<NodeHandle>) -> u64 {
    node.active_set
        .lock()
        .await
        .set_local_status(NodeStatus::Active);
    committed_index(node).await
}

pub(super) async fn set_syncing(node: &Arc<NodeHandle>) {
    node.active_set
        .lock()
        .await
        .set_local_status(NodeStatus::Syncing);
}

pub(super) async fn set_inactive(node: &Arc<NodeHandle>) {
    node.active.store(false, Ordering::Relaxed);
    node.active_set
        .lock()
        .await
        .set_local_status(NodeStatus::Inactive);
}

pub(super) async fn sync_from_peer(
    node: &Arc<NodeHandle>,
    peer_addr: SocketAddr,
    from_index: u64,
) -> anyhow::Result<usize> {
    let req = ClusterMessage::RequestLog { from_index };
    match send_cluster(peer_addr, &req, node.tls_connector.as_ref()).await? {
        Some(ClusterMessage::LogEntries { entries }) => Ok(apply_log_entries(node, entries).await),
        _ => Ok(0),
    }
}

async fn apply_log_entries(
    node: &Arc<NodeHandle>,
    entries: Vec<ditto_protocol::LogEntry>,
) -> usize {
    let count = entries.len();
    for entry in entries {
        node.apply_locally(&entry.key, entry.value.clone(), entry.ttl_secs, entry.index);
        node.write_log.lock().await.commit(entry.index);
        node.active_set.lock().await.set_local_applied(entry.index);
    }
    count
}

pub(super) async fn broadcast_synced(
    node: &Arc<NodeHandle>,
    peers: &[SocketAddr],
    last_applied: u64,
) {
    let synced_msg = ClusterMessage::Synced {
        node_id: node.id,
        last_applied,
    };
    for addr in peers {
        let _ = send_cluster(*addr, &synced_msg, node.tls_connector.as_ref()).await;
    }
}

pub(super) fn should_wait_for_recovery_peer_discovery(
    discovered_peer_count: usize,
    configured_seed_count: usize,
) -> bool {
    discovered_peer_count == 0 && configured_seed_count > 0
}

pub(super) async fn admin_request_to_addr(
    node: &Arc<NodeHandle>,
    addr: SocketAddr,
    req: AdminRequest,
) -> Option<ditto_protocol::AdminResponse> {
    let msg = ClusterMessage::Admin(req);
    match send_cluster(addr, &msg, node.tls_connector.as_ref()).await {
        Ok(Some(ClusterMessage::AdminResponse(resp))) => Some(*resp),
        _ => None,
    }
}

pub(super) fn should_run_full_reconcile(run_no: u64, every: u64) -> bool {
    every > 0 && run_no > 0 && run_no.is_multiple_of(every)
}

pub(super) fn should_throttle_anti_entropy_repair(
    now_ms: u64,
    last_ms: u64,
    min_interval_ms: u64,
) -> bool {
    now_ms < last_ms.saturating_add(min_interval_ms.max(1))
}

pub(super) fn anti_entropy_budget_exhausted(
    started_at: std::time::Instant,
    consumed_checks: usize,
    max_checks: usize,
    max_duration_ms: u64,
) -> bool {
    if max_checks > 0 && consumed_checks >= max_checks {
        return true;
    }
    if max_duration_ms > 0 && started_at.elapsed() >= Duration::from_millis(max_duration_ms) {
        return true;
    }
    false
}

pub(super) fn anti_entropy_budget_remaining_checks(
    consumed_checks: usize,
    max_checks: usize,
) -> usize {
    if max_checks == 0 {
        usize::MAX
    } else {
        max_checks.saturating_sub(consumed_checks)
    }
}

pub(super) fn classify_mixed_version_response(
    resp: Option<ditto_protocol::AdminResponse>,
) -> (bool, bool) {
    match resp {
        Some(ditto_protocol::AdminResponse::Properties(entries)) => {
            let expected = PROTOCOL_VERSION.to_string();
            let peer_version = entries
                .into_iter()
                .find(|(k, _)| k == "protocol-version")
                .map(|(_, v)| v);
            match peer_version {
                Some(v) if v.trim() == expected => (false, false),
                Some(_) => (true, false),
                None => (true, false),
            }
        }
        _ => (false, true),
    }
}

pub fn start_log_compaction(node: Arc<NodeHandle>) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(30));
        loop {
            ticker.tick().await;
            let safe_index = node.active_set.lock().await.min_recoverable_applied();
            let mut log = node.write_log.lock().await;
            let before = log.committed_index();
            log.compact(safe_index);
            tracing::debug!(
                "Log compaction: safe_index={} committed_index={}",
                safe_index,
                before
            );
        }
    });
}

pub fn start_version_check(node: Arc<NodeHandle>) {
    tokio::spawn(async move {
        loop {
            let interval_ms = node
                .config
                .lock()
                .unwrap()
                .replication
                .version_check_interval_ms;
            if interval_ms == 0 {
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
            tokio::time::sleep(Duration::from_millis(interval_ms)).await;

            let (status, am_primary, our_index, primary_index) = {
                let set = node.active_set.lock().await;
                let status = set.local_status();
                let am_primary = set.is_primary();
                let our_index = set
                    .all_nodes()
                    .iter()
                    .find(|n| n.id == set.local_id())
                    .map(|n| n.last_applied)
                    .unwrap_or(0);
                let primary_index = {
                    let all = set.all_nodes();
                    set.primary_id()
                        .and_then(|pid| all.into_iter().find(|n| n.id == pid))
                        .map(|n| n.last_applied)
                        .unwrap_or(0)
                };
                (status, am_primary, our_index, primary_index)
            };

            if status != NodeStatus::Active || am_primary {
                continue;
            }

            if our_index < primary_index {
                tracing::warn!(
                    "Version check: lag detected (our_index={}, primary_index={}). Triggering resync.",
                    our_index, primary_index
                );
                Arc::clone(&node).run_resync().await;
            } else {
                tracing::debug!("Version check: in sync (index={}).", our_index);
            }
        }
    });
}

pub fn start_mixed_version_probe(node: Arc<NodeHandle>) {
    tokio::spawn(async move {
        loop {
            let (enabled, interval_ms) = {
                let cfg = node.config.lock().unwrap();
                (
                    cfg.replication.mixed_version_probe_enabled,
                    cfg.replication.mixed_version_probe_interval_ms.max(1),
                )
            };

            if !enabled {
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }

            tokio::time::sleep(Duration::from_millis(interval_ms)).await;
            node.mixed_version_probe_runs_total
                .fetch_add(1, Ordering::Relaxed);

            let peers: Vec<SocketAddr> = {
                let set = node.active_set.lock().await;
                set.all_nodes()
                    .into_iter()
                    .filter(|n| n.id != set.local_id())
                    .map(|n| SocketAddr::new(n.addr.ip(), n.cluster_port))
                    .collect()
            };

            let mut detected = 0u64;
            let mut errors = 0u64;
            for addr in peers {
                let resp = admin_request_to_addr(
                    &node,
                    addr,
                    AdminRequest::GetProperty {
                        name: "protocol-version".into(),
                    },
                )
                .await;
                let (is_mixed, is_error) = classify_mixed_version_response(resp);
                if is_mixed {
                    detected = detected.saturating_add(1);
                }
                if is_error {
                    errors = errors.saturating_add(1);
                }
            }

            node.mixed_version_last_detected_peer_count
                .store(detected, Ordering::Relaxed);
            if detected > 0 {
                node.mixed_version_peers_detected_total
                    .fetch_add(detected, Ordering::Relaxed);
                tracing::warn!(
                    "Mixed-version probe: detected {} peer(s) not matching protocol-version={}.",
                    detected,
                    PROTOCOL_VERSION
                );
            }
            if errors > 0 {
                node.mixed_version_probe_errors_total
                    .fetch_add(errors, Ordering::Relaxed);
            }
        }
    });
}
