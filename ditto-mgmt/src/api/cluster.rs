//! Cluster-level status endpoints.
//!
//! | Method | Path | Description |
//! |--------|------|-------------|
//! | GET | `/api/cluster` | Aggregate cluster status + node list |
//! | GET | `/api/cluster/primary` | Current primary node ID |

use crate::api::SharedState;
use crate::node_client::{admin_rpc, resolve_target};
use axum::{extract::State, response::IntoResponse, Json};
use ditto_protocol::{AdminRequest, AdminResponse, NodeStatus};
use serde::Serialize;
use std::net::SocketAddr;
use tokio_rustls::TlsConnector;

// ---------------------------------------------------------------------------
// Helper: find the primary node's ID
// ---------------------------------------------------------------------------

/// Query all known nodes and return the UUID of the one that reports `is_primary: true`.
///
/// Returns `None` if no node reports itself as primary (e.g. election in progress).
pub async fn find_primary_id(
    all_addrs: &[SocketAddr],
    tls: Option<&TlsConnector>,
) -> Option<String> {
    for &addr in all_addrs {
        if let Ok(AdminResponse::Stats(s)) = admin_rpc(addr, AdminRequest::GetStats, tls).await {
            if s.is_primary {
                return Some(s.node_id.to_string());
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// GET /api/cluster
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct ClusterStatusResponse {
    pub total: usize,
    pub active: usize,
    pub syncing: usize,
    pub inactive: usize,
    pub offline: usize,
    pub primary: Option<String>,
    pub nodes: Vec<ClusterNodeSummary>,
}

#[derive(Serialize)]
pub struct ClusterNodeSummary {
    pub id: String,
    pub status: String,
    pub is_primary: bool,
    pub last_applied: u64,
}

/// `GET /api/cluster` — Aggregate cluster overview.
///
/// Returns counts by status (active, syncing, inactive, offline),
/// the current primary ID, and a summary row per node.
pub async fn cluster_status(State(state): State<SharedState>) -> impl IntoResponse {
    let seed = state
        .cfg
        .connection
        .seeds
        .first()
        .cloned()
        .unwrap_or_else(|| format!("127.0.0.1:{}", state.cfg.connection.cluster_port));
    let addrs = resolve_target(
        &seed,
        state.cfg.connection.cluster_port,
        &state.cfg.connection.seeds,
    )
    .await;
    let addr = match addrs.first() {
        Some(a) => *a,
        None => return Json(serde_json::json!({ "error": "no seed configured" })).into_response(),
    };

    let all_addrs = state.cluster_addrs().await;
    let primary_id = find_primary_id(&all_addrs, state.tls.as_ref()).await;

    match admin_rpc(addr, AdminRequest::ClusterStatus, state.tls.as_ref()).await {
        Ok(AdminResponse::ClusterView(nodes)) => {
            let active = nodes
                .iter()
                .filter(|n| n.status == NodeStatus::Active)
                .count();
            let syncing = nodes
                .iter()
                .filter(|n| n.status == NodeStatus::Syncing)
                .count();
            let inactive = nodes
                .iter()
                .filter(|n| n.status == NodeStatus::Inactive)
                .count();
            let offline = nodes
                .iter()
                .filter(|n| n.status == NodeStatus::Offline)
                .count();

            let summaries: Vec<_> = nodes
                .iter()
                .map(|n| {
                    let is_primary = primary_id
                        .as_deref()
                        .map(|pid| pid == n.id.to_string())
                        .unwrap_or(false);
                    ClusterNodeSummary {
                        id: n.id.to_string(),
                        status: format!("{:?}", n.status),
                        is_primary,
                        last_applied: n.last_applied,
                    }
                })
                .collect();

            Json(
                serde_json::to_value(ClusterStatusResponse {
                    total: nodes.len(),
                    active,
                    syncing,
                    inactive,
                    offline,
                    primary: primary_id,
                    nodes: summaries,
                })
                .unwrap(),
            )
            .into_response()
        }
        Err(e) => Json(serde_json::json!({ "error": e.to_string() })).into_response(),
        _ => Json(serde_json::json!({ "error": "unexpected response" })).into_response(),
    }
}

// ---------------------------------------------------------------------------
// GET /api/cluster/primary
// ---------------------------------------------------------------------------

/// `GET /api/cluster/primary` — Return the UUID of the current primary node.
///
/// Response: `{ "primary": "<uuid>" }` or `{ "primary": null }` when no primary is elected.
pub async fn cluster_primary(State(state): State<SharedState>) -> impl IntoResponse {
    let all_addrs = state.cluster_addrs().await;
    let primary = find_primary_id(&all_addrs, state.tls.as_ref()).await;
    Json(serde_json::json!({ "primary": primary }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{api::AppState, config::MgmtConfig};
    use axum::{
        body::to_bytes,
        response::{IntoResponse, Response},
    };
    use ditto_protocol::{decode, encode, ClusterMessage, NodeInfo, NodeStats};
    use std::{net::SocketAddr, sync::Arc};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::Mutex,
    };
    use uuid::Uuid;

    fn state_for_seed(seed: String, cluster_port: u16) -> SharedState {
        let mut cfg = MgmtConfig::default();
        cfg.connection.seeds = vec![seed];
        cfg.connection.cluster_port = cluster_port;

        Arc::new(AppState {
            cfg: Arc::new(cfg),
            tls: None,
            http_client: reqwest::Client::new(),
            addr_cache: Mutex::new(None),
        })
    }

    async fn admin_responses(
        responses: Vec<AdminResponse>,
    ) -> (SocketAddr, tokio::task::JoinHandle<Vec<AdminRequest>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            let mut requests = Vec::new();
            for response in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut len_buf = [0u8; 4];
                stream.read_exact(&mut len_buf).await.unwrap();
                let len = u32::from_be_bytes(len_buf) as usize;
                let mut payload = vec![0u8; len];
                stream.read_exact(&mut payload).await.unwrap();

                let request = match decode::<ClusterMessage>(&payload, 1024 * 1024).unwrap() {
                    ClusterMessage::Admin(req) => req,
                    _ => panic!("unexpected non-admin request"),
                };
                requests.push(request);

                let frame = encode(&ClusterMessage::AdminResponse(Box::new(response))).unwrap();
                stream.write_all(&frame).await.unwrap();
            }
            requests
        });

        (addr, handle)
    }

    async fn json_body(response: Response) -> serde_json::Value {
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    fn node(id: u128, addr: SocketAddr, status: NodeStatus, last_applied: u64) -> NodeInfo {
        NodeInfo {
            id: Uuid::from_u128(id),
            addr,
            cluster_port: addr.port(),
            status,
            last_applied,
        }
    }

    fn stats(id: Uuid, is_primary: bool) -> NodeStats {
        NodeStats {
            node_id: id,
            status: NodeStatus::Active,
            is_primary,
            committed_index: 0,
            key_count: 0,
            memory_used_bytes: 0,
            memory_max_bytes: 0,
            evictions: 0,
            hit_count: 0,
            miss_count: 0,
            uptime_secs: 0,
            value_size_limit_bytes: 0,
            max_keys_limit: 0,
            compression_enabled: false,
            compression_threshold_bytes: 0,
            node_name: "node".into(),
            backup_dir_bytes: 0,
            snapshot_last_load_path: None,
            snapshot_last_load_duration_ms: 0,
            snapshot_last_load_entries: 0,
            snapshot_last_load_age_secs: None,
            snapshot_restore_attempt_total: 0,
            snapshot_restore_success_total: 0,
            snapshot_restore_failure_total: 0,
            snapshot_restore_not_found_total: 0,
            snapshot_restore_policy_block_total: 0,
            snapshot_restore_success_ratio_pct: 0,
            persistence_platform_allowed: false,
            persistence_runtime_enabled: false,
            persistence_enabled: false,
            persistence_backup_enabled: false,
            persistence_export_enabled: false,
            persistence_import_enabled: false,
            tenancy_enabled: false,
            tenancy_default_namespace: "default".into(),
            tenancy_max_keys_per_namespace: 0,
            rate_limit_enabled: false,
            rate_limited_requests_total: 0,
            circuit_breaker_enabled: false,
            hot_key_enabled: false,
            hot_key_adaptive_waiters_enabled: false,
            read_repair_enabled: false,
            hot_key_coalesced_hits_total: 0,
            hot_key_fallback_exec_total: 0,
            hot_key_wait_timeout_total: 0,
            hot_key_stale_served_total: 0,
            hot_key_inflight_keys: 0,
            hot_key_stale_cache_entries: 0,
            hot_key_adaptive_state_keys: 0,
            hot_key_adaptive_limit_increase_total: 0,
            hot_key_adaptive_limit_decrease_total: 0,
            read_repair_trigger_total: 0,
            read_repair_success_total: 0,
            read_repair_throttled_total: 0,
            read_repair_budget_exhausted_total: 0,
            namespace_quota_reject_total: 0,
            namespace_quota_reject_rate_per_min: 0,
            namespace_quota_reject_trend: "steady".into(),
            namespace_quota_top_usage: vec![],
            namespace_latency_top: vec![],
            hot_key_top_usage: vec![],
            anti_entropy_runs_total: 0,
            anti_entropy_repair_trigger_total: 0,
            anti_entropy_repair_throttled_total: 0,
            anti_entropy_last_detected_lag: 0,
            anti_entropy_key_checks_total: 0,
            anti_entropy_key_mismatch_total: 0,
            anti_entropy_full_reconcile_runs_total: 0,
            anti_entropy_full_reconcile_key_checks_total: 0,
            anti_entropy_full_reconcile_mismatch_total: 0,
            anti_entropy_budget_exhausted_total: 0,
            mixed_version_probe_runs_total: 0,
            mixed_version_peers_detected_total: 0,
            mixed_version_probe_errors_total: 0,
            mixed_version_last_detected_peer_count: 0,
            circuit_breaker_state: "closed".into(),
            circuit_breaker_open_total: 0,
            circuit_breaker_reject_total: 0,
            client_requests_total: 0,
            client_requests_tcp_total: 0,
            client_requests_http_total: 0,
            client_requests_internal_total: 0,
            client_request_latency_le_1ms_total: 0,
            client_request_latency_le_5ms_total: 0,
            client_request_latency_le_20ms_total: 0,
            client_request_latency_le_100ms_total: 0,
            client_request_latency_le_500ms_total: 0,
            client_request_latency_gt_500ms_total: 0,
            client_latency_p50_estimate_ms: None,
            client_latency_p90_estimate_ms: None,
            client_latency_p95_estimate_ms: None,
            client_latency_p99_estimate_ms: None,
            client_error_total: 0,
            client_errors_tcp_total: 0,
            client_errors_http_total: 0,
            client_errors_internal_total: 0,
            client_error_auth_total: 0,
            client_error_throttle_total: 0,
            client_error_availability_total: 0,
            client_error_validation_total: 0,
            client_error_internal_total: 0,
            client_error_other_total: 0,
        }
    }

    #[tokio::test]
    async fn find_primary_id_returns_first_primary_stats_response() {
        let primary_id = Uuid::from_u128(7);
        let (first, first_requests) = admin_responses(vec![AdminResponse::Stats(Box::new(stats(
            Uuid::from_u128(1),
            false,
        )))])
        .await;
        let (second, second_requests) = admin_responses(vec![AdminResponse::Stats(Box::new(
            stats(primary_id, true),
        ))])
        .await;

        let found = find_primary_id(&[first, second], None).await;

        assert_eq!(found, Some(primary_id.to_string()));
        assert!(matches!(
            first_requests.await.unwrap()[0],
            AdminRequest::GetStats
        ));
        assert!(matches!(
            second_requests.await.unwrap()[0],
            AdminRequest::GetStats
        ));
    }

    #[tokio::test]
    async fn cluster_status_aggregates_counts_and_marks_primary_node() {
        let primary_id = Uuid::from_u128(11);
        let (addr, requests) = admin_responses(vec![
            AdminResponse::ClusterView(vec![]),
            AdminResponse::Stats(Box::new(stats(primary_id, true))),
            AdminResponse::ClusterView(vec![
                node(
                    11,
                    "127.0.0.1:10001".parse().unwrap(),
                    NodeStatus::Active,
                    9,
                ),
                node(
                    12,
                    "127.0.0.1:10002".parse().unwrap(),
                    NodeStatus::Syncing,
                    8,
                ),
                node(
                    13,
                    "127.0.0.1:10003".parse().unwrap(),
                    NodeStatus::Inactive,
                    7,
                ),
                node(
                    14,
                    "127.0.0.1:10004".parse().unwrap(),
                    NodeStatus::Offline,
                    6,
                ),
            ]),
        ])
        .await;
        let state = state_for_seed(addr.to_string(), addr.port());

        let response = cluster_status(State(state)).await.into_response();
        let body = json_body(response).await;

        assert_eq!(body["total"], 4);
        assert_eq!(body["active"], 1);
        assert_eq!(body["syncing"], 1);
        assert_eq!(body["inactive"], 1);
        assert_eq!(body["offline"], 1);
        assert_eq!(body["primary"], primary_id.to_string());
        assert_eq!(body["nodes"][0]["is_primary"], true);
        assert_eq!(body["nodes"][0]["status"], "Active");

        let requests = requests.await.unwrap();
        assert!(matches!(requests[0], AdminRequest::ClusterStatus));
        assert!(matches!(requests[1], AdminRequest::GetStats));
        assert!(matches!(requests[2], AdminRequest::ClusterStatus));
    }

    #[tokio::test]
    async fn cluster_primary_returns_null_when_no_primary_is_reported() {
        let (addr, requests) = admin_responses(vec![
            AdminResponse::ClusterView(vec![]),
            AdminResponse::Stats(Box::new(stats(Uuid::from_u128(1), false))),
        ])
        .await;
        let state = state_for_seed(addr.to_string(), addr.port());

        let response = cluster_primary(State(state)).await.into_response();
        let body = json_body(response).await;

        assert_eq!(body["primary"], serde_json::Value::Null);
        let requests = requests.await.unwrap();
        assert!(matches!(requests[0], AdminRequest::ClusterStatus));
        assert!(matches!(requests[1], AdminRequest::GetStats));
    }
}
