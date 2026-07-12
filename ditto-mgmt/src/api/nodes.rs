//! Node management endpoints.
//!
//! | Method | Path | Description |
//! |--------|------|-------------|
//! | GET | `/api/nodes` | Status of all cluster nodes |
//! | GET | `/api/nodes/:target/status` | Status of one or all nodes |
//! | GET | `/api/nodes/:target/describe` | Full property list |
//! | GET | `/api/nodes/:target/property/:name` | Single property value |
//! | POST | `/api/nodes/:target/property/:name` | Update a property |
//! | POST | `/api/nodes/:target/set-active` | Activate / deactivate a node |
//! | POST | `/api/nodes/:target/backup` | Trigger immediate backup |
//! | POST | `/api/nodes/:target/restore-snapshot` | Restore latest snapshot |

use crate::api::SharedState;
use crate::app::nodes as app_nodes;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// GET /api/nodes
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct NodeListResponse {
    pub nodes: Vec<app_nodes::NodeInfo>,
}

/// `GET /api/nodes` — Return health status of every node in the cluster.
///
/// Discovers all nodes via seed addresses + gossip, then queries each in sequence.
/// Unreachable nodes appear in the response with `reachable: false`.
pub async fn list_nodes(State(state): State<SharedState>) -> impl IntoResponse {
    Json(NodeListResponse {
        nodes: app_nodes::collect_nodes(state).await,
    })
}

// ---------------------------------------------------------------------------
// GET /api/nodes/:target/status
// ---------------------------------------------------------------------------

/// `GET /api/nodes/:target/status` — Status of a specific node (or all nodes).
///
/// `target` may be `"all"`, `"local"`, or a `host:port` address.
pub async fn node_status(
    State(state): State<SharedState>,
    Path(target): Path<String>,
) -> impl IntoResponse {
    Json(app_nodes::collect_target_nodes(state, &target).await)
}

// ---------------------------------------------------------------------------
// GET /api/nodes/:target/describe
// ---------------------------------------------------------------------------

/// `GET /api/nodes/:target/describe` — All key-value properties of a node.
pub async fn node_describe(
    State(state): State<SharedState>,
    Path(target): Path<String>,
) -> impl IntoResponse {
    Json(app_nodes::describe_nodes(state, &target).await)
}

// ---------------------------------------------------------------------------
// GET /api/nodes/:target/property/:name
// ---------------------------------------------------------------------------

/// `GET /api/nodes/:target/property/:name` — Read a single node property.
pub async fn get_property(
    State(state): State<SharedState>,
    Path((target, name)): Path<(String, String)>,
) -> impl IntoResponse {
    let (status, payload) = app_nodes::get_property_value(state, &target, &name).await;
    (
        StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
        Json(payload),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// POST /api/nodes/:target/property/:name   body: { "value": "..." }
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct SetPropertyBody {
    pub value: String,
}

/// `POST /api/nodes/:target/property/:name` — Write a node property.
///
/// Body: `{ "value": "<new-value>" }`
pub async fn set_property(
    State(state): State<SharedState>,
    Path((target, name)): Path<(String, String)>,
    Json(body): Json<SetPropertyBody>,
) -> impl IntoResponse {
    Json(app_nodes::set_property_value(state, &target, &name, &body.value).await)
}

// ---------------------------------------------------------------------------
// POST /api/nodes/:target/set-active   body: { "active": bool }
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct SetActiveBody {
    pub active: bool,
}

/// `POST /api/nodes/:target/set-active` — Toggle node active/inactive state.
///
/// Body: `{ "active": true | false }`
pub async fn set_active(
    State(state): State<SharedState>,
    Path(target): Path<String>,
    Json(body): Json<SetActiveBody>,
) -> impl IntoResponse {
    let active_str = if body.active { "true" } else { "false" };
    Json(app_nodes::set_property_value(state, &target, "active", active_str).await)
}

// ---------------------------------------------------------------------------
// POST /api/nodes/:target/backup
// ---------------------------------------------------------------------------

/// `POST /api/nodes/:target/backup` — Trigger an immediate backup on a node.
///
/// The node must be `Inactive` for the backup to proceed; the handler enforces
/// this via the admin protocol (the node itself rejects the request otherwise).
pub async fn backup_node(
    State(state): State<SharedState>,
    Path(target): Path<String>,
) -> impl IntoResponse {
    Json(app_nodes::trigger_backup(state, &target).await)
}

/// `POST /api/nodes/:target/restore-snapshot` — Restore latest local snapshot on a node.
pub async fn restore_snapshot(
    State(state): State<SharedState>,
    Path(target): Path<String>,
) -> impl IntoResponse {
    Json(app_nodes::restore_latest_snapshot(state, &target).await)
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use ditto_protocol::{AdminRequest, AdminResponse};
    use crate::app::nodes::build_node_info;
    use crate::{api::AppState, config::MgmtConfig};
    use axum::{
        body::to_bytes,
        response::{IntoResponse, Response},
    };
    use ditto_protocol::{
        decode, encode, ClusterMessage, NamespaceQuotaUsage, NodeStats, NodeStatus,
    };
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

    fn stats() -> NodeStats {
        NodeStats {
            node_id: Uuid::from_u128(42),
            status: NodeStatus::Active,
            is_primary: true,
            committed_index: 99,
            key_count: 7,
            memory_used_bytes: 1024,
            memory_max_bytes: 4096,
            evictions: 2,
            hit_count: 11,
            miss_count: 3,
            uptime_secs: 60,
            value_size_limit_bytes: 0,
            max_keys_limit: 0,
            compression_enabled: false,
            compression_threshold_bytes: 0,
            node_name: "node-a".into(),
            backup_dir_bytes: 55,
            snapshot_last_load_path: Some("snap.json".into()),
            snapshot_last_load_duration_ms: 8,
            snapshot_last_load_entries: 9,
            snapshot_last_load_age_secs: Some(10),
            snapshot_restore_attempt_total: 1,
            snapshot_restore_success_total: 1,
            snapshot_restore_failure_total: 0,
            snapshot_restore_not_found_total: 0,
            snapshot_restore_policy_block_total: 0,
            snapshot_restore_success_ratio_pct: 100,
            persistence_platform_allowed: true,
            persistence_runtime_enabled: true,
            persistence_enabled: true,
            persistence_backup_enabled: true,
            persistence_export_enabled: true,
            persistence_import_enabled: true,
            tenancy_enabled: true,
            tenancy_default_namespace: "tenant-a".into(),
            tenancy_max_keys_per_namespace: 100,
            rate_limit_enabled: true,
            rate_limited_requests_total: 4,
            circuit_breaker_enabled: true,
            hot_key_enabled: true,
            hot_key_adaptive_waiters_enabled: false,
            read_repair_enabled: true,
            hot_key_coalesced_hits_total: 5,
            hot_key_fallback_exec_total: 6,
            hot_key_wait_timeout_total: 7,
            hot_key_stale_served_total: 8,
            hot_key_inflight_keys: 9,
            hot_key_stale_cache_entries: 10,
            hot_key_adaptive_state_keys: 0,
            hot_key_adaptive_limit_increase_total: 0,
            hot_key_adaptive_limit_decrease_total: 0,
            read_repair_trigger_total: 12,
            read_repair_success_total: 13,
            read_repair_throttled_total: 14,
            read_repair_budget_exhausted_total: 0,
            namespace_quota_reject_total: 15,
            namespace_quota_reject_rate_per_min: 16,
            namespace_quota_reject_trend: "rising".into(),
            namespace_quota_top_usage: vec![NamespaceQuotaUsage {
                namespace: "tenant-a".into(),
                key_count: 90,
                quota_limit: 100,
                usage_pct: 90,
                remaining_keys: 10,
            }],
            namespace_latency_top: vec![],
            hot_key_top_usage: vec![],
            anti_entropy_runs_total: 17,
            anti_entropy_repair_trigger_total: 18,
            anti_entropy_repair_throttled_total: 0,
            anti_entropy_last_detected_lag: 19,
            anti_entropy_key_checks_total: 20,
            anti_entropy_key_mismatch_total: 21,
            anti_entropy_full_reconcile_runs_total: 22,
            anti_entropy_full_reconcile_key_checks_total: 23,
            anti_entropy_full_reconcile_mismatch_total: 24,
            anti_entropy_budget_exhausted_total: 0,
            mixed_version_probe_runs_total: 25,
            mixed_version_peers_detected_total: 26,
            mixed_version_probe_errors_total: 27,
            mixed_version_last_detected_peer_count: 28,
            circuit_breaker_state: "closed".into(),
            circuit_breaker_open_total: 29,
            circuit_breaker_reject_total: 30,
            client_requests_total: 31,
            client_requests_tcp_total: 32,
            client_requests_http_total: 33,
            client_requests_internal_total: 34,
            client_request_latency_le_1ms_total: 35,
            client_request_latency_le_5ms_total: 36,
            client_request_latency_le_20ms_total: 37,
            client_request_latency_le_100ms_total: 38,
            client_request_latency_le_500ms_total: 39,
            client_request_latency_gt_500ms_total: 40,
            client_latency_p50_estimate_ms: Some(1),
            client_latency_p90_estimate_ms: Some(5),
            client_latency_p95_estimate_ms: Some(20),
            client_latency_p99_estimate_ms: Some(100),
            client_error_total: 41,
            client_errors_tcp_total: 42,
            client_errors_http_total: 43,
            client_errors_internal_total: 44,
            client_error_auth_total: 45,
            client_error_throttle_total: 46,
            client_error_availability_total: 47,
            client_error_validation_total: 48,
            client_error_internal_total: 49,
            client_error_other_total: 50,
        }
    }

    #[test]
    fn build_node_info_maps_stats_and_unreachable_responses() {
        let addr: SocketAddr = "127.0.0.1:7779".parse().unwrap();
        let info = build_node_info(addr, Ok(AdminResponse::Stats(Box::new(stats()))), 12);

        assert!(info.reachable);
        assert_eq!(info.heartbeat_ms, Some(12));
        assert_eq!(info.node_name.as_deref(), Some("node-a"));
        assert_eq!(info.status.as_deref(), Some("Active"));
        assert_eq!(info.is_primary, Some(true));
        assert_eq!(info.committed_index, Some(99));
        assert_eq!(info.namespace_quota_reject_trend.as_deref(), Some("rising"));
        assert_eq!(
            info.namespace_quota_top_usage
                .as_ref()
                .and_then(|items| items.first())
                .map(|item| item.usage_pct),
            Some(90)
        );
        assert_eq!(info.client_latency_p99_estimate_ms, Some(100));
        assert_eq!(info.client_error_other_total, Some(50));

        let unreachable = build_node_info(addr, Ok(AdminResponse::Ok), 1);
        assert!(!unreachable.reachable);
        assert_eq!(unreachable.node_name, None);
        assert_eq!(unreachable.key_count, None);
    }

    #[tokio::test]
    async fn get_property_returns_matching_value_and_records_request() {
        let (addr, requests) = admin_responses(vec![AdminResponse::Properties(vec![
            ("active".into(), "true".into()),
            ("mode".into(), "primary".into()),
        ])])
        .await;
        let state = state_for_seed(addr.to_string(), addr.port());

        let response = get_property(State(state), Path((addr.to_string(), "mode".into())))
            .await
            .into_response();

        let body = json_body(response).await;
        assert_eq!(body["value"], "primary");
        assert!(matches!(
            requests.await.unwrap()[0],
            AdminRequest::GetProperty { ref name } if name == "mode"
        ));
    }

    #[tokio::test]
    async fn node_describe_maps_properties_response() {
        let (addr, requests) = admin_responses(vec![AdminResponse::Properties(vec![(
            "tcp-production-safe".into(),
            "true".into(),
        )])])
        .await;
        let state = state_for_seed(addr.to_string(), addr.port());

        let response = node_describe(State(state), Path(addr.to_string()))
            .await
            .into_response();

        let body = json_body(response).await;
        assert_eq!(body[0]["addr"], addr.to_string());
        assert_eq!(body[0]["properties"][0][0], "tcp-production-safe");
        assert_eq!(body[0]["error"], serde_json::Value::Null);
        assert!(matches!(requests.await.unwrap()[0], AdminRequest::Describe));
    }

    #[tokio::test]
    async fn set_active_sends_boolean_property_update() {
        let (addr, requests) = admin_responses(vec![AdminResponse::Ok]).await;
        let state = state_for_seed(addr.to_string(), addr.port());

        let response = set_active(
            State(state),
            Path(addr.to_string()),
            Json(SetActiveBody { active: false }),
        )
        .await
        .into_response();

        let body = json_body(response).await;
        assert_eq!(body[0]["ok"], true);
        assert!(matches!(
            requests.await.unwrap()[0],
            AdminRequest::SetProperty {
                ref name,
                ref value
            } if name == "active" && value == "false"
        ));
    }

    #[tokio::test]
    async fn backup_node_and_restore_snapshot_render_success_payloads() {
        let (backup_addr, backup_requests) = admin_responses(vec![AdminResponse::BackupResult {
            path: "backup.json".into(),
            bytes: 123,
        }])
        .await;
        let backup_state = state_for_seed(backup_addr.to_string(), backup_addr.port());

        let backup_response = backup_node(State(backup_state), Path(backup_addr.to_string()))
            .await
            .into_response();
        let backup_body = json_body(backup_response).await;
        assert_eq!(backup_body[0]["ok"], true);
        assert_eq!(backup_body[0]["path"], "backup.json");
        assert!(matches!(
            backup_requests.await.unwrap()[0],
            AdminRequest::BackupNow
        ));

        let (restore_addr, restore_requests) =
            admin_responses(vec![AdminResponse::RestoreResult {
                path: "snapshot.json".into(),
                entries: 5,
                duration_ms: 17,
            }])
            .await;
        let restore_state = state_for_seed(restore_addr.to_string(), restore_addr.port());

        let restore_response =
            restore_snapshot(State(restore_state), Path(restore_addr.to_string()))
                .await
                .into_response();
        let restore_body = json_body(restore_response).await;
        assert_eq!(restore_body[0]["ok"], true);
        assert_eq!(restore_body[0]["entries"], 5);
        assert_eq!(restore_body[0]["duration_ms"], 17);
        assert!(matches!(
            restore_requests.await.unwrap()[0],
            AdminRequest::RestoreLatestSnapshot
        ));
    }
}
