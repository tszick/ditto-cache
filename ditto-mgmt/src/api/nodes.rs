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
use crate::node_client::{admin_rpc, resolve_target};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use ditto_protocol::{AdminRequest, AdminResponse};
use serde::{Deserialize, Serialize};
use std::time::Instant;

#[derive(Debug, Clone, Serialize)]
pub struct NamespaceQuotaUsageView {
    pub namespace: String,
    pub key_count: u64,
    pub quota_limit: u64,
    pub usage_pct: u64,
    pub remaining_keys: u64,
}

// ---------------------------------------------------------------------------
// NodeInfo — JSON representation of a node's health status
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct NodeInfo {
    pub addr: String,
    pub reachable: bool,
    pub heartbeat_ms: Option<u64>,
    pub node_name: Option<String>,
    pub node_id: Option<String>,
    pub status: Option<String>,
    pub is_primary: Option<bool>,
    pub committed_index: Option<u64>,
    pub memory_used_bytes: Option<u64>,
    pub memory_max_bytes: Option<u64>,
    pub uptime_secs: Option<u64>,
    pub backup_dir_bytes: Option<u64>,
    pub snapshot_last_load_path: Option<String>,
    pub snapshot_last_load_duration_ms: Option<u64>,
    pub snapshot_last_load_entries: Option<u64>,
    pub snapshot_last_load_age_secs: Option<u64>,
    pub snapshot_restore_attempt_total: Option<u64>,
    pub snapshot_restore_success_total: Option<u64>,
    pub snapshot_restore_failure_total: Option<u64>,
    pub snapshot_restore_not_found_total: Option<u64>,
    pub snapshot_restore_policy_block_total: Option<u64>,
    pub persistence_enabled: Option<bool>,
    pub persistence_backup_enabled: Option<bool>,
    pub persistence_export_enabled: Option<bool>,
    pub persistence_import_enabled: Option<bool>,
    pub tenancy_enabled: Option<bool>,
    pub tenancy_default_namespace: Option<String>,
    pub tenancy_max_keys_per_namespace: Option<usize>,
    pub rate_limit_enabled: Option<bool>,
    pub rate_limited_requests_total: Option<u64>,
    pub hot_key_enabled: Option<bool>,
    pub hot_key_coalesced_hits_total: Option<u64>,
    pub hot_key_fallback_exec_total: Option<u64>,
    pub hot_key_inflight_keys: Option<u64>,
    pub read_repair_enabled: Option<bool>,
    pub read_repair_trigger_total: Option<u64>,
    pub read_repair_success_total: Option<u64>,
    pub read_repair_throttled_total: Option<u64>,
    pub namespace_quota_reject_total: Option<u64>,
    pub namespace_quota_reject_rate_per_min: Option<u64>,
    pub namespace_quota_reject_trend: Option<String>,
    pub namespace_quota_top_usage: Option<Vec<NamespaceQuotaUsageView>>,
    pub anti_entropy_runs_total: Option<u64>,
    pub anti_entropy_repair_trigger_total: Option<u64>,
    pub anti_entropy_last_detected_lag: Option<u64>,
    pub anti_entropy_key_checks_total: Option<u64>,
    pub anti_entropy_key_mismatch_total: Option<u64>,
    pub anti_entropy_full_reconcile_runs_total: Option<u64>,
    pub anti_entropy_full_reconcile_key_checks_total: Option<u64>,
    pub anti_entropy_full_reconcile_mismatch_total: Option<u64>,
    pub mixed_version_probe_runs_total: Option<u64>,
    pub mixed_version_peers_detected_total: Option<u64>,
    pub mixed_version_probe_errors_total: Option<u64>,
    pub mixed_version_last_detected_peer_count: Option<u64>,
    pub circuit_breaker_enabled: Option<bool>,
    pub circuit_breaker_state: Option<String>,
    pub circuit_breaker_open_total: Option<u64>,
    pub circuit_breaker_reject_total: Option<u64>,
    pub client_requests_total: Option<u64>,
    pub client_requests_tcp_total: Option<u64>,
    pub client_requests_http_total: Option<u64>,
    pub client_requests_internal_total: Option<u64>,
    pub client_request_latency_le_1ms_total: Option<u64>,
    pub client_request_latency_le_5ms_total: Option<u64>,
    pub client_request_latency_le_20ms_total: Option<u64>,
    pub client_request_latency_le_100ms_total: Option<u64>,
    pub client_request_latency_le_500ms_total: Option<u64>,
    pub client_request_latency_gt_500ms_total: Option<u64>,
    pub client_latency_p50_estimate_ms: Option<u64>,
    pub client_latency_p90_estimate_ms: Option<u64>,
    pub client_latency_p95_estimate_ms: Option<u64>,
    pub client_latency_p99_estimate_ms: Option<u64>,
    pub client_error_total: Option<u64>,
    pub client_errors_tcp_total: Option<u64>,
    pub client_errors_http_total: Option<u64>,
    pub client_errors_internal_total: Option<u64>,
    pub client_error_auth_total: Option<u64>,
    pub client_error_throttle_total: Option<u64>,
    pub client_error_availability_total: Option<u64>,
    pub client_error_validation_total: Option<u64>,
    pub client_error_internal_total: Option<u64>,
    pub client_error_other_total: Option<u64>,
    pub key_count: Option<u64>,
    pub evictions: Option<u64>,
    pub hit_count: Option<u64>,
    pub miss_count: Option<u64>,
}

fn build_node_info(
    addr: std::net::SocketAddr,
    result: anyhow::Result<AdminResponse>,
    heartbeat_ms: u64,
) -> NodeInfo {
    match result {
        Ok(AdminResponse::Stats(s)) => NodeInfo {
            addr: addr.to_string(),
            reachable: true,
            heartbeat_ms: Some(heartbeat_ms),
            node_name: Some(s.node_name),
            node_id: Some(s.node_id.to_string()),
            status: Some(format!("{:?}", s.status)),
            is_primary: Some(s.is_primary),
            committed_index: Some(s.committed_index),
            memory_used_bytes: Some(s.memory_used_bytes),
            memory_max_bytes: Some(s.memory_max_bytes),
            uptime_secs: Some(s.uptime_secs),
            backup_dir_bytes: Some(s.backup_dir_bytes),
            snapshot_last_load_path: s.snapshot_last_load_path,
            snapshot_last_load_duration_ms: Some(s.snapshot_last_load_duration_ms),
            snapshot_last_load_entries: Some(s.snapshot_last_load_entries),
            snapshot_last_load_age_secs: s.snapshot_last_load_age_secs,
            snapshot_restore_attempt_total: Some(s.snapshot_restore_attempt_total),
            snapshot_restore_success_total: Some(s.snapshot_restore_success_total),
            snapshot_restore_failure_total: Some(s.snapshot_restore_failure_total),
            snapshot_restore_not_found_total: Some(s.snapshot_restore_not_found_total),
            snapshot_restore_policy_block_total: Some(s.snapshot_restore_policy_block_total),
            persistence_enabled: Some(s.persistence_enabled),
            persistence_backup_enabled: Some(s.persistence_backup_enabled),
            persistence_export_enabled: Some(s.persistence_export_enabled),
            persistence_import_enabled: Some(s.persistence_import_enabled),
            tenancy_enabled: Some(s.tenancy_enabled),
            tenancy_default_namespace: Some(s.tenancy_default_namespace),
            tenancy_max_keys_per_namespace: Some(s.tenancy_max_keys_per_namespace),
            rate_limit_enabled: Some(s.rate_limit_enabled),
            rate_limited_requests_total: Some(s.rate_limited_requests_total),
            hot_key_enabled: Some(s.hot_key_enabled),
            hot_key_coalesced_hits_total: Some(s.hot_key_coalesced_hits_total),
            hot_key_fallback_exec_total: Some(s.hot_key_fallback_exec_total),
            hot_key_inflight_keys: Some(s.hot_key_inflight_keys),
            read_repair_enabled: Some(s.read_repair_enabled),
            read_repair_trigger_total: Some(s.read_repair_trigger_total),
            read_repair_success_total: Some(s.read_repair_success_total),
            read_repair_throttled_total: Some(s.read_repair_throttled_total),
            namespace_quota_reject_total: Some(s.namespace_quota_reject_total),
            namespace_quota_reject_rate_per_min: Some(s.namespace_quota_reject_rate_per_min),
            namespace_quota_reject_trend: Some(s.namespace_quota_reject_trend),
            namespace_quota_top_usage: Some(
                s.namespace_quota_top_usage
                    .into_iter()
                    .map(|u| NamespaceQuotaUsageView {
                        namespace: u.namespace,
                        key_count: u.key_count,
                        quota_limit: u.quota_limit,
                        usage_pct: u.usage_pct,
                        remaining_keys: u.remaining_keys,
                    })
                    .collect(),
            ),
            anti_entropy_runs_total: Some(s.anti_entropy_runs_total),
            anti_entropy_repair_trigger_total: Some(s.anti_entropy_repair_trigger_total),
            anti_entropy_last_detected_lag: Some(s.anti_entropy_last_detected_lag),
            anti_entropy_key_checks_total: Some(s.anti_entropy_key_checks_total),
            anti_entropy_key_mismatch_total: Some(s.anti_entropy_key_mismatch_total),
            anti_entropy_full_reconcile_runs_total: Some(s.anti_entropy_full_reconcile_runs_total),
            anti_entropy_full_reconcile_key_checks_total: Some(
                s.anti_entropy_full_reconcile_key_checks_total,
            ),
            anti_entropy_full_reconcile_mismatch_total: Some(
                s.anti_entropy_full_reconcile_mismatch_total,
            ),
            mixed_version_probe_runs_total: Some(s.mixed_version_probe_runs_total),
            mixed_version_peers_detected_total: Some(s.mixed_version_peers_detected_total),
            mixed_version_probe_errors_total: Some(s.mixed_version_probe_errors_total),
            mixed_version_last_detected_peer_count: Some(s.mixed_version_last_detected_peer_count),
            circuit_breaker_enabled: Some(s.circuit_breaker_enabled),
            circuit_breaker_state: Some(s.circuit_breaker_state),
            circuit_breaker_open_total: Some(s.circuit_breaker_open_total),
            circuit_breaker_reject_total: Some(s.circuit_breaker_reject_total),
            client_requests_total: Some(s.client_requests_total),
            client_requests_tcp_total: Some(s.client_requests_tcp_total),
            client_requests_http_total: Some(s.client_requests_http_total),
            client_requests_internal_total: Some(s.client_requests_internal_total),
            client_request_latency_le_1ms_total: Some(s.client_request_latency_le_1ms_total),
            client_request_latency_le_5ms_total: Some(s.client_request_latency_le_5ms_total),
            client_request_latency_le_20ms_total: Some(s.client_request_latency_le_20ms_total),
            client_request_latency_le_100ms_total: Some(s.client_request_latency_le_100ms_total),
            client_request_latency_le_500ms_total: Some(s.client_request_latency_le_500ms_total),
            client_request_latency_gt_500ms_total: Some(s.client_request_latency_gt_500ms_total),
            client_latency_p50_estimate_ms: s.client_latency_p50_estimate_ms,
            client_latency_p90_estimate_ms: s.client_latency_p90_estimate_ms,
            client_latency_p95_estimate_ms: s.client_latency_p95_estimate_ms,
            client_latency_p99_estimate_ms: s.client_latency_p99_estimate_ms,
            client_error_total: Some(s.client_error_total),
            client_errors_tcp_total: Some(s.client_errors_tcp_total),
            client_errors_http_total: Some(s.client_errors_http_total),
            client_errors_internal_total: Some(s.client_errors_internal_total),
            client_error_auth_total: Some(s.client_error_auth_total),
            client_error_throttle_total: Some(s.client_error_throttle_total),
            client_error_availability_total: Some(s.client_error_availability_total),
            client_error_validation_total: Some(s.client_error_validation_total),
            client_error_internal_total: Some(s.client_error_internal_total),
            client_error_other_total: Some(s.client_error_other_total),
            key_count: Some(s.key_count),
            evictions: Some(s.evictions),
            hit_count: Some(s.hit_count),
            miss_count: Some(s.miss_count),
        },
        _ => NodeInfo {
            addr: addr.to_string(),
            reachable: false,
            heartbeat_ms: None,
            node_name: None,
            node_id: None,
            status: None,
            is_primary: None,
            committed_index: None,
            memory_used_bytes: None,
            memory_max_bytes: None,
            uptime_secs: None,
            backup_dir_bytes: None,
            snapshot_last_load_path: None,
            snapshot_last_load_duration_ms: None,
            snapshot_last_load_entries: None,
            snapshot_last_load_age_secs: None,
            snapshot_restore_attempt_total: None,
            snapshot_restore_success_total: None,
            snapshot_restore_failure_total: None,
            snapshot_restore_not_found_total: None,
            snapshot_restore_policy_block_total: None,
            persistence_enabled: None,
            persistence_backup_enabled: None,
            persistence_export_enabled: None,
            persistence_import_enabled: None,
            tenancy_enabled: None,
            tenancy_default_namespace: None,
            tenancy_max_keys_per_namespace: None,
            rate_limit_enabled: None,
            rate_limited_requests_total: None,
            hot_key_enabled: None,
            hot_key_coalesced_hits_total: None,
            hot_key_fallback_exec_total: None,
            hot_key_inflight_keys: None,
            read_repair_enabled: None,
            read_repair_trigger_total: None,
            read_repair_success_total: None,
            read_repair_throttled_total: None,
            namespace_quota_reject_total: None,
            namespace_quota_reject_rate_per_min: None,
            namespace_quota_reject_trend: None,
            namespace_quota_top_usage: None,
            anti_entropy_runs_total: None,
            anti_entropy_repair_trigger_total: None,
            anti_entropy_last_detected_lag: None,
            anti_entropy_key_checks_total: None,
            anti_entropy_key_mismatch_total: None,
            anti_entropy_full_reconcile_runs_total: None,
            anti_entropy_full_reconcile_key_checks_total: None,
            anti_entropy_full_reconcile_mismatch_total: None,
            mixed_version_probe_runs_total: None,
            mixed_version_peers_detected_total: None,
            mixed_version_probe_errors_total: None,
            mixed_version_last_detected_peer_count: None,
            circuit_breaker_enabled: None,
            circuit_breaker_state: None,
            circuit_breaker_open_total: None,
            circuit_breaker_reject_total: None,
            client_requests_total: None,
            client_requests_tcp_total: None,
            client_requests_http_total: None,
            client_requests_internal_total: None,
            client_request_latency_le_1ms_total: None,
            client_request_latency_le_5ms_total: None,
            client_request_latency_le_20ms_total: None,
            client_request_latency_le_100ms_total: None,
            client_request_latency_le_500ms_total: None,
            client_request_latency_gt_500ms_total: None,
            client_latency_p50_estimate_ms: None,
            client_latency_p90_estimate_ms: None,
            client_latency_p95_estimate_ms: None,
            client_latency_p99_estimate_ms: None,
            client_error_total: None,
            client_errors_tcp_total: None,
            client_errors_http_total: None,
            client_errors_internal_total: None,
            client_error_auth_total: None,
            client_error_throttle_total: None,
            client_error_availability_total: None,
            client_error_validation_total: None,
            client_error_internal_total: None,
            client_error_other_total: None,
            key_count: None,
            evictions: None,
            hit_count: None,
            miss_count: None,
        },
    }
}

// ---------------------------------------------------------------------------
// GET /api/nodes
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct NodeListResponse {
    pub nodes: Vec<NodeInfo>,
}

/// `GET /api/nodes` — Return health status of every node in the cluster.
///
/// Discovers all nodes via seed addresses + gossip, then queries each in sequence.
/// Unreachable nodes appear in the response with `reachable: false`.
pub async fn list_nodes(State(state): State<SharedState>) -> impl IntoResponse {
    let addrs = state.cluster_addrs().await;

    // Query all nodes concurrently so that one slow/unreachable node doesn't
    // block the responses from the healthy ones.
    let mut tasks = tokio::task::JoinSet::new();
    for addr in addrs {
        let state = state.clone();
        tasks.spawn(async move {
            let t0 = Instant::now();
            let resp = admin_rpc(addr, AdminRequest::GetStats, state.tls.as_ref()).await;
            let ms = t0.elapsed().as_millis() as u64;
            build_node_info(addr, resp, ms)
        });
    }

    let mut nodes = Vec::new();
    while let Some(res) = tasks.join_next().await {
        if let Ok(info) = res {
            nodes.push(info);
        }
    }
    // Stable order in the UI regardless of which task finishes first.
    nodes.sort_by(|a, b| a.addr.cmp(&b.addr));

    Json(NodeListResponse { nodes })
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
    let addrs = if target == "all" {
        state.cluster_addrs().await
    } else {
        resolve_target(
            &target,
            state.cfg.connection.cluster_port,
            &state.cfg.connection.seeds,
        )
        .await
    };

    let mut nodes = Vec::new();
    for addr in addrs {
        let t0 = Instant::now();
        let resp = admin_rpc(addr, AdminRequest::GetStats, state.tls.as_ref()).await;
        let ms = t0.elapsed().as_millis() as u64;
        nodes.push(build_node_info(addr, resp, ms));
    }

    Json(nodes)
}

// ---------------------------------------------------------------------------
// GET /api/nodes/:target/describe
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct DescribeEntry {
    pub addr: String,
    pub properties: Vec<(String, String)>,
    pub error: Option<String>,
}

/// `GET /api/nodes/:target/describe` — All key-value properties of a node.
pub async fn node_describe(
    State(state): State<SharedState>,
    Path(target): Path<String>,
) -> impl IntoResponse {
    let addrs = if target == "all" {
        state.cluster_addrs().await
    } else {
        resolve_target(
            &target,
            state.cfg.connection.cluster_port,
            &state.cfg.connection.seeds,
        )
        .await
    };

    let mut result = Vec::new();
    for addr in addrs {
        match admin_rpc(addr, AdminRequest::Describe, state.tls.as_ref()).await {
            Ok(AdminResponse::Properties(props)) => result.push(DescribeEntry {
                addr: addr.to_string(),
                properties: props,
                error: None,
            }),
            Err(e) => result.push(DescribeEntry {
                addr: addr.to_string(),
                properties: vec![],
                error: Some(e.to_string()),
            }),
            _ => result.push(DescribeEntry {
                addr: addr.to_string(),
                properties: vec![],
                error: Some("unexpected response".into()),
            }),
        }
    }
    Json(result)
}

// ---------------------------------------------------------------------------
// GET /api/nodes/:target/property/:name
// ---------------------------------------------------------------------------

/// `GET /api/nodes/:target/property/:name` — Read a single node property.
pub async fn get_property(
    State(state): State<SharedState>,
    Path((target, name)): Path<(String, String)>,
) -> impl IntoResponse {
    let addrs = resolve_target(
        &target,
        state.cfg.connection.cluster_port,
        &state.cfg.connection.seeds,
    )
    .await;
    if let Some(&addr) = addrs.first() {
        match admin_rpc(
            addr,
            AdminRequest::GetProperty { name: name.clone() },
            state.tls.as_ref(),
        )
        .await
        {
            Ok(AdminResponse::Properties(props)) => {
                let value = props.into_iter().find(|(k, _)| k == &name).map(|(_, v)| v);
                return (StatusCode::OK, Json(serde_json::json!({ "value": value })))
                    .into_response();
            }
            Err(e) => {
                return (
                    StatusCode::BAD_GATEWAY,
                    Json(serde_json::json!({ "error": e.to_string() })),
                )
                    .into_response()
            }
            _ => {}
        }
    }
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({ "error": "not found" })),
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
    let addrs = if target == "all" {
        state.cluster_addrs().await
    } else {
        resolve_target(
            &target,
            state.cfg.connection.cluster_port,
            &state.cfg.connection.seeds,
        )
        .await
    };

    let mut results = Vec::new();
    for addr in addrs {
        match admin_rpc(addr, AdminRequest::SetProperty { name: name.clone(), value: body.value.clone() }, state.tls.as_ref()).await {
            Ok(AdminResponse::Ok) =>
                results.push(serde_json::json!({ "addr": addr.to_string(), "ok": true })),
            Err(e) =>
                results.push(serde_json::json!({ "addr": addr.to_string(), "ok": false, "error": e.to_string() })),
            _ =>
                results.push(serde_json::json!({ "addr": addr.to_string(), "ok": false, "error": "unexpected response" })),
        }
    }
    Json(results)
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
    let addrs = if target == "all" {
        state.cluster_addrs().await
    } else {
        resolve_target(
            &target,
            state.cfg.connection.cluster_port,
            &state.cfg.connection.seeds,
        )
        .await
    };

    let active_str = if body.active { "true" } else { "false" };
    let mut results = Vec::new();
    for addr in addrs {
        match admin_rpc(addr, AdminRequest::SetProperty {
            name:  "active".into(),
            value: active_str.into(),
        }, state.tls.as_ref()).await {
            Ok(AdminResponse::Ok) =>
                results.push(serde_json::json!({ "addr": addr.to_string(), "ok": true })),
            Err(e) =>
                results.push(serde_json::json!({ "addr": addr.to_string(), "ok": false, "error": e.to_string() })),
            _ =>
                results.push(serde_json::json!({ "addr": addr.to_string(), "ok": false, "error": "unexpected response" })),
        }
    }
    Json(results)
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
    let addrs = if target == "all" {
        state.cluster_addrs().await
    } else {
        resolve_target(
            &target,
            state.cfg.connection.cluster_port,
            &state.cfg.connection.seeds,
        )
        .await
    };

    let mut results = Vec::new();
    for addr in addrs {
        match admin_rpc(addr, AdminRequest::BackupNow, state.tls.as_ref()).await {
            Ok(AdminResponse::BackupResult { path, .. }) =>
                results.push(serde_json::json!({ "addr": addr.to_string(), "ok": true, "path": path })),
            Ok(AdminResponse::Error { message }) =>
                results.push(serde_json::json!({ "addr": addr.to_string(), "ok": false, "error": message })),
            Err(e) =>
                results.push(serde_json::json!({ "addr": addr.to_string(), "ok": false, "error": e.to_string() })),
            _ =>
                results.push(serde_json::json!({ "addr": addr.to_string(), "ok": false, "error": "unexpected response" })),
        }
    }
    Json(results)
}

/// `POST /api/nodes/:target/restore-snapshot` — Restore latest local snapshot on a node.
pub async fn restore_snapshot(
    State(state): State<SharedState>,
    Path(target): Path<String>,
) -> impl IntoResponse {
    let addrs = if target == "all" {
        state.cluster_addrs().await
    } else {
        resolve_target(
            &target,
            state.cfg.connection.cluster_port,
            &state.cfg.connection.seeds,
        )
        .await
    };

    let mut results = Vec::new();
    for addr in addrs {
        match admin_rpc(
            addr,
            AdminRequest::RestoreLatestSnapshot,
            state.tls.as_ref(),
        )
        .await
        {
            Ok(AdminResponse::RestoreResult {
                path,
                entries,
                duration_ms,
            }) => results.push(serde_json::json!({
                "addr": addr.to_string(),
                "ok": true,
                "path": path,
                "entries": entries,
                "duration_ms": duration_ms
            })),
            Ok(AdminResponse::NotFound) => results.push(serde_json::json!({
                "addr": addr.to_string(),
                "ok": false,
                "error": "no snapshot found"
            })),
            Ok(AdminResponse::Error { message }) => results.push(serde_json::json!({
                "addr": addr.to_string(),
                "ok": false,
                "error": message
            })),
            Err(e) => results.push(serde_json::json!({
                "addr": addr.to_string(),
                "ok": false,
                "error": e.to_string()
            })),
            _ => results.push(serde_json::json!({
                "addr": addr.to_string(),
                "ok": false,
                "error": "unexpected response"
            })),
        }
    }
    Json(results)
}

// ---------------------------------------------------------------------------
