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
    pub persistence_enabled: Option<bool>,
    pub persistence_backup_enabled: Option<bool>,
    pub persistence_export_enabled: Option<bool>,
    pub persistence_import_enabled: Option<bool>,
    pub rate_limit_enabled: Option<bool>,
    pub rate_limited_requests_total: Option<u64>,
    pub hot_key_enabled: Option<bool>,
    pub hot_key_coalesced_hits_total: Option<u64>,
    pub hot_key_fallback_exec_total: Option<u64>,
    pub circuit_breaker_enabled: Option<bool>,
    pub circuit_breaker_state: Option<String>,
    pub circuit_breaker_open_total: Option<u64>,
    pub circuit_breaker_reject_total: Option<u64>,
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
            persistence_enabled: Some(s.persistence_enabled),
            persistence_backup_enabled: Some(s.persistence_backup_enabled),
            persistence_export_enabled: Some(s.persistence_export_enabled),
            persistence_import_enabled: Some(s.persistence_import_enabled),
            rate_limit_enabled: Some(s.rate_limit_enabled),
            rate_limited_requests_total: Some(s.rate_limited_requests_total),
            hot_key_enabled: Some(s.hot_key_enabled),
            hot_key_coalesced_hits_total: Some(s.hot_key_coalesced_hits_total),
            hot_key_fallback_exec_total: Some(s.hot_key_fallback_exec_total),
            circuit_breaker_enabled: Some(s.circuit_breaker_enabled),
            circuit_breaker_state: Some(s.circuit_breaker_state),
            circuit_breaker_open_total: Some(s.circuit_breaker_open_total),
            circuit_breaker_reject_total: Some(s.circuit_breaker_reject_total),
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
            persistence_enabled: None,
            persistence_backup_enabled: None,
            persistence_export_enabled: None,
            persistence_import_enabled: None,
            rate_limit_enabled: None,
            rate_limited_requests_total: None,
            hot_key_enabled: None,
            hot_key_coalesced_hits_total: None,
            hot_key_fallback_exec_total: None,
            circuit_breaker_enabled: None,
            circuit_breaker_state: None,
            circuit_breaker_open_total: None,
            circuit_breaker_reject_total: None,
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

// ---------------------------------------------------------------------------
