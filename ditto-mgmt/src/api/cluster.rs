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
        if let Ok(AdminResponse::Stats(s)) =
            admin_rpc(addr, AdminRequest::GetStats, tls).await
        {
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
    pub total:    usize,
    pub active:   usize,
    pub syncing:  usize,
    pub inactive: usize,
    pub offline:  usize,
    pub primary:  Option<String>,
    pub nodes:    Vec<ClusterNodeSummary>,
}

#[derive(Serialize)]
pub struct ClusterNodeSummary {
    pub id:           String,
    pub status:       String,
    pub is_primary:   bool,
    pub last_applied: u64,
}

/// `GET /api/cluster` — Aggregate cluster overview.
///
/// Returns counts by status (active, syncing, inactive, offline),
/// the current primary ID, and a summary row per node.
pub async fn cluster_status(State(state): State<SharedState>) -> impl IntoResponse {
    let seed = state.cfg.connection.seeds.first().cloned()
        .unwrap_or_else(|| format!("127.0.0.1:{}", state.cfg.connection.cluster_port));
    let addrs = resolve_target(&seed, state.cfg.connection.cluster_port, &state.cfg.connection.seeds).await;
    let addr = match addrs.first() {
        Some(a) => *a,
        None => return Json(serde_json::json!({ "error": "no seed configured" })).into_response(),
    };

    let all_addrs = state.cluster_addrs().await;
    let primary_id = find_primary_id(&all_addrs, state.tls.as_ref()).await;

    match admin_rpc(addr, AdminRequest::ClusterStatus, state.tls.as_ref()).await {
        Ok(AdminResponse::ClusterView(nodes)) => {
            let active   = nodes.iter().filter(|n| n.status == NodeStatus::Active).count();
            let syncing  = nodes.iter().filter(|n| n.status == NodeStatus::Syncing).count();
            let inactive = nodes.iter().filter(|n| n.status == NodeStatus::Inactive).count();
            let offline  = nodes.iter().filter(|n| n.status == NodeStatus::Offline).count();

            let summaries: Vec<_> = nodes.iter().map(|n| {
                let is_primary = primary_id.as_deref()
                    .map(|pid| pid == n.id.to_string())
                    .unwrap_or(false);
                ClusterNodeSummary {
                    id:           n.id.to_string(),
                    status:       format!("{:?}", n.status),
                    is_primary,
                    last_applied: n.last_applied,
                }
            }).collect();

            Json(serde_json::to_value(ClusterStatusResponse {
                total:    nodes.len(),
                active,
                syncing,
                inactive,
                offline,
                primary:  primary_id,
                nodes:    summaries,
            }).unwrap()).into_response()
        }
        Err(e) => Json(serde_json::json!({ "error": e.to_string() })).into_response(),
        _      => Json(serde_json::json!({ "error": "unexpected response" })).into_response(),
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
