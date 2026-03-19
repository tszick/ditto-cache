//! Cache management endpoints.
//!
//! Admin operations (stats, flush, list keys, set-compressed) go via the
//! **admin TCP protocol** (port 7779).  Cache data operations (get/set/delete key)
//! are **proxied** to the node's HTTP REST port (cluster_port − 1, default 7778).
//!
//! | Method | Path | Description |
//! |--------|------|-------------|
//! | GET    | `/api/cache/:target/stats`              | Cache statistics |
//! | POST   | `/api/cache/:target/flush`              | Flush all keys |
//! | GET    | `/api/cache/:target/keys`               | List keys (optional `?pattern=`) |
//! | GET    | `/api/cache/:target/keys/:key`          | Get value (proxied) |
//! | PUT    | `/api/cache/:target/keys/:key`          | Set value (proxied) |
//! | DELETE | `/api/cache/:target/keys/:key`          | Delete key (proxied) |
//! | POST   | `/api/cache/:target/keys/:key/compressed` | Set compression flag |
//! | POST   | `/api/cache/:target/ttl`                  | Set TTL by key pattern | |

use crate::api::SharedState;
use crate::node_client::{admin_rpc, http_port_for, resolve_target};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use ditto_protocol::{AdminRequest, AdminResponse};
use serde::{Deserialize, Serialize};

/// Validate that a cache key contains only an allowlisted set of characters.
/// This prevents malformed or control characters from affecting proxied URLs.
fn is_valid_cache_key(key: &str) -> bool {
    if key.is_empty() {
        return false;
    }
    key.chars().all(|c| {
        c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':' | '/')
    })
}

// ---------------------------------------------------------------------------
// GET /api/cache/:target/stats
// ---------------------------------------------------------------------------

/// `GET /api/cache/:target/stats` — Cache statistics for one or all nodes.
pub async fn cache_stats(
    State(state): State<SharedState>,
    Path(target): Path<String>,
) -> impl IntoResponse {
    let addrs = if target == "all" {
        state.cluster_addrs().await
    } else {
        resolve_target(&target, state.cfg.connection.cluster_port, &state.cfg.connection.seeds).await
    };

    let mut results = Vec::new();
    for addr in addrs {
        match admin_rpc(addr, AdminRequest::GetStats, state.tls.as_ref()).await {
            Ok(AdminResponse::Stats(s)) => {
                let hit_rate = if s.hit_count + s.miss_count > 0 {
                    s.hit_count * 100 / (s.hit_count + s.miss_count)
                } else { 0 };
                results.push(serde_json::json!({
                    "addr":               addr.to_string(),
                    "key_count":          s.key_count,
                    "memory_used_bytes":  s.memory_used_bytes,
                    "memory_max_bytes":   s.memory_max_bytes,
                    "evictions":          s.evictions,
                    "hit_count":          s.hit_count,
                    "miss_count":         s.miss_count,
                    "hit_rate_pct":       hit_rate,
                }));
            }
            Err(e) => results.push(serde_json::json!({ "addr": addr.to_string(), "error": e.to_string() })),
            _ => {}
        }
    }
    Json(results)
}

// ---------------------------------------------------------------------------
// POST /api/cache/:target/flush
// ---------------------------------------------------------------------------

/// `POST /api/cache/:target/flush` — Evict all keys from the cache.
///
/// Use `target = "all"` to flush every node simultaneously.
/// **Irreversible** — data is lost; use with caution in production.
pub async fn flush_cache(
    State(state): State<SharedState>,
    Path(target): Path<String>,
) -> impl IntoResponse {
    let addrs = if target == "all" {
        state.cluster_addrs().await
    } else {
        resolve_target(&target, state.cfg.connection.cluster_port, &state.cfg.connection.seeds).await
    };

    let mut results = Vec::new();
    for addr in addrs {
        match admin_rpc(addr, AdminRequest::FlushCache, state.tls.as_ref()).await {
            Ok(_)  => results.push(serde_json::json!({ "addr": addr.to_string(), "ok": true })),
            Err(e) => results.push(serde_json::json!({ "addr": addr.to_string(), "ok": false, "error": e.to_string() })),
        }
    }
    Json(results)
}

// ---------------------------------------------------------------------------
// GET /api/cache/:target/keys?pattern=...
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct ListKeysQuery {
    pub pattern: Option<String>,
}

/// `GET /api/cache/:target/keys[?pattern=glob]` — List cache keys.
///
/// An optional `pattern` query parameter filters keys using shell-glob syntax
/// (e.g. `user:*`).
pub async fn list_keys(
    State(state): State<SharedState>,
    Path(target): Path<String>,
    Query(q): Query<ListKeysQuery>,
) -> impl IntoResponse {
    let addrs = if target == "all" {
        state.cluster_addrs().await
    } else {
        resolve_target(&target, state.cfg.connection.cluster_port, &state.cfg.connection.seeds).await
    };

    let mut results = Vec::new();
    for addr in addrs {
        match admin_rpc(addr, AdminRequest::ListKeys { pattern: q.pattern.clone() }, state.tls.as_ref()).await {
            Ok(AdminResponse::Keys(keys)) =>
                results.push(serde_json::json!({ "addr": addr.to_string(), "keys": keys })),
            Err(e) =>
                results.push(serde_json::json!({ "addr": addr.to_string(), "error": e.to_string() })),
            _ => {}
        }
    }
    Json(results)
}

// ---------------------------------------------------------------------------
// GET /api/cache/:target/keys/:key
// Proxied to node HTTP port (cluster_port - 1)
// ---------------------------------------------------------------------------

/// `GET /api/cache/:target/keys/:key` — Read a cache value (proxied to node HTTP port).
pub async fn get_key(
    State(state): State<SharedState>,
    Path((target, key)): Path<(String, String)>,
) -> impl IntoResponse {
    let addrs = resolve_target(&target, state.cfg.connection.cluster_port, &state.cfg.connection.seeds).await;
    let addr = match addrs.first() {
        Some(a) => *a,
        None => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "invalid target" }))).into_response(),
    };

    let http_addr = http_port_for(addr, state.cfg.connection.cluster_port);
    let url = format!("{}://{}/key/{}", state.http_scheme(), http_addr, key);

    match node_http_request(state.http_client.get(&url), &state).send().await {
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            if status.is_success() {
                (StatusCode::OK, Json(serde_json::json!({ "value": body }))).into_response()
            } else {
                (StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY),
                 Json(serde_json::json!({ "error": body }))).into_response()
            }
        }
        Err(e) => (StatusCode::BAD_GATEWAY, Json(serde_json::json!({ "error": e.to_string() }))).into_response(),
    }
}

// ---------------------------------------------------------------------------
// PUT /api/cache/:target/keys/:key   body: { "value": "...", "ttl_secs": null }
// Proxied to node HTTP port
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct SetKeyBody {
    pub value:    String,
    pub ttl_secs: Option<u64>,
}

/// `PUT /api/cache/:target/keys/:key` — Write a cache value (proxied to node HTTP port).
///
/// Body: `{ "value": "...", "ttl_secs": null|N }`
pub async fn set_key(
    State(state): State<SharedState>,
    Path((target, key)): Path<(String, String)>,
    Json(body): Json<SetKeyBody>,
) -> impl IntoResponse {
    if !is_valid_cache_key(&key) {
        return (StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "invalid key" }))).into_response();
    }

    let addrs = resolve_target(&target, state.cfg.connection.cluster_port, &state.cfg.connection.seeds).await;
    let addr = match addrs.first() {
        Some(a) => *a,
        None => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "invalid target" }))).into_response(),
    };

    let http_addr = http_port_for(addr, state.cfg.connection.cluster_port);
    let mut url = format!("{}://{}/key/{}", state.http_scheme(), http_addr, key);
    if let Some(ttl) = body.ttl_secs {
        url.push_str(&format!("?ttl={}", ttl));
    }

    match node_http_request(state.http_client.put(&url), &state).body(body.value).send().await {
        Ok(resp) if resp.status().is_success() =>
            (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response(),
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            (StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY),
             Json(serde_json::json!({ "error": body }))).into_response()
        }
        Err(e) => (StatusCode::BAD_GATEWAY, Json(serde_json::json!({ "error": e.to_string() }))).into_response(),
    }
}

// ---------------------------------------------------------------------------
// DELETE /api/cache/:target/keys/:key
// Proxied to node HTTP port
// ---------------------------------------------------------------------------

/// `DELETE /api/cache/:target/keys/:key` — Delete a cache key (proxied to node HTTP port).
pub async fn delete_key(
    State(state): State<SharedState>,
    Path((target, key)): Path<(String, String)>,
) -> impl IntoResponse {
    if !is_valid_cache_key(&key) {
        return (StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "invalid key" }))).into_response();
    }

    let addrs = resolve_target(&target, state.cfg.connection.cluster_port, &state.cfg.connection.seeds).await;
    let addr = match addrs.first() {
        Some(a) => *a,
        None => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "invalid target" }))).into_response(),
    };

    let http_addr = http_port_for(addr, state.cfg.connection.cluster_port);
    let url = format!("{}://{}/key/{}", state.http_scheme(), http_addr, key);

    match node_http_request(state.http_client.delete(&url), &state).send().await {
        Ok(resp) if resp.status().is_success() =>
            (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response(),
        Ok(resp) => {
            let s = resp.status();
            let body = resp.text().await.unwrap_or_default();
            (StatusCode::from_u16(s.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY),
             Json(serde_json::json!({ "error": body }))).into_response()
        }
        Err(e) => (StatusCode::BAD_GATEWAY, Json(serde_json::json!({ "error": e.to_string() }))).into_response(),
    }
}

// ---------------------------------------------------------------------------
// POST /api/cache/:target/keys/:key/compressed   body: { "compressed": bool }
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct SetCompressedBody {
    pub compressed: bool,
}

#[derive(Serialize)]
struct CompressedResult {
    addr:  String,
    ok:    bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// `POST /api/cache/:target/keys/:key/compressed` — Toggle LZ4 compression for a key.
///
/// Body: `{ "compressed": true | false }`
pub async fn set_compressed(
    State(state): State<SharedState>,
    Path((target, key)): Path<(String, String)>,
    Json(body): Json<SetCompressedBody>,
) -> impl IntoResponse {
    let addrs = if target == "all" {
        state.cluster_addrs().await
    } else {
        resolve_target(&target, state.cfg.connection.cluster_port, &state.cfg.connection.seeds).await
    };

    let value = if body.compressed { "true" } else { "false" }.to_string();
    let mut results: Vec<CompressedResult> = Vec::new();

    for addr in addrs {
        match admin_rpc(addr, AdminRequest::SetKeyProperty {
            key:   key.clone(),
            name:  "compressed".into(),
            value: value.clone(),
        }, state.tls.as_ref()).await {
            Ok(AdminResponse::KeyPropertyUpdated) =>
                results.push(CompressedResult { addr: addr.to_string(), ok: true, error: None }),
            Ok(AdminResponse::NotFound) =>
                results.push(CompressedResult { addr: addr.to_string(), ok: false, error: Some("key not found".into()) }),
            Ok(AdminResponse::Error { message }) =>
                results.push(CompressedResult { addr: addr.to_string(), ok: false, error: Some(message) }),
            Err(e) =>
                results.push(CompressedResult { addr: addr.to_string(), ok: false, error: Some(e.to_string()) }),
            _ =>
                results.push(CompressedResult { addr: addr.to_string(), ok: false, error: Some("unexpected response".into()) }),
        }
    }
    Json(results)
}

// ---------------------------------------------------------------------------
// POST /api/cache/:target/ttl   body: { "pattern": "glob", "ttl_secs": N|null }
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct SetTtlBody {
    pub pattern:  String,
    pub ttl_secs: Option<u64>,
}

#[derive(Serialize)]
struct TtlResult {
    addr:    String,
    updated: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    error:   Option<String>,
}

/// `POST /api/cache/:target/ttl` — Set TTL for all keys matching a glob pattern.
///
/// Body: `{ "pattern": "user:*", "ttl_secs": 3600 }` (`ttl_secs: null` removes TTL)
///
/// Use `target = "all"` to update every node simultaneously.
pub async fn set_keys_ttl(
    State(state): State<SharedState>,
    Path(target): Path<String>,
    Json(body): Json<SetTtlBody>,
) -> impl IntoResponse {
    let addrs = if target == "all" {
        state.cluster_addrs().await
    } else {
        resolve_target(&target, state.cfg.connection.cluster_port, &state.cfg.connection.seeds).await
    };

    let mut results: Vec<TtlResult> = Vec::new();
    for addr in addrs {
        match admin_rpc(addr, AdminRequest::SetKeysTtl {
            pattern:  body.pattern.clone(),
            ttl_secs: body.ttl_secs,
        }, state.tls.as_ref()).await {
            Ok(AdminResponse::TtlUpdated { updated }) =>
                results.push(TtlResult { addr: addr.to_string(), updated, error: None }),
            Err(e) =>
                results.push(TtlResult { addr: addr.to_string(), updated: 0, error: Some(e.to_string()) }),
            _ =>
                results.push(TtlResult { addr: addr.to_string(), updated: 0, error: Some("unexpected response".into()) }),
        }
    }
    Json(results)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Apply HTTP Basic Auth credentials (from `[http_client_auth]` config) to a
/// reqwest `RequestBuilder` when proxying requests to dittod's HTTP port (7778).
/// When no credentials are configured, the builder is returned unchanged.
fn node_http_request(
    req: reqwest::RequestBuilder,
    state: &crate::api::AppState,
) -> reqwest::RequestBuilder {
    match (&state.cfg.http_client_auth.username, &state.cfg.http_client_auth.password) {
        (Some(user), Some(pass)) => req.basic_auth(user, Some(pass)),
        _ => req,
    }
}
