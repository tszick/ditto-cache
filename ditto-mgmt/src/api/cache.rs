//! Cache management endpoints.
//!
//! Admin operations (stats, flush, list keys, set-compressed) go via the
//! **admin TCP protocol** (port 7779).  Cache data operations (get/set/delete key)
//! are **proxied** to the node's HTTP REST port (cluster_port − 1, default 7778).
//!
//! | Method | Path | Description |
//! |--------|------|-------------|
//! | GET    | `/api/cache/:target/stats`              | Cache statistics |
//! | POST   | `/api/cache/:target/flush`              | Flush all keys, or one namespace with `?namespace=` |
//! | GET    | `/api/cache/:target/keys`               | List keys (optional `?pattern=`) |
//! | GET    | `/api/cache/:target/keys/:key`          | Get value (proxied) |
//! | PUT    | `/api/cache/:target/keys/:key`          | Set value (proxied) |
//! | DELETE | `/api/cache/:target/keys/:key`          | Delete key (proxied) |
//! | POST   | `/api/cache/:target/keys/:key/compressed` | Set compression flag |
//! | POST   | `/api/cache/:target/ttl`                  | Set TTL by key pattern | |

use crate::api::SharedState;
use crate::node_client::{admin_rpc, http_authority_for_target, resolve_target};
use axum::{
    extract::{Path, Query, State},
    http::HeaderValue,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use ditto_protocol::{AdminRequest, AdminResponse};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Validate that a cache key contains only an allowlisted set of characters.
/// This prevents malformed or control characters from affecting proxied URLs.
///
/// The forward-slash (`/`) is intentionally excluded: allowing it would let a
/// crafted key traverse path segments in the proxied URL (e.g. `../admin`).
/// Keys that need a hierarchy separator should use `:` or `_` instead.
fn is_valid_cache_key(key: &str) -> bool {
    if key.is_empty() {
        return false;
    }
    key.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':'))
}

/// Percent-encode a validated cache key for safe embedding in a URL path.
/// Only the characters allowed by [`is_valid_cache_key`] need to be handled;
/// colons are encoded so they cannot be misread as a port separator.
fn encode_key_for_url(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for b in key.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' => out.push(b as char),
            b => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

const NAMESPACE_HEADER: &str = "x-ditto-namespace";

#[derive(Deserialize, Default, Clone)]
pub struct NamespaceQuery {
    pub namespace: Option<String>,
}

#[derive(Deserialize, Default, Clone)]
pub struct GetKeyQuery {
    pub namespace: Option<String>,
    #[serde(default)]
    pub reveal: bool,
}

#[derive(Debug, Clone, Serialize)]
struct CacheValueResponse {
    key: String,
    namespace: Option<String>,
    length_bytes: usize,
    sha256: String,
    preview: String,
    masked: bool,
    reveal_allowed: bool,
    reveal_blocked_reason: Option<String>,
    upstream_version: Option<u64>,
    sensitive: bool,
    sensitive_reasons: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
}

fn normalize_namespace(ns: Option<String>) -> Option<String> {
    ns.map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
}

fn namespaced_key(namespace: &Option<String>, key: &str) -> String {
    match namespace {
        Some(ns) => format!("{}::{}", ns, key),
        None => key.to_string(),
    }
}

fn namespaced_pattern(namespace: &Option<String>, pattern: Option<String>) -> Option<String> {
    match (namespace, pattern) {
        (Some(ns), Some(p)) => Some(format!("{}::{}", ns, p)),
        (Some(ns), None) => Some(format!("{}::*", ns)),
        (None, p) => p,
    }
}

fn strip_namespace_prefix(namespace: &Option<String>, key: String) -> String {
    match namespace {
        Some(ns) => key
            .strip_prefix(&format!("{}::", ns))
            .unwrap_or(&key)
            .to_string(),
        None => key,
    }
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
        resolve_target(
            &target,
            state.cfg.connection.cluster_port,
            &state.cfg.connection.seeds,
        )
        .await
    };

    let mut results = Vec::new();
    for addr in addrs {
        match admin_rpc(addr, AdminRequest::GetStats, state.tls.as_ref()).await {
            Ok(AdminResponse::Stats(s)) => {
                let hit_rate = if s.hit_count + s.miss_count > 0 {
                    s.hit_count * 100 / (s.hit_count + s.miss_count)
                } else {
                    0
                };
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
            Err(e) => results
                .push(serde_json::json!({ "addr": addr.to_string(), "error": e.to_string() })),
            _ => {}
        }
    }
    Json(results).into_response()
}

// ---------------------------------------------------------------------------
// POST /api/cache/:target/flush
// ---------------------------------------------------------------------------

/// `POST /api/cache/:target/flush` — Evict keys from the cache.
///
/// Use `target = "all"` to flush every node simultaneously.
/// Include `?namespace=<tenant>` to delete only keys in one namespace.
/// **Irreversible** — data is lost; use with caution in production.
pub async fn flush_cache(
    State(state): State<SharedState>,
    Path(target): Path<String>,
    Query(nsq): Query<NamespaceQuery>,
) -> impl IntoResponse {
    if let Some(namespace) = normalize_namespace(nsq.namespace) {
        return flush_namespace(state, target, namespace).await;
    }

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
        match admin_rpc(addr, AdminRequest::FlushCache, state.tls.as_ref()).await {
            Ok(_)  => results.push(serde_json::json!({ "addr": addr.to_string(), "ok": true })),
            Err(e) => results.push(serde_json::json!({ "addr": addr.to_string(), "ok": false, "error": e.to_string() })),
        }
    }
    Json(results).into_response()
}

async fn flush_namespace(
    state: SharedState,
    target: String,
    namespace: String,
) -> axum::response::Response {
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
        let authority = if target == "all" {
            cluster_addr_http_authority(addr)
        } else {
            http_authority_for_target(
                &target,
                state.cfg.connection.cluster_port,
                &state.cfg.connection.seeds,
            )
            .or_else(|| cluster_addr_http_authority(addr))
        };

        let Some(authority) = authority else {
            results.push(serde_json::json!({
                "addr": addr.to_string(),
                "ok": false,
                "deleted": 0,
                "namespace": namespace.clone(),
                "error": "invalid HTTP target"
            }));
            continue;
        };

        let url = format!(
            "{}://{}/keys/delete-by-pattern",
            state.http_scheme(),
            authority
        );
        match node_http_request(
            state.http_client.post(&url),
            &state,
            Some(namespace.clone()),
        )
        .json(&serde_json::json!({ "pattern": "*" }))
        .send()
        .await
        {
            Ok(resp) if resp.status().is_success() => {
                let body = resp.json::<serde_json::Value>().await.unwrap_or_default();
                results.push(serde_json::json!({
                    "addr": addr.to_string(),
                    "ok": true,
                    "deleted": body.get("deleted").and_then(|v| v.as_u64()).unwrap_or(0),
                    "namespace": namespace.clone(),
                    "error": null
                }));
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                results.push(serde_json::json!({
                    "addr": addr.to_string(),
                    "ok": false,
                    "deleted": 0,
                    "namespace": namespace.clone(),
                    "error": format!("HTTP {}: {}", status.as_u16(), body)
                }));
            }
            Err(e) => results.push(serde_json::json!({
                "addr": addr.to_string(),
                "ok": false,
                "deleted": 0,
                "namespace": namespace.clone(),
                "error": e.to_string()
            })),
        }
    }
    Json(results).into_response()
}

fn cluster_addr_http_authority(addr: std::net::SocketAddr) -> Option<String> {
    addr.port()
        .checked_sub(1)
        .map(|http_port| format!("{}:{}", addr.ip(), http_port))
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
    Query(nsq): Query<NamespaceQuery>,
) -> impl IntoResponse {
    let namespace = normalize_namespace(nsq.namespace);
    let pattern = namespaced_pattern(&namespace, q.pattern.clone());
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
            AdminRequest::ListKeys {
                pattern: pattern.clone(),
            },
            state.tls.as_ref(),
        )
        .await
        {
            Ok(AdminResponse::Keys(keys)) => {
                let keys: Vec<String> = keys
                    .into_iter()
                    .map(|k| strip_namespace_prefix(&namespace, k))
                    .collect();
                results.push(serde_json::json!({ "addr": addr.to_string(), "keys": keys }))
            }
            Err(e) => results
                .push(serde_json::json!({ "addr": addr.to_string(), "error": e.to_string() })),
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
    Query(q): Query<GetKeyQuery>,
) -> impl IntoResponse {
    // Validate the key before it is embedded in the proxied URL.  set_key and
    // delete_key already perform this check; apply it here for consistency and
    // to prevent path-traversal / URL-injection (CodeQL rust/request-forgery #2).
    if !is_valid_cache_key(&key) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "invalid key" })),
        )
            .into_response();
    }

    let namespace = normalize_namespace(q.namespace);
    let authority = match http_authority_for_target(
        &target,
        state.cfg.connection.cluster_port,
        &state.cfg.connection.seeds,
    ) {
        Some(a) => a,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "invalid target" })),
            )
                .into_response()
        }
    };

    let url = format!(
        "{}://{}/key/{}",
        state.http_scheme(),
        authority,
        encode_key_for_url(&key)
    );

    match node_http_request(state.http_client.get(&url), &state, namespace.clone())
        .send()
        .await
    {
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            if status.is_success() {
                let (parsed_value, upstream_version) = parse_node_get_value(&body);
                let value = parsed_value.as_deref().unwrap_or(&body);
                let value = build_cache_value_response(
                    &key,
                    namespace.as_deref(),
                    value,
                    q.reveal,
                    upstream_version,
                    &state.cfg.cache_values,
                );
                (StatusCode::OK, Json(value)).into_response()
            } else {
                (
                    StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY),
                    Json(serde_json::json!({ "error": body })),
                )
                    .into_response()
            }
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

fn build_cache_value_response(
    key: &str,
    namespace: Option<&str>,
    value: &str,
    reveal_requested: bool,
    upstream_version: Option<u64>,
    policy: &crate::config::CacheValuePolicy,
) -> CacheValueResponse {
    let sensitive_reasons = sensitive_reasons(key, value, policy);
    let sensitive = !sensitive_reasons.is_empty();
    let reveal_allowed =
        policy.allow_value_reveal && (!sensitive || policy.allow_sensitive_value_reveal);
    let reveal_blocked_reason = if reveal_requested && !policy.allow_value_reveal {
        Some("value reveal is disabled by policy".to_string())
    } else if reveal_requested && sensitive && !policy.allow_sensitive_value_reveal {
        Some("sensitive value reveal is disabled by policy".to_string())
    } else {
        None
    };
    let should_reveal = reveal_requested && reveal_allowed;
    let masked = policy.mask_values_by_default && !should_reveal;

    CacheValueResponse {
        key: key.to_string(),
        namespace: namespace.map(str::to_string),
        length_bytes: value.len(),
        sha256: sha256_hex(value),
        preview: masked_preview(value),
        masked,
        reveal_allowed,
        reveal_blocked_reason,
        upstream_version,
        sensitive,
        sensitive_reasons,
        value: should_reveal.then(|| value.to_string()),
    }
}

fn parse_node_get_value(body: &str) -> (Option<String>, Option<u64>) {
    let Ok(json) = serde_json::from_str::<serde_json::Value>(body) else {
        return (None, None);
    };
    let value = json
        .get("value")
        .and_then(|value| value.as_str())
        .map(str::to_string);
    let version = json.get("version").and_then(|value| value.as_u64());
    (value, version)
}

fn sha256_hex(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    digest.iter().map(|b| format!("{:02x}", b)).collect()
}

fn masked_preview(value: &str) -> String {
    let chars: Vec<char> = value.chars().collect();
    if chars.is_empty() {
        return "".into();
    }
    if chars.len() <= 8 {
        return "***".into();
    }
    let start: String = chars.iter().take(4).collect();
    let end: String = chars
        .iter()
        .rev()
        .take(4)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("{start}...***...{end}")
}

fn sensitive_reasons(
    key: &str,
    value: &str,
    policy: &crate::config::CacheValuePolicy,
) -> Vec<String> {
    let mut reasons = Vec::new();
    let key_lower = key.to_ascii_lowercase();
    for pattern in &policy.sensitive_key_patterns {
        let pattern = pattern.to_ascii_lowercase();
        if !pattern.is_empty() && key_lower.contains(&pattern) {
            reasons.push(format!("key matched '{pattern}'"));
        }
    }

    let value_lower = value.to_ascii_lowercase();
    for marker in [
        "access_token",
        "refresh_token",
        "session_id",
        "id_token",
        "client_secret",
        "password",
        "api_key",
        "authorization",
        "bearer ",
    ] {
        if value_lower.contains(marker) {
            reasons.push(format!("value contains '{marker}'"));
        }
    }
    if looks_like_jwt(value) {
        reasons.push("value looks like a JWT".into());
    }
    if looks_like_secret_token(value) {
        reasons.push("value looks like a high-entropy token".into());
    }
    reasons.sort();
    reasons.dedup();
    reasons
}

fn looks_like_jwt(value: &str) -> bool {
    let parts: Vec<&str> = value.trim().split('.').collect();
    parts.len() == 3
        && parts.iter().all(|p| {
            p.len() >= 8
                && p.chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        })
}

fn looks_like_secret_token(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.len() < 32 || trimmed.len() > 4096 || trimmed.contains(char::is_whitespace) {
        return false;
    }
    let token_chars = trimmed
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '+' | '/' | '='))
        .count();
    token_chars == trimmed.chars().count()
}

// ---------------------------------------------------------------------------
// PUT /api/cache/:target/keys/:key   body: { "value": "...", "ttl_secs": null }
// Proxied to node HTTP port
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct SetKeyBody {
    pub value: String,
    pub ttl_secs: Option<u64>,
}

/// `PUT /api/cache/:target/keys/:key` — Write a cache value (proxied to node HTTP port).
///
/// Body: `{ "value": "...", "ttl_secs": null|N }`
pub async fn set_key(
    State(state): State<SharedState>,
    Path((target, key)): Path<(String, String)>,
    Query(nsq): Query<NamespaceQuery>,
    Json(body): Json<SetKeyBody>,
) -> impl IntoResponse {
    if !is_valid_cache_key(&key) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "invalid key" })),
        )
            .into_response();
    }

    let namespace = normalize_namespace(nsq.namespace);
    let authority = match http_authority_for_target(
        &target,
        state.cfg.connection.cluster_port,
        &state.cfg.connection.seeds,
    ) {
        Some(a) => a,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "invalid target" })),
            )
                .into_response()
        }
    };

    let mut url = format!(
        "{}://{}/key/{}",
        state.http_scheme(),
        authority,
        encode_key_for_url(&key)
    );
    if let Some(ttl) = body.ttl_secs {
        url.push_str(&format!("?ttl={}", ttl));
    }

    match node_http_request(state.http_client.put(&url), &state, namespace)
        .body(body.value)
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {
            (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response()
        }
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            (
                StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY),
                Json(serde_json::json!({ "error": body })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
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
    Query(nsq): Query<NamespaceQuery>,
) -> impl IntoResponse {
    if !is_valid_cache_key(&key) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "invalid key" })),
        )
            .into_response();
    }

    let namespace = normalize_namespace(nsq.namespace);
    let authority = match http_authority_for_target(
        &target,
        state.cfg.connection.cluster_port,
        &state.cfg.connection.seeds,
    ) {
        Some(a) => a,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "invalid target" })),
            )
                .into_response()
        }
    };

    let url = format!(
        "{}://{}/key/{}",
        state.http_scheme(),
        authority,
        encode_key_for_url(&key)
    );

    match node_http_request(state.http_client.delete(&url), &state, namespace)
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {
            (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response()
        }
        Ok(resp) => {
            let s = resp.status();
            let body = resp.text().await.unwrap_or_default();
            (
                StatusCode::from_u16(s.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY),
                Json(serde_json::json!({ "error": body })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
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
    addr: String,
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// `POST /api/cache/:target/keys/:key/compressed` — Toggle LZ4 compression for a key.
///
/// Body: `{ "compressed": true | false }`
pub async fn set_compressed(
    State(state): State<SharedState>,
    Path((target, key)): Path<(String, String)>,
    Query(nsq): Query<NamespaceQuery>,
    Json(body): Json<SetCompressedBody>,
) -> impl IntoResponse {
    if !is_valid_cache_key(&key) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "invalid key" })),
        )
            .into_response();
    }

    let namespace = normalize_namespace(nsq.namespace);
    let key = namespaced_key(&namespace, &key);
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

    let value = if body.compressed { "true" } else { "false" }.to_string();
    let mut results: Vec<CompressedResult> = Vec::new();

    for addr in addrs {
        match admin_rpc(
            addr,
            AdminRequest::SetKeyProperty {
                key: key.clone(),
                name: "compressed".into(),
                value: value.clone(),
            },
            state.tls.as_ref(),
        )
        .await
        {
            Ok(AdminResponse::KeyPropertyUpdated) => results.push(CompressedResult {
                addr: addr.to_string(),
                ok: true,
                error: None,
            }),
            Ok(AdminResponse::NotFound) => results.push(CompressedResult {
                addr: addr.to_string(),
                ok: false,
                error: Some("key not found".into()),
            }),
            Ok(AdminResponse::Error { message }) => results.push(CompressedResult {
                addr: addr.to_string(),
                ok: false,
                error: Some(message),
            }),
            Err(e) => results.push(CompressedResult {
                addr: addr.to_string(),
                ok: false,
                error: Some(e.to_string()),
            }),
            _ => results.push(CompressedResult {
                addr: addr.to_string(),
                ok: false,
                error: Some("unexpected response".into()),
            }),
        }
    }
    Json(results).into_response()
}

// ---------------------------------------------------------------------------
// POST /api/cache/:target/ttl   body: { "pattern": "glob", "ttl_secs": N|null }
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct SetTtlBody {
    pub pattern: String,
    pub ttl_secs: Option<u64>,
}

#[derive(Serialize)]
struct TtlResult {
    addr: String,
    updated: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// `POST /api/cache/:target/ttl` — Set TTL for all keys matching a glob pattern.
///
/// Body: `{ "pattern": "user:*", "ttl_secs": 3600 }` (`ttl_secs: null` removes TTL)
///
/// Use `target = "all"` to update every node simultaneously.
pub async fn set_keys_ttl(
    State(state): State<SharedState>,
    Path(target): Path<String>,
    Query(nsq): Query<NamespaceQuery>,
    Json(body): Json<SetTtlBody>,
) -> impl IntoResponse {
    let namespace = normalize_namespace(nsq.namespace);
    let pattern = namespaced_pattern(&namespace, Some(body.pattern.clone()))
        .unwrap_or_else(|| body.pattern.clone());
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

    let mut results: Vec<TtlResult> = Vec::new();
    for addr in addrs {
        match admin_rpc(
            addr,
            AdminRequest::SetKeysTtl {
                pattern: pattern.clone(),
                ttl_secs: body.ttl_secs,
            },
            state.tls.as_ref(),
        )
        .await
        {
            Ok(AdminResponse::TtlUpdated { updated }) => results.push(TtlResult {
                addr: addr.to_string(),
                updated,
                error: None,
            }),
            Err(e) => results.push(TtlResult {
                addr: addr.to_string(),
                updated: 0,
                error: Some(e.to_string()),
            }),
            _ => results.push(TtlResult {
                addr: addr.to_string(),
                updated: 0,
                error: Some("unexpected response".into()),
            }),
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
    namespace: Option<String>,
) -> reqwest::RequestBuilder {
    let req = match (
        &state.cfg.http_client_auth.username,
        &state.cfg.http_client_auth.password,
    ) {
        (Some(user), Some(pass)) => req.basic_auth(user, Some(pass)),
        _ => req,
    };
    if let Some(ns) = namespace {
        if let Ok(v) = HeaderValue::from_str(&ns) {
            return req.header(NAMESPACE_HEADER, v);
        }
    }
    req
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{api::AppState, config::MgmtConfig};
    use axum::{
        body::to_bytes,
        response::{IntoResponse, Response},
    };
    use ditto_protocol::{decode, encode, ClusterMessage};
    use std::{net::SocketAddr, sync::Arc};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::Mutex,
    };

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

    fn state_with_cfg(cfg: MgmtConfig) -> SharedState {
        Arc::new(AppState {
            cfg: Arc::new(cfg),
            tls: None,
            http_client: reqwest::Client::new(),
            addr_cache: Mutex::new(None),
        })
    }

    async fn one_admin_response(
        response: AdminResponse,
    ) -> (SocketAddr, tokio::task::JoinHandle<AdminRequest>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
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

            let frame = encode(&ClusterMessage::AdminResponse(Box::new(response))).unwrap();
            stream.write_all(&frame).await.unwrap();
            request
        });

        (addr, handle)
    }

    async fn one_http_response(
        status: &str,
        body: &str,
    ) -> (SocketAddr, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let status = status.to_string();
        let body = body.to_string();

        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buf = [0u8; 512];
            loop {
                let n = stream.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..n]);
                if request.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }

            let header_end = request
                .windows(4)
                .position(|w| w == b"\r\n\r\n")
                .map(|pos| pos + 4)
                .unwrap_or(request.len());
            let header_text = String::from_utf8_lossy(&request[..header_end]);
            let content_len = header_text
                .lines()
                .find_map(|line| {
                    line.strip_prefix("content-length:")
                        .or_else(|| line.strip_prefix("Content-Length:"))
                        .and_then(|v| v.trim().parse::<usize>().ok())
                })
                .unwrap_or(0);
            while request.len().saturating_sub(header_end) < content_len {
                let n = stream.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..n]);
            }

            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            String::from_utf8_lossy(&request).to_string()
        });

        (addr, handle)
    }

    fn proxy_state_for_http_addr(http_addr: SocketAddr) -> SharedState {
        let cluster_port = http_addr.port() + 1;
        state_for_seed(format!("127.0.0.1:{cluster_port}"), cluster_port)
    }

    async fn json_body(response: Response) -> serde_json::Value {
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    #[test]
    fn cache_key_url_and_namespace_helpers_cover_edge_cases() {
        assert!(is_valid_cache_key("user:42.alpha_beta-1"));
        assert!(!is_valid_cache_key(""));
        assert!(!is_valid_cache_key("nested/key"));
        assert!(!is_valid_cache_key("white space"));

        assert_eq!(encode_key_for_url("user:42"), "user%3A42");
        assert_eq!(
            normalize_namespace(Some(" tenant ".into())).as_deref(),
            Some("tenant")
        );
        assert_eq!(normalize_namespace(Some("   ".into())), None);
        assert_eq!(
            namespaced_key(&Some("tenant".into()), "alpha"),
            "tenant::alpha"
        );
        assert_eq!(
            namespaced_pattern(&Some("tenant".into()), None).as_deref(),
            Some("tenant::*")
        );
        assert_eq!(
            namespaced_pattern(&None, Some("user:*".into())).as_deref(),
            Some("user:*")
        );
        assert_eq!(
            strip_namespace_prefix(&Some("tenant".into()), "tenant::alpha".into()),
            "alpha"
        );
        assert_eq!(
            strip_namespace_prefix(&Some("tenant".into()), "other::alpha".into()),
            "other::alpha"
        );
    }

    #[test]
    fn node_http_request_applies_optional_basic_auth_and_namespace_header() {
        let mut cfg = MgmtConfig::default();
        cfg.http_client_auth.username = Some("node-user".into());
        cfg.http_client_auth.password = Some("node-pass".into());
        let state = state_with_cfg(cfg);

        let request = node_http_request(
            state.http_client.get("http://127.0.0.1:7778/key/alpha"),
            &state,
            Some("tenant".into()),
        )
        .build()
        .unwrap();

        assert_eq!(
            request
                .headers()
                .get(reqwest::header::AUTHORIZATION)
                .unwrap(),
            "Basic bm9kZS11c2VyOm5vZGUtcGFzcw=="
        );
        assert_eq!(request.headers().get(NAMESPACE_HEADER).unwrap(), "tenant");

        let no_auth_state = state_for_seed("127.0.0.1:7779".into(), 7779);
        let request = node_http_request(
            no_auth_state
                .http_client
                .get("http://127.0.0.1:7778/key/alpha"),
            &no_auth_state,
            Some("bad\r\nnamespace".into()),
        )
        .build()
        .unwrap();
        assert!(request
            .headers()
            .get(reqwest::header::AUTHORIZATION)
            .is_none());
        assert!(request.headers().get(NAMESPACE_HEADER).is_none());
    }

    #[tokio::test]
    async fn list_keys_applies_namespace_pattern_and_strips_response_prefixes() {
        let (addr, handle) = one_admin_response(AdminResponse::Keys(vec![
            "tenant::alpha".into(),
            "tenant::nested::beta".into(),
            "other::gamma".into(),
        ]))
        .await;
        let state = state_for_seed(addr.to_string(), addr.port());

        let response = list_keys(
            State(state),
            Path(addr.to_string()),
            Query(ListKeysQuery {
                pattern: Some("*".into()),
            }),
            Query(NamespaceQuery {
                namespace: Some(" tenant ".into()),
            }),
        )
        .await
        .into_response();

        let request = handle.await.unwrap();
        assert!(matches!(
            request,
            AdminRequest::ListKeys {
                pattern: Some(ref pattern)
            } if pattern == "tenant::*"
        ));

        let body = json_body(response).await;
        assert_eq!(body[0]["addr"], addr.to_string());
        assert_eq!(
            body[0]["keys"],
            serde_json::json!(["alpha", "nested::beta", "other::gamma"])
        );
    }

    #[tokio::test]
    async fn flush_without_namespace_uses_admin_flush() {
        let (addr, handle) = one_admin_response(AdminResponse::Flushed).await;
        let state = state_for_seed(addr.to_string(), addr.port());

        let response = flush_cache(
            State(state),
            Path(addr.to_string()),
            Query(NamespaceQuery::default()),
        )
        .await
        .into_response();

        assert!(matches!(handle.await.unwrap(), AdminRequest::FlushCache));
        let body = json_body(response).await;
        assert_eq!(body[0]["ok"], true);
    }

    #[tokio::test]
    async fn flush_with_namespace_deletes_by_pattern_over_http() {
        let (http_addr, handle) = one_http_response("200 OK", r#"{"deleted":2}"#).await;
        let state = proxy_state_for_http_addr(http_addr);
        let target = format!("127.0.0.1:{}", http_addr.port() + 1);

        let response = flush_cache(
            State(state),
            Path(target),
            Query(NamespaceQuery {
                namespace: Some(" tenant-a ".into()),
            }),
        )
        .await
        .into_response();

        let request = handle.await.unwrap();
        assert!(request.starts_with("POST /keys/delete-by-pattern HTTP/1.1"));
        assert!(request.contains("x-ditto-namespace: tenant-a"));
        assert!(request.ends_with(r#"{"pattern":"*"}"#));

        let body = json_body(response).await;
        assert_eq!(body[0]["ok"], true);
        assert_eq!(body[0]["deleted"], 2);
        assert_eq!(body[0]["namespace"], "tenant-a");
    }

    #[tokio::test]
    async fn set_compressed_sends_namespaced_admin_property_update() {
        let (addr, handle) = one_admin_response(AdminResponse::KeyPropertyUpdated).await;
        let state = state_for_seed(addr.to_string(), addr.port());

        let response = set_compressed(
            State(state),
            Path((addr.to_string(), "alpha".into())),
            Query(NamespaceQuery {
                namespace: Some("tenant".into()),
            }),
            Json(SetCompressedBody { compressed: true }),
        )
        .await
        .into_response();

        let request = handle.await.unwrap();
        assert!(matches!(
            request,
            AdminRequest::SetKeyProperty {
                ref key,
                ref name,
                ref value
            } if key == "tenant::alpha" && name == "compressed" && value == "true"
        ));

        let body = json_body(response).await;
        assert_eq!(body[0]["ok"], true);
        assert_eq!(body[0]["error"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn set_compressed_formats_not_found_error_and_unexpected_response() {
        let (missing_addr, missing_handle) = one_admin_response(AdminResponse::NotFound).await;
        let missing_state = state_for_seed(missing_addr.to_string(), missing_addr.port());
        let response = set_compressed(
            State(missing_state),
            Path((missing_addr.to_string(), "alpha".into())),
            Query(NamespaceQuery::default()),
            Json(SetCompressedBody { compressed: false }),
        )
        .await
        .into_response();
        assert!(matches!(
            missing_handle.await.unwrap(),
            AdminRequest::SetKeyProperty { .. }
        ));
        let body = json_body(response).await;
        assert_eq!(body[0]["ok"], false);
        assert_eq!(body[0]["error"], "key not found");

        let (unexpected_addr, unexpected_handle) = one_admin_response(AdminResponse::Flushed).await;
        let unexpected_state = state_for_seed(unexpected_addr.to_string(), unexpected_addr.port());
        let response = set_compressed(
            State(unexpected_state),
            Path((unexpected_addr.to_string(), "alpha".into())),
            Query(NamespaceQuery::default()),
            Json(SetCompressedBody { compressed: false }),
        )
        .await
        .into_response();
        assert!(matches!(
            unexpected_handle.await.unwrap(),
            AdminRequest::SetKeyProperty { .. }
        ));
        let body = json_body(response).await;
        assert_eq!(body[0]["ok"], false);
        assert_eq!(body[0]["error"], "unexpected response");
    }

    #[tokio::test]
    async fn set_keys_ttl_sends_namespaced_pattern() {
        let (addr, handle) = one_admin_response(AdminResponse::TtlUpdated { updated: 3 }).await;
        let state = state_for_seed(addr.to_string(), addr.port());

        let response = set_keys_ttl(
            State(state),
            Path(addr.to_string()),
            Query(NamespaceQuery {
                namespace: Some("tenant".into()),
            }),
            Json(SetTtlBody {
                pattern: "session:*".into(),
                ttl_secs: Some(60),
            }),
        )
        .await
        .into_response();

        let request = handle.await.unwrap();
        assert!(matches!(
            request,
            AdminRequest::SetKeysTtl {
                ref pattern,
                ttl_secs: Some(60)
            } if pattern == "tenant::session:*"
        ));

        let body = json_body(response).await;
        assert_eq!(body[0]["updated"], 3);
        assert_eq!(body[0]["error"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn set_keys_ttl_formats_unexpected_response() {
        let (addr, handle) = one_admin_response(AdminResponse::Flushed).await;
        let state = state_for_seed(addr.to_string(), addr.port());

        let response = set_keys_ttl(
            State(state),
            Path(addr.to_string()),
            Query(NamespaceQuery::default()),
            Json(SetTtlBody {
                pattern: "*".into(),
                ttl_secs: None,
            }),
        )
        .await
        .into_response();

        assert!(matches!(
            handle.await.unwrap(),
            AdminRequest::SetKeysTtl { .. }
        ));
        let body = json_body(response).await;
        assert_eq!(body[0]["updated"], 0);
        assert_eq!(body[0]["error"], "unexpected response");
    }

    #[tokio::test]
    async fn get_key_proxies_successful_node_http_response() {
        let (http_addr, handle) = one_http_response("200 OK", "value-body").await;
        let state = proxy_state_for_http_addr(http_addr);

        let response = get_key(
            State(state),
            Path((
                format!("127.0.0.1:{}", http_addr.port() + 1),
                "user:42".into(),
            )),
            Query(GetKeyQuery {
                namespace: Some("tenant".into()),
                reveal: false,
            }),
        )
        .await
        .into_response();

        let request = handle.await.unwrap();
        assert!(request.starts_with("GET /key/user%3A42 HTTP/1.1"));
        assert!(request.contains("x-ditto-namespace: tenant"));
        let body = json_body(response).await;
        assert_eq!(body["key"], "user:42");
        assert_eq!(body["namespace"], "tenant");
        assert_eq!(body["length_bytes"], 10);
        assert_eq!(body["preview"], "valu...***...body");
        assert_eq!(body["masked"], true);
        assert_eq!(body["reveal_allowed"], false);
        assert_eq!(body["reveal_blocked_reason"], serde_json::Value::Null);
        assert_eq!(body["value"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn get_key_extracts_value_from_node_json_response() {
        let (http_addr, handle) =
            one_http_response("200 OK", r#"{"value":"access-token-body","version":42}"#).await;
        let state = proxy_state_for_http_addr(http_addr);

        let response = get_key(
            State(state),
            Path((
                format!("127.0.0.1:{}", http_addr.port() + 1),
                "access_token".into(),
            )),
            Query(GetKeyQuery {
                namespace: None,
                reveal: false,
            }),
        )
        .await
        .into_response();

        let request = handle.await.unwrap();
        assert!(request.starts_with("GET /key/access_token HTTP/1.1"));
        let body = json_body(response).await;
        assert_eq!(body["key"], "access_token");
        assert_eq!(body["length_bytes"], 17);
        assert_eq!(body["preview"], "acce...***...body");
        assert_eq!(body["upstream_version"], 42);
        assert_eq!(body["sensitive"], true);
        assert_eq!(body["value"], serde_json::Value::Null);
    }

    #[test]
    fn cache_value_response_masks_and_flags_sensitive_values() {
        let policy = crate::config::CacheValuePolicy::default();
        let body = build_cache_value_response(
            "access_token",
            Some("tenant"),
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature123",
            false,
            None,
            &policy,
        );

        assert!(body.masked);
        assert!(body.sensitive);
        assert!(!body.reveal_allowed);
        assert!(body.value.is_none());
        assert!(body.sensitive_reasons.iter().any(|r| r.contains("access")));
        assert!(body.sensitive_reasons.iter().any(|r| r.contains("JWT")));
    }

    #[test]
    fn cache_value_response_reveals_only_when_policy_allows() {
        let mut policy = crate::config::CacheValuePolicy::default();
        policy.allow_value_reveal = true;

        let body = build_cache_value_response("profile", None, "plain-value", true, None, &policy);

        assert!(!body.masked);
        assert!(body.reveal_allowed);
        assert_eq!(body.value.as_deref(), Some("plain-value"));
    }

    #[test]
    fn cache_value_response_blocks_sensitive_reveal_without_sensitive_policy() {
        let mut policy = crate::config::CacheValuePolicy::default();
        policy.allow_value_reveal = true;

        let body = build_cache_value_response(
            "session_token",
            None,
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature123",
            true,
            Some(7),
            &policy,
        );

        assert!(body.masked);
        assert!(!body.reveal_allowed);
        assert_eq!(body.upstream_version, Some(7));
        assert_eq!(
            body.reveal_blocked_reason.as_deref(),
            Some("sensitive value reveal is disabled by policy")
        );
        assert!(body.value.is_none());
    }

    #[test]
    fn parse_node_get_value_extracts_json_value_and_version() {
        let (value, version) = parse_node_get_value(r#"{"value":"stored-token","version":42}"#);

        assert_eq!(value.as_deref(), Some("stored-token"));
        assert_eq!(version, Some(42));

        let (value, version) = parse_node_get_value("legacy-raw-value");
        assert!(value.is_none());
        assert!(version.is_none());
    }

    #[tokio::test]
    async fn set_key_proxies_value_ttl_and_success_status() {
        let (http_addr, handle) = one_http_response("204 No Content", "").await;
        let state = proxy_state_for_http_addr(http_addr);

        let response = set_key(
            State(state),
            Path((
                format!("127.0.0.1:{}", http_addr.port() + 1),
                "alpha".into(),
            )),
            Query(NamespaceQuery::default()),
            Json(SetKeyBody {
                value: "stored-value".into(),
                ttl_secs: Some(30),
            }),
        )
        .await
        .into_response();

        let request = handle.await.unwrap();
        assert!(request.starts_with("PUT /key/alpha?ttl=30 HTTP/1.1"));
        assert!(request.ends_with("stored-value"));
        let body = json_body(response).await;
        assert_eq!(body["ok"], true);
    }

    #[tokio::test]
    async fn delete_key_preserves_node_http_error_status_and_body() {
        let (http_addr, handle) = one_http_response("404 Not Found", "missing").await;
        let state = proxy_state_for_http_addr(http_addr);

        let response = delete_key(
            State(state),
            Path((
                format!("127.0.0.1:{}", http_addr.port() + 1),
                "alpha".into(),
            )),
            Query(NamespaceQuery::default()),
        )
        .await
        .into_response();

        let request = handle.await.unwrap();
        assert!(request.starts_with("DELETE /key/alpha HTTP/1.1"));
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = json_body(response).await;
        assert_eq!(body["error"], "missing");
    }

    #[tokio::test]
    async fn single_key_handlers_reject_invalid_keys_before_proxying() {
        let state = state_for_seed("127.0.0.1:7779".into(), 7779);

        let get_response = get_key(
            State(Arc::clone(&state)),
            Path(("127.0.0.1:7779".into(), "../secret".into())),
            Query(GetKeyQuery::default()),
        )
        .await
        .into_response();
        assert_eq!(get_response.status(), StatusCode::BAD_REQUEST);

        let set_response = set_key(
            State(Arc::clone(&state)),
            Path(("127.0.0.1:7779".into(), "bad/key".into())),
            Query(NamespaceQuery::default()),
            Json(SetKeyBody {
                value: "value".into(),
                ttl_secs: None,
            }),
        )
        .await
        .into_response();
        assert_eq!(set_response.status(), StatusCode::BAD_REQUEST);

        let delete_response = delete_key(
            State(state),
            Path(("127.0.0.1:7779".into(), "".into())),
            Query(NamespaceQuery::default()),
        )
        .await
        .into_response();
        assert_eq!(delete_response.status(), StatusCode::BAD_REQUEST);
    }
}
