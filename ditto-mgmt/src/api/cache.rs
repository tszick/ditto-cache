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
use crate::app::cache as app_cache;
use crate::policy::cache_value_redaction::{
    build_cache_value_response, parse_node_get_value,
};
#[cfg(test)]
use ditto_protocol::{AdminRequest, AdminResponse};
use axum::{
    extract::{Path, Query, State},
    http::HeaderValue,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Deserialize;

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

// ---------------------------------------------------------------------------
// GET /api/cache/:target/stats
// ---------------------------------------------------------------------------

/// `GET /api/cache/:target/stats` — Cache statistics for one or all nodes.
pub async fn cache_stats(
    State(state): State<SharedState>,
    Path(target): Path<String>,
) -> impl IntoResponse {
    Json(app_cache::collect_cache_stats(state, &target).await).into_response()
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
        return Json(app_cache::flush_namespace(state, &target, &namespace).await).into_response();
    }
    Json(app_cache::flush_cache(state, &target).await).into_response()
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
    Json(app_cache::collect_list_keys(state, &target, pattern, &namespace).await)
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
    let authority = match app_cache::resolve_cache_http_authority(&state, &target) {
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
    let (status, payload) =
        app_cache::set_key(state, &target, &key, &body.value, body.ttl_secs, namespace).await;
    (
        StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
        Json(payload),
    )
        .into_response()
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
    let (status, payload) = app_cache::delete_key(state, &target, &key, namespace).await;
    (
        StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
        Json(payload),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// POST /api/cache/:target/keys/:key/compressed   body: { "compressed": bool }
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct SetCompressedBody {
    pub compressed: bool,
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
    let value = if body.compressed { "true" } else { "false" }.to_string();
    Json(app_cache::set_compressed(state, &target, &key, &value).await).into_response()
}

// ---------------------------------------------------------------------------
// POST /api/cache/:target/ttl   body: { "pattern": "glob", "ttl_secs": N|null }
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct SetTtlBody {
    pub pattern: String,
    pub ttl_secs: Option<u64>,
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
    Json(app_cache::set_keys_ttl(state, &target, &pattern, body.ttl_secs).await)
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
            app_cache::strip_namespace_prefix(&Some("tenant".into()), "tenant::alpha".into()),
            "alpha"
        );
        assert_eq!(
            app_cache::strip_namespace_prefix(&Some("tenant".into()), "other::alpha".into()),
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
        let jwt_like_value = test_jwt_like_value();
        let body = build_cache_value_response(
            "access_token",
            Some("tenant"),
            &jwt_like_value,
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
        let jwt_like_value = test_jwt_like_value();

        let body = build_cache_value_response(
            "session_token",
            None,
            &jwt_like_value,
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

    fn test_jwt_like_value() -> String {
        [
            "eyJhbGciOiJIUzI1NiJ9",
            "eyJzdWIiOiIxMjM0NTY3ODkwIn0",
            "signature123",
        ]
        .join(".")
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
