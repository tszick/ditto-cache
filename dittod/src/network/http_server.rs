use crate::node::NodeHandle;
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use bytes::Bytes;
use ditto_protocol::{ClientRequest, ClientResponse, ErrorCode};
use serde::Deserialize;
use std::sync::Arc;
use tracing::info;
const MAX_BATCH_SET_ITEMS: usize = 5_000;

const NAMESPACE_HEADER: &str = "x-ditto-namespace";

fn namespace_from_headers(headers: &HeaderMap) -> Option<String> {
    headers
        .get(NAMESPACE_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string)
}

/// Start the HTTP REST server on `bind`.
///
/// `tls` — if `Some((cert_path, key_path))`, the server listens over HTTPS
///         using server-only TLS (no client cert required).
///         Uses the same node cert/key as the cluster port.
pub async fn start(
    bind: String,
    node: Arc<NodeHandle>,
    tls: Option<(String, String)>,
) -> anyhow::Result<()> {
    let app = build_app(node);

    if let Some((cert, key)) = tls {
        use axum_server::tls_rustls::RustlsConfig;
        let config = RustlsConfig::from_pem_file(&cert, &key).await?;
        let addr: std::net::SocketAddr = bind.parse()?;
        info!("HTTP REST (HTTPS) listening on {}", bind);
        axum_server::bind_rustls(addr, config)
            .serve(app.into_make_service())
            .await?;
    } else {
        let listener = tokio::net::TcpListener::bind(&bind).await?;
        info!("HTTP REST listening on {}", bind);
        axum::serve(listener, app).await?;
    }
    Ok(())
}

fn build_app(node: Arc<NodeHandle>) -> Router {
    Router::new()
        .route(
            "/key/{key}",
            get(handle_get).put(handle_set).delete(handle_delete),
        )
        .route("/keys/delete-by-pattern", post(handle_delete_by_pattern))
        .route("/keys/batch", post(handle_batch_set))
        .route("/keys/ttl-by-pattern", post(handle_set_ttl_by_pattern))
        .route("/ping", get(handle_ping))
        .route("/stats", get(handle_stats))
        .route("/health/summary", get(handle_health_summary))
        .layer(middleware::from_fn_with_state(
            Arc::clone(&node),
            http_basic_auth,
        ))
        .with_state(node)
}

// ---------------------------------------------------------------------------
// Basic Auth middleware
// ---------------------------------------------------------------------------

/// HTTP Basic Auth middleware for the REST port.
///
/// Verifies `Authorization: Basic <base64(user:pass)>` against the bcrypt hash
/// stored in `[http_auth]` config.  When `password_hash` is absent, every
/// request is allowed through (auth disabled).
///
/// `/ping` is always exempt so health-check tooling works unauthenticated.
async fn http_basic_auth(
    State(node): State<Arc<NodeHandle>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    // /ping is always exempt.
    if req.uri().path() == "/ping" {
        return next.run(req).await;
    }

    // Extract auth config from the Mutex BEFORE any .await so the MutexGuard
    // (which is !Send) is dropped before the future suspends.
    let auth = {
        let cfg = node.config.lock().unwrap();
        (
            cfg.http_auth.password_hash.clone(),
            cfg.http_auth.username.clone(),
        )
    }; // MutexGuard dropped here — future is Send from this point on

    let (expected_user, expected_hash) = match auth {
        (None, _) => return next.run(req).await, // auth disabled
        (Some(hash), username) => {
            let user = username.unwrap_or_else(|| "ditto".to_string());
            (user, hash)
        }
    };

    let credential = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Basic "))
        .and_then(|b64| {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD.decode(b64).ok()
        })
        .and_then(|bytes| String::from_utf8(bytes).ok());

    let (is_valid_user, pass_to_check) = match credential.as_deref().and_then(|s| s.split_once(':'))
    {
        Some((user, pass)) if user == expected_user => (true, pass.to_string()),
        _ => (
            false,
            "dummy-password-for-timing-attack-mitigation".to_string(),
        ),
    };

    let is_valid_pass = tokio::task::spawn_blocking(move || {
        bcrypt::verify(&pass_to_check, &expected_hash).unwrap_or(false)
    })
    .await
    .unwrap_or(false);

    let authorized = is_valid_user && is_valid_pass;

    if authorized {
        next.run(req).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, r#"Basic realm="ditto""#)],
            Body::empty(),
        )
            .into_response()
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn handle_get(
    headers: HeaderMap,
    Path(key): Path<String>,
    State(node): State<Arc<NodeHandle>>,
) -> Response {
    match node
        .handle_client_http(ClientRequest::Get {
            key,
            namespace: namespace_from_headers(&headers),
        })
        .await
    {
        ClientResponse::Value { value, version, .. } => {
            let body = serde_json::json!({
                "value":   String::from_utf8_lossy(&value),
                "version": version,
            });
            Json(body).into_response()
        }
        ClientResponse::NotFound => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "not_found" })),
        )
            .into_response(),
        ClientResponse::Error { code, message } => error_response(code, message),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[derive(Deserialize)]
struct SetQuery {
    ttl: Option<u64>,
}

#[derive(Deserialize)]
struct BatchSetItem {
    key: String,
    value: String,
    ttl_secs: Option<u64>,
}

#[derive(Deserialize)]
struct BatchSetBody {
    items: Vec<BatchSetItem>,
}

async fn handle_set(
    headers: HeaderMap,
    Path(key): Path<String>,
    Query(q): Query<SetQuery>,
    State(node): State<Arc<NodeHandle>>,
    body: String,
) -> Response {
    let req = ClientRequest::Set {
        key,
        value: Bytes::from(body.into_bytes()),
        ttl_secs: q.ttl,
        namespace: namespace_from_headers(&headers),
    };
    match node.handle_client_http(req).await {
        ClientResponse::Ok { version } => {
            Json(serde_json::json!({ "version": version })).into_response()
        }
        ClientResponse::Error { code, message } => error_response(code, message),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn handle_batch_set(
    headers: HeaderMap,
    State(node): State<Arc<NodeHandle>>,
    Json(body): Json<BatchSetBody>,
) -> Response {
    if body.items.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "items_must_not_be_empty" })),
        )
            .into_response();
    }
    if body.items.len() > MAX_BATCH_SET_ITEMS {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "too_many_items",
                "max_items": MAX_BATCH_SET_ITEMS
            })),
        )
            .into_response();
    }

    let mut succeeded = 0usize;
    let mut errors = Vec::new();

    for (idx, item) in body.items.into_iter().enumerate() {
        let req = ClientRequest::Set {
            key: item.key.clone(),
            value: Bytes::from(item.value.into_bytes()),
            ttl_secs: item.ttl_secs,
            namespace: namespace_from_headers(&headers),
        };
        match node.handle_client_http(req).await {
            ClientResponse::Ok { .. } => succeeded += 1,
            ClientResponse::Error { code, message } => {
                errors.push(serde_json::json!({
                    "index": idx,
                    "key": item.key,
                    "error": format!("{:?}", code),
                    "message": message
                }));
            }
            _ => {
                errors.push(serde_json::json!({
                    "index": idx,
                    "key": item.key,
                    "error": "InternalError",
                    "message": "unexpected response"
                }));
            }
        }
    }

    let failed = errors.len();
    Json(serde_json::json!({
        "received": succeeded + failed,
        "succeeded": succeeded,
        "failed": failed,
        "errors": errors
    }))
    .into_response()
}

async fn handle_delete(
    headers: HeaderMap,
    Path(key): Path<String>,
    State(node): State<Arc<NodeHandle>>,
) -> Response {
    match node
        .handle_client_http(ClientRequest::Delete {
            key,
            namespace: namespace_from_headers(&headers),
        })
        .await
    {
        ClientResponse::Deleted => StatusCode::NO_CONTENT.into_response(),
        ClientResponse::NotFound => StatusCode::NOT_FOUND.into_response(),
        ClientResponse::Error { code, message } => error_response(code, message),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[derive(Deserialize)]
struct DeleteByPatternBody {
    pattern: String,
}

async fn handle_delete_by_pattern(
    headers: HeaderMap,
    State(node): State<Arc<NodeHandle>>,
    Json(body): Json<DeleteByPatternBody>,
) -> Response {
    match node
        .handle_client_http(ClientRequest::DeleteByPattern {
            pattern: body.pattern,
            namespace: namespace_from_headers(&headers),
        })
        .await
    {
        ClientResponse::PatternDeleted { deleted } => {
            Json(serde_json::json!({ "deleted": deleted })).into_response()
        }
        ClientResponse::Error { code, message } => error_response(code, message),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[derive(Deserialize)]
struct SetTtlByPatternBody {
    pattern: String,
    ttl_secs: Option<u64>,
}

async fn handle_set_ttl_by_pattern(
    headers: HeaderMap,
    State(node): State<Arc<NodeHandle>>,
    Json(body): Json<SetTtlByPatternBody>,
) -> Response {
    match node
        .handle_client_http(ClientRequest::SetTtlByPattern {
            pattern: body.pattern,
            ttl_secs: body.ttl_secs,
            namespace: namespace_from_headers(&headers),
        })
        .await
    {
        ClientResponse::PatternTtlUpdated { updated } => {
            Json(serde_json::json!({ "updated": updated })).into_response()
        }
        ClientResponse::Error { code, message } => error_response(code, message),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn handle_ping(State(node): State<Arc<NodeHandle>>) -> Response {
    node.handle_client_http(ClientRequest::Ping).await;
    Json(serde_json::json!({ "pong": true })).into_response()
}

async fn handle_stats(State(node): State<Arc<NodeHandle>>) -> Response {
    let stats = node.stats().await;
    Json(stats).into_response()
}

async fn handle_health_summary(State(node): State<Arc<NodeHandle>>) -> Response {
    let stats = node.stats().await;
    let availability = availability_for_stats(&stats.status, &stats.circuit_breaker_state);

    let body = serde_json::json!({
        "availability": availability,
        "node_id": stats.node_id,
        "status": format!("{:?}", stats.status),
        "is_primary": stats.is_primary,
        "committed_index": stats.committed_index,
        "key_count": stats.key_count,
        "memory_used_bytes": stats.memory_used_bytes,
        "memory_max_bytes": stats.memory_max_bytes,
        "uptime_secs": stats.uptime_secs,
        "rate_limit_enabled": stats.rate_limit_enabled,
        "rate_limited_requests_total": stats.rate_limited_requests_total,
        "circuit_breaker_enabled": stats.circuit_breaker_enabled,
        "circuit_breaker_state": stats.circuit_breaker_state,
        "circuit_breaker_open_total": stats.circuit_breaker_open_total,
        "circuit_breaker_reject_total": stats.circuit_breaker_reject_total,
        "hot_key_inflight_keys": stats.hot_key_inflight_keys,
        "client_requests_total": stats.client_requests_total,
        "client_requests_tcp_total": stats.client_requests_tcp_total,
        "client_requests_http_total": stats.client_requests_http_total,
        "client_requests_internal_total": stats.client_requests_internal_total,
        "client_request_latency_le_1ms_total": stats.client_request_latency_le_1ms_total,
        "client_request_latency_le_5ms_total": stats.client_request_latency_le_5ms_total,
        "client_request_latency_le_20ms_total": stats.client_request_latency_le_20ms_total,
        "client_request_latency_le_100ms_total": stats.client_request_latency_le_100ms_total,
        "client_request_latency_le_500ms_total": stats.client_request_latency_le_500ms_total,
        "client_request_latency_gt_500ms_total": stats.client_request_latency_gt_500ms_total,
        "client_latency_p50_estimate_ms": stats.client_latency_p50_estimate_ms,
        "client_latency_p90_estimate_ms": stats.client_latency_p90_estimate_ms,
        "client_latency_p95_estimate_ms": stats.client_latency_p95_estimate_ms,
        "client_latency_p99_estimate_ms": stats.client_latency_p99_estimate_ms,
        "client_error_total": stats.client_error_total,
        "client_errors_tcp_total": stats.client_errors_tcp_total,
        "client_errors_http_total": stats.client_errors_http_total,
        "client_errors_internal_total": stats.client_errors_internal_total,
        "client_error_auth_total": stats.client_error_auth_total,
        "client_error_throttle_total": stats.client_error_throttle_total,
        "client_error_availability_total": stats.client_error_availability_total,
        "client_error_validation_total": stats.client_error_validation_total,
        "client_error_internal_total": stats.client_error_internal_total,
        "client_error_other_total": stats.client_error_other_total,
        "read_repair_enabled": stats.read_repair_enabled,
        "read_repair_trigger_total": stats.read_repair_trigger_total,
        "read_repair_throttled_total": stats.read_repair_throttled_total,
        "anti_entropy_last_detected_lag": stats.anti_entropy_last_detected_lag,
        "anti_entropy_repair_trigger_total": stats.anti_entropy_repair_trigger_total,
        "mixed_version_last_detected_peer_count": stats.mixed_version_last_detected_peer_count,
        "namespace_quota_reject_total": stats.namespace_quota_reject_total,
        "namespace_quota_reject_rate_per_min": stats.namespace_quota_reject_rate_per_min,
        "namespace_quota_reject_trend": stats.namespace_quota_reject_trend,
        "namespace_quota_top_usage": stats.namespace_quota_top_usage,
        "persistence_enabled": stats.persistence_enabled,
        "tenancy_enabled": stats.tenancy_enabled,
        "snapshot_last_load_age_secs": stats.snapshot_last_load_age_secs,
        "snapshot_restore_attempt_total": stats.snapshot_restore_attempt_total,
        "snapshot_restore_success_total": stats.snapshot_restore_success_total,
        "snapshot_restore_failure_total": stats.snapshot_restore_failure_total,
        "snapshot_restore_not_found_total": stats.snapshot_restore_not_found_total,
        "snapshot_restore_policy_block_total": stats.snapshot_restore_policy_block_total,
    });

    Json(body).into_response()
}

fn availability_for_stats(
    status: &ditto_protocol::NodeStatus,
    circuit_breaker_state: &str,
) -> &'static str {
    use ditto_protocol::NodeStatus;
    match status {
        NodeStatus::Active => {
            if circuit_breaker_state.eq_ignore_ascii_case("open") {
                "degraded"
            } else {
                "ready"
            }
        }
        NodeStatus::Syncing => "syncing",
        NodeStatus::Offline | NodeStatus::Inactive => "unavailable",
    }
}

fn error_response(code: ErrorCode, message: String) -> Response {
    let status = status_for_error(&code);
    let body = serde_json::json!({ "error": format!("{:?}", code), "message": message });
    (status, Json(body)).into_response()
}

fn status_for_error(code: &ErrorCode) -> StatusCode {
    match code {
        ErrorCode::NodeInactive => StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::NoQuorum => StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::CircuitOpen => StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::KeyNotFound => StatusCode::NOT_FOUND,
        ErrorCode::WriteTimeout => StatusCode::GATEWAY_TIMEOUT,
        ErrorCode::RateLimited => StatusCode::TOO_MANY_REQUESTS,
        ErrorCode::NamespaceQuotaExceeded => StatusCode::TOO_MANY_REQUESTS,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use super::{availability_for_stats, status_for_error};
    use axum::http::StatusCode;
    use ditto_protocol::{ErrorCode, NodeStatus};

    #[test]
    fn status_mapping_for_rate_limit_and_circuit_open() {
        assert_eq!(
            status_for_error(&ErrorCode::RateLimited),
            StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(
            status_for_error(&ErrorCode::CircuitOpen),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn health_summary_availability_mapping() {
        assert_eq!(
            availability_for_stats(&NodeStatus::Active, "closed"),
            "ready"
        );
        assert_eq!(
            availability_for_stats(&NodeStatus::Active, "open"),
            "degraded"
        );
        assert_eq!(
            availability_for_stats(&NodeStatus::Syncing, "closed"),
            "syncing"
        );
        assert_eq!(
            availability_for_stats(&NodeStatus::Inactive, "closed"),
            "unavailable"
        );
    }
}
