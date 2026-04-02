use crate::node::NodeHandle;
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, Request, StatusCode},
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
            "/key/:key",
            get(handle_get).put(handle_set).delete(handle_delete),
        )
        .route("/keys/delete-by-pattern", post(handle_delete_by_pattern))
        .route("/keys/batch", post(handle_batch_set))
        .route("/keys/ttl-by-pattern", post(handle_set_ttl_by_pattern))
        .route("/ping", get(handle_ping))
        .route("/stats", get(handle_stats))
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

async fn handle_get(Path(key): Path<String>, State(node): State<Arc<NodeHandle>>) -> Response {
    match node.handle_client(ClientRequest::Get { key }).await {
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
    Path(key): Path<String>,
    Query(q): Query<SetQuery>,
    State(node): State<Arc<NodeHandle>>,
    body: String,
) -> Response {
    let req = ClientRequest::Set {
        key,
        value: Bytes::from(body.into_bytes()),
        ttl_secs: q.ttl,
    };
    match node.handle_client(req).await {
        ClientResponse::Ok { version } => {
            Json(serde_json::json!({ "version": version })).into_response()
        }
        ClientResponse::Error { code, message } => error_response(code, message),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn handle_batch_set(
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
        };
        match node.handle_client(req).await {
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

async fn handle_delete(Path(key): Path<String>, State(node): State<Arc<NodeHandle>>) -> Response {
    match node.handle_client(ClientRequest::Delete { key }).await {
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
    State(node): State<Arc<NodeHandle>>,
    Json(body): Json<DeleteByPatternBody>,
) -> Response {
    match node
        .handle_client(ClientRequest::DeleteByPattern {
            pattern: body.pattern,
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
    State(node): State<Arc<NodeHandle>>,
    Json(body): Json<SetTtlByPatternBody>,
) -> Response {
    match node
        .handle_client(ClientRequest::SetTtlByPattern {
            pattern: body.pattern,
            ttl_secs: body.ttl_secs,
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
    node.handle_client(ClientRequest::Ping).await;
    Json(serde_json::json!({ "pong": true })).into_response()
}

async fn handle_stats(State(node): State<Arc<NodeHandle>>) -> Response {
    let stats = node.stats().await;
    Json(stats).into_response()
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
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use super::status_for_error;
    use axum::http::StatusCode;
    use ditto_protocol::ErrorCode;

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
}
