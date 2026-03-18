use crate::node::NodeHandle;
use axum::{
    Router,
    body::Body,
    extract::{Path, Query, State},
    http::{Request, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Json,
};
use bytes::Bytes;
use ditto_protocol::{ClientRequest, ClientResponse, ErrorCode};
use serde::Deserialize;
use std::sync::Arc;
use tracing::info;

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
        .route("/key/:key", get(handle_get)
            .put(handle_set)
            .delete(handle_delete))
        .route("/ping",  get(handle_ping))
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
        (cfg.http_auth.password_hash.clone(), cfg.http_auth.username.clone())
    }; // MutexGuard dropped here — future is Send from this point on

    let (expected_user, expected_hash) = match auth {
        (None, _) => return next.run(req).await,   // auth disabled
        (Some(hash), username) => {
            let user = username.unwrap_or_else(|| "ditto".to_string());
            (user, hash)
        }
    };

    let credential = req.headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Basic "))
        .and_then(|b64| {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD.decode(b64).ok()
        })
        .and_then(|bytes| String::from_utf8(bytes).ok());

    let (is_valid_user, pass_to_check) = match credential.as_deref().and_then(|s| s.split_once(':')) {
        Some((user, pass)) if user == expected_user => (true, pass.to_string()),
        _ => (false, "dummy-password-for-timing-attack-mitigation".to_string()),
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
    Path(key): Path<String>,
    State(node): State<Arc<NodeHandle>>,
) -> Response {
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
        ).into_response(),
        ClientResponse::Error { code, message } => error_response(code, message),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[derive(Deserialize)]
struct SetQuery {
    ttl: Option<u64>,
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

async fn handle_delete(
    Path(key): Path<String>,
    State(node): State<Arc<NodeHandle>>,
) -> Response {
    match node.handle_client(ClientRequest::Delete { key }).await {
        ClientResponse::Deleted           => StatusCode::NO_CONTENT.into_response(),
        ClientResponse::NotFound          => StatusCode::NOT_FOUND.into_response(),
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
    let status = match code {
        ErrorCode::NodeInactive => StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::NoQuorum     => StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::KeyNotFound  => StatusCode::NOT_FOUND,
        ErrorCode::WriteTimeout => StatusCode::GATEWAY_TIMEOUT,
        _                       => StatusCode::INTERNAL_SERVER_ERROR,
    };
    let body = serde_json::json!({ "error": format!("{:?}", code), "message": message });
    (status, Json(body)).into_response()
}
