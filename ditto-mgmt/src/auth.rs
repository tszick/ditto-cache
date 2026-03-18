//! HTTP Basic Auth middleware for the ditto-mgmt server (port 7781).
//!
//! When `[admin] password_hash` is set in the config, every incoming request
//! must carry a valid `Authorization: Basic <base64(user:pass)>` header.
//! Requests without or with wrong credentials receive `401 Unauthorized` with a
//! `WWW-Authenticate: Basic realm="ditto-mgmt"` header, which triggers the
//! browser's native login dialog.
//!
//! When `password_hash` is absent the middleware is a no-op (auth disabled).

use crate::api::SharedState;
use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};

/// Axum middleware: enforce HTTP Basic Auth when `[admin]` is configured.
pub async fn basic_auth_middleware(
    State(state): State<SharedState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let (expected_user, expected_hash) = match &state.cfg.admin.password_hash {
        None => return next.run(req).await,   // auth not configured → pass through
        Some(hash) => {
            let user = state.cfg.admin.username
                .as_deref()
                .unwrap_or("admin")
                .to_string();
            (user, hash.clone())
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
            [(header::WWW_AUTHENTICATE, r#"Basic realm="ditto-mgmt""#)],
            Body::empty(),
        )
            .into_response()
    }
}
