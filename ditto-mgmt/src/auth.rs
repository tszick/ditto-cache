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
    http::{header, Method, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use reqwest::Url;

/// Axum middleware: enforce HTTP Basic Auth when `[admin]` is configured.
pub async fn basic_auth_middleware(
    State(state): State<SharedState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    if !same_origin_for_unsafe_request(&req) {
        return (
            StatusCode::FORBIDDEN,
            Body::from("cross-origin state-changing request rejected"),
        )
            .into_response();
    }

    let (expected_user, expected_hash) = match &state.cfg.admin.password_hash {
        None => return next.run(req).await, // auth not configured → pass through
        Some(hash) => {
            let user = state
                .cfg
                .admin
                .username
                .as_deref()
                .unwrap_or("admin")
                .to_string();
            (user, hash.clone())
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
            [(header::WWW_AUTHENTICATE, r#"Basic realm="ditto-mgmt""#)],
            Body::empty(),
        )
            .into_response()
    }
}

fn same_origin_for_unsafe_request(req: &Request<Body>) -> bool {
    if matches!(
        *req.method(),
        Method::GET | Method::HEAD | Method::OPTIONS | Method::TRACE
    ) {
        return true;
    }

    let Some(host) = req
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(normalize_authority)
    else {
        return false;
    };

    let origin_header = req
        .headers()
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok());
    if let Some(origin) = origin_header {
        return origin_authority(origin).is_some_and(|origin| origin == host);
    }

    let referer_header = req
        .headers()
        .get(header::REFERER)
        .and_then(|v| v.to_str().ok());
    match referer_header {
        Some(referer) => origin_authority(referer).is_some_and(|referer| referer == host),
        None => true,
    }
}

fn origin_authority(value: &str) -> Option<String> {
    Url::parse(value)
        .ok()
        .and_then(|url| url.host_str().map(|host| (host.to_string(), url.port())))
        .map(|(host, port)| match port {
            Some(port) => normalize_authority(&format!("{host}:{port}")),
            None => normalize_authority(&host),
        })
}

fn normalize_authority(value: &str) -> String {
    let value = value.trim();
    if let Some((host, port)) = value.rsplit_once(':') {
        if port.chars().all(|c| c.is_ascii_digit()) && !host.contains(':') {
            return format!("{}:{}", host.trim_end_matches('.'), port).to_ascii_lowercase();
        }
    }
    value.trim_end_matches('.').to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::{normalize_authority, origin_authority, same_origin_for_unsafe_request};
    use axum::{body::Body, http::Request};

    #[test]
    fn safe_methods_skip_same_origin_checks() {
        for method in ["GET", "HEAD", "OPTIONS", "TRACE"] {
            let req = Request::builder()
                .method(method)
                .uri("/api/nodes")
                .body(Body::empty())
                .unwrap();
            assert!(
                same_origin_for_unsafe_request(&req),
                "{method} should be accepted without origin metadata"
            );
        }
    }

    #[test]
    fn unsafe_request_rejects_missing_host_header() {
        let req = Request::builder()
            .method("DELETE")
            .uri("/api/cache/local/keys/alpha")
            .body(Body::empty())
            .unwrap();

        assert!(!same_origin_for_unsafe_request(&req));
    }

    #[test]
    fn unsafe_request_rejects_cross_origin_browser_origin() {
        let req = Request::builder()
            .method("POST")
            .uri("/api/nodes/local/set-active")
            .header("host", "localhost:7781")
            .header("origin", "https://evil.example")
            .body(Body::empty())
            .unwrap();

        assert!(!same_origin_for_unsafe_request(&req));
    }

    #[test]
    fn unsafe_request_allows_same_origin_and_cli_without_origin() {
        let same_origin = Request::builder()
            .method("POST")
            .uri("/api/nodes/local/set-active")
            .header("host", "localhost:7781")
            .header("origin", "https://localhost:7781")
            .body(Body::empty())
            .unwrap();
        assert!(same_origin_for_unsafe_request(&same_origin));

        let cli = Request::builder()
            .method("POST")
            .uri("/api/nodes/local/set-active")
            .header("host", "localhost:7781")
            .body(Body::empty())
            .unwrap();
        assert!(same_origin_for_unsafe_request(&cli));
    }

    #[test]
    fn unsafe_request_rejects_malformed_origin_or_referer() {
        let malformed_origin = Request::builder()
            .method("POST")
            .uri("/api/nodes/local/set-active")
            .header("host", "localhost:7781")
            .header("origin", "not a url")
            .body(Body::empty())
            .unwrap();
        assert!(!same_origin_for_unsafe_request(&malformed_origin));

        let malformed_referer = Request::builder()
            .method("POST")
            .uri("/api/nodes/local/set-active")
            .header("host", "localhost:7781")
            .header("referer", "not a url")
            .body(Body::empty())
            .unwrap();
        assert!(!same_origin_for_unsafe_request(&malformed_referer));
    }

    #[test]
    fn origin_authority_normalizes_case_ports_and_trailing_dots() {
        assert_eq!(
            origin_authority("https://LOCALHOST.:7781/admin").as_deref(),
            Some("localhost:7781")
        );
        assert_eq!(
            origin_authority("http://Example.COM/path").as_deref(),
            Some("example.com")
        );
        assert_eq!(origin_authority("not a url"), None);
        assert_eq!(normalize_authority(" EXAMPLE.com. "), "example.com");
    }
}
