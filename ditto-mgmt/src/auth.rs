//! HTTP admin auth middleware for the ditto-mgmt server (port 7781).
//!
//! Basic auth remains supported for local and CLI use. Bearer auth can validate
//! either a configured SHA-256 token digest or an OAuth2/OIDC introspection
//! endpoint response.

use crate::api::SharedState;
use axum::{
    body::Body,
    extract::State,
    http::{header, Method, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use reqwest::Url;
use serde::Deserialize;
use sha2::{Digest, Sha256};

#[derive(Debug, Deserialize)]
struct IntrospectionResponse {
    active: bool,
    #[serde(default)]
    scope: Option<serde_json::Value>,
    #[serde(default)]
    aud: Option<serde_json::Value>,
}

/// Axum middleware: enforce configured admin auth modes.
pub async fn admin_auth_middleware(
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

    if !state.cfg.admin.auth_configured() {
        return next.run(req).await;
    }

    let authorization = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    if let Some(token) = authorization
        .as_deref()
        .and_then(|v| v.strip_prefix("Bearer "))
    {
        if bearer_authorized(&state, token).await {
            return next.run(req).await;
        }
    }

    if let Some(hash) = &state.cfg.admin.password_hash {
        if basic_authorized(
            authorization.as_deref(),
            state.cfg.admin.username.as_deref(),
            hash,
        )
        .await
        {
            return next.run(req).await;
        }
    }

    let challenge = if state.cfg.admin.bearer_token_sha256.is_some()
        || state.cfg.admin.bearer_introspection_url.is_some()
    {
        r#"Bearer realm="ditto-mgmt""#
    } else {
        r#"Basic realm="ditto-mgmt""#
    };

    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, challenge)],
        Body::empty(),
    )
        .into_response()
}

async fn basic_authorized(
    authorization: Option<&str>,
    expected_user: Option<&str>,
    expected_hash: &str,
) -> bool {
    let expected_user = expected_user.unwrap_or("admin").to_string();
    let expected_hash = expected_hash.to_string();
    let credential = authorization
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

    is_valid_user && is_valid_pass
}

async fn bearer_authorized(state: &SharedState, token: &str) -> bool {
    if token.trim().is_empty() {
        return false;
    }

    if let Some(expected_hash) = &state.cfg.admin.bearer_token_sha256 {
        if bearer_token_matches_hash(token, expected_hash) {
            return true;
        }
    }

    let Some(url) = state.cfg.admin.bearer_introspection_url.as_deref() else {
        return false;
    };

    let mut req = state.http_client.post(url).form(&[("token", token)]);
    if let (Some(client_id), Some(client_secret)) = (
        state.cfg.admin.bearer_introspection_client_id.as_deref(),
        state
            .cfg
            .admin
            .bearer_introspection_client_secret
            .as_deref(),
    ) {
        req = req.basic_auth(client_id, Some(client_secret));
    }

    let Ok(resp) = req.send().await else {
        return false;
    };
    if !resp.status().is_success() {
        return false;
    }
    let Ok(body) = resp.json::<IntrospectionResponse>().await else {
        return false;
    };

    body.active
        && claim_contains(
            body.scope.as_ref(),
            state.cfg.admin.bearer_required_scope.as_deref(),
        )
        && claim_contains(
            body.aud.as_ref(),
            state.cfg.admin.bearer_required_audience.as_deref(),
        )
}

fn bearer_token_matches_hash(token: &str, expected_sha256_hex: &str) -> bool {
    let digest = Sha256::digest(token.as_bytes());
    let actual = digest
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>();
    actual.eq_ignore_ascii_case(expected_sha256_hex.trim())
}

fn claim_contains(value: Option<&serde_json::Value>, required: Option<&str>) -> bool {
    let Some(required) = required.filter(|v| !v.trim().is_empty()) else {
        return true;
    };
    let required = required.trim();
    match value {
        Some(serde_json::Value::String(v)) => v.split_whitespace().any(|part| part == required),
        Some(serde_json::Value::Array(values)) => values.iter().any(|v| {
            v.as_str().is_some_and(|part| {
                part == required || part.split_whitespace().any(|p| p == required)
            })
        }),
        _ => false,
    }
}

fn same_origin_for_unsafe_request(req: &Request<Body>) -> bool {
    if matches!(
        *req.method(),
        Method::GET | Method::HEAD | Method::OPTIONS | Method::TRACE
    ) {
        return true;
    }

    let Some(host) = request_authority(req) else {
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

fn request_authority(req: &Request<Body>) -> Option<String> {
    req.headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(normalize_authority)
        .or_else(|| {
            req.uri()
                .authority()
                .map(|v| normalize_authority(v.as_str()))
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
    use super::{
        bearer_token_matches_hash, claim_contains, normalize_authority, origin_authority,
        request_authority, same_origin_for_unsafe_request,
    };
    use axum::{body::Body, http::Request};
    use serde_json::json;

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
    fn unsafe_request_allows_http2_authority_without_host_header() {
        let req = Request::builder()
            .method("PUT")
            .uri("https://localhost:7781/api/cache/node-1/keys/access_token")
            .header("origin", "https://localhost:7781")
            .body(Body::empty())
            .unwrap();

        assert_eq!(request_authority(&req).as_deref(), Some("localhost:7781"));
        assert!(same_origin_for_unsafe_request(&req));
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

    #[test]
    fn bearer_hash_validation_uses_sha256_hex() {
        assert!(bearer_token_matches_hash(
            "test-token",
            "4c5dc9b7708905f77f5e5d16316b5dfb425e68cb326dcd55a860e90a7707031e"
        ));
        assert!(bearer_token_matches_hash(
            "test-token",
            "4C5DC9B7708905F77F5E5D16316B5DFB425E68CB326DCD55A860E90A7707031E"
        ));
        assert!(!bearer_token_matches_hash(
            "other",
            "4c5dc9b7708905f77f5e5d16316b5dfb425e68cb326dcd55a860e90a7707031e"
        ));
    }

    #[test]
    fn bearer_claim_matching_handles_scope_strings_and_audience_arrays() {
        assert!(claim_contains(
            Some(&json!("openid ditto.mgmt")),
            Some("ditto.mgmt")
        ));
        assert!(claim_contains(
            Some(&json!(["account", "ditto-mgmt"])),
            Some("ditto-mgmt")
        ));
        assert!(claim_contains(None, None));
        assert!(!claim_contains(
            Some(&json!("openid profile")),
            Some("ditto.mgmt")
        ));
    }
}
