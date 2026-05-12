//! Structured audit logging for management API operations.
//!
//! Audit events intentionally avoid request bodies, cache values, passwords, and
//! bearer tokens. Bearer credentials are represented by a short SHA-256
//! fingerprint so operators can correlate activity without storing the secret.

use axum::{
    body::Body,
    http::{header, Method, Request},
    middleware::Next,
    response::Response,
};
use base64::Engine;
use sha2::{Digest, Sha256};
use tracing::info;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditActor {
    pub auth_scheme: &'static str,
    pub subject: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AuditOperation {
    action: &'static str,
    resource: &'static str,
}

pub async fn audit_middleware(req: Request<Body>, next: Next) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let query = req.uri().query().map(str::to_string);
    let operation = audit_operation(&method, &path, query.as_deref());
    let actor = audit_actor(&req);
    let user_agent = req
        .headers()
        .get(header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
        .to_string();
    let request_id = req
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("none")
        .to_string();

    let response = next.run(req).await;

    if let Some(operation) = operation {
        let status = response.status().as_u16();
        let outcome = if response.status().is_success() {
            "success"
        } else if response.status().is_client_error() {
            "rejected"
        } else {
            "failed"
        };

        info!(
            target: "ditto.audit",
            audit = true,
            event = "management_api",
            method = %method,
            path = %path,
            action = operation.action,
            resource = operation.resource,
            status = status,
            outcome = outcome,
            auth_scheme = actor.auth_scheme,
            actor = %actor.subject,
            request_id = %request_id,
            user_agent = %user_agent
        );
    }

    response
}

fn audit_operation(method: &Method, path: &str, query: Option<&str>) -> Option<AuditOperation> {
    if !path.starts_with("/api/") {
        return None;
    }

    if *method == Method::GET
        && path.starts_with("/api/cache/")
        && path.contains("/keys/")
        && query
            .unwrap_or("")
            .split('&')
            .any(|part| part == "reveal=true")
    {
        return Some(AuditOperation {
            action: "reveal_cache_value",
            resource: "cache_key",
        });
    }

    match *method {
        Method::POST | Method::PUT | Method::DELETE => Some(classify_mutating_operation(path)),
        _ => None,
    }
}

fn classify_mutating_operation(path: &str) -> AuditOperation {
    if path.contains("/flush") {
        AuditOperation {
            action: "flush",
            resource: "cache",
        }
    } else if path.contains("/ttl") {
        AuditOperation {
            action: "set_ttl",
            resource: "cache_keys",
        }
    } else if path.contains("/compressed") {
        AuditOperation {
            action: "set_compressed",
            resource: "cache_key",
        }
    } else if path.contains("/keys/") {
        AuditOperation {
            action: "mutate_key",
            resource: "cache_key",
        }
    } else if path.contains("/set-active") {
        AuditOperation {
            action: "set_active",
            resource: "node",
        }
    } else if path.contains("/backup") {
        AuditOperation {
            action: "backup",
            resource: "node",
        }
    } else if path.contains("/restore-snapshot") {
        AuditOperation {
            action: "restore_snapshot",
            resource: "node",
        }
    } else if path.contains("/property/") {
        AuditOperation {
            action: "set_property",
            resource: "node",
        }
    } else {
        AuditOperation {
            action: "mutate",
            resource: "management_api",
        }
    }
}

fn audit_actor(req: &Request<Body>) -> AuditActor {
    let Some(authorization) = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    else {
        return AuditActor {
            auth_scheme: "none",
            subject: "anonymous".into(),
        };
    };

    if let Some(token) = authorization.strip_prefix("Bearer ") {
        return AuditActor {
            auth_scheme: "bearer",
            subject: format!("bearer:{}", sha256_prefix(token)),
        };
    }

    if let Some(b64) = authorization.strip_prefix("Basic ") {
        let subject = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .ok()
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .and_then(|credential| credential.split_once(':').map(|(user, _)| user.to_string()))
            .filter(|user| !user.trim().is_empty())
            .unwrap_or_else(|| "invalid-basic".into());

        return AuditActor {
            auth_scheme: "basic",
            subject,
        };
    }

    AuditActor {
        auth_scheme: "unknown",
        subject: "unknown".into(),
    }
}

fn sha256_prefix(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    digest
        .iter()
        .take(6)
        .map(|b| format!("{:02x}", b))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(method: &str, uri: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::empty())
            .unwrap()
    }

    #[test]
    fn audit_operation_captures_mutations_and_sensitive_reveals() {
        let flush = audit_operation(&Method::POST, "/api/cache/all/flush", None).unwrap();
        assert_eq!(flush.action, "flush");
        assert_eq!(flush.resource, "cache");

        let reveal = audit_operation(
            &Method::GET,
            "/api/cache/local/keys/session_token",
            Some("reveal=true"),
        )
        .unwrap();
        assert_eq!(reveal.action, "reveal_cache_value");

        assert!(
            audit_operation(&Method::GET, "/api/cache/local/keys/session_token", None).is_none()
        );
        assert!(audit_operation(&Method::GET, "/", None).is_none());
    }

    #[test]
    fn audit_actor_never_exposes_bearer_or_basic_password() {
        let bearer = Request::builder()
            .uri("/api/nodes/local/set-active")
            .header(header::AUTHORIZATION, "Bearer very-secret-token")
            .body(Body::empty())
            .unwrap();
        let actor = audit_actor(&bearer);
        assert_eq!(actor.auth_scheme, "bearer");
        assert!(actor.subject.starts_with("bearer:"));
        assert!(!actor.subject.contains("very-secret-token"));

        let basic = Request::builder()
            .uri("/api/nodes/local/set-active")
            .header(header::AUTHORIZATION, "Basic YWRtaW46c2VjcmV0")
            .body(Body::empty())
            .unwrap();
        let actor = audit_actor(&basic);
        assert_eq!(actor.auth_scheme, "basic");
        assert_eq!(actor.subject, "admin");
        assert!(!actor.subject.contains("secret"));
    }

    #[test]
    fn audit_actor_handles_missing_or_invalid_auth() {
        assert_eq!(
            audit_actor(&request("POST", "/api/cache/all/flush")).subject,
            "anonymous"
        );

        let invalid = Request::builder()
            .uri("/api/cache/all/flush")
            .header(header::AUTHORIZATION, "Basic !!!")
            .body(Body::empty())
            .unwrap();
        assert_eq!(audit_actor(&invalid).subject, "invalid-basic");
    }
}
