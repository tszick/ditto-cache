use axum::http::{HeaderMap, StatusCode};
use base64::Engine;
use bytes::Bytes;
use ditto_protocol::{ErrorCode, NodeStatus};

const NAMESPACE_HEADER: &str = "x-ditto-namespace";

pub(crate) fn namespace_from_headers(headers: &HeaderMap) -> Option<String> {
    headers
        .get(NAMESPACE_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string)
}

pub(crate) fn tcp_client_bind_loopback_only(bind_addr: &str) -> bool {
    ditto_config::is_loopback_bind_addr(bind_addr)
}

pub(crate) fn tcp_production_safe(bind_addr: &str, client_auth_enabled: bool) -> bool {
    client_auth_enabled || tcp_client_bind_loopback_only(bind_addr)
}

pub(crate) fn tcp_supported_topology(
    bind_addr: &str,
    client_auth_enabled: bool,
) -> &'static str {
    if client_auth_enabled {
        "token-auth-exposed"
    } else if tcp_client_bind_loopback_only(bind_addr) {
        "loopback-only"
    } else {
        "unsupported-for-production"
    }
}

pub(crate) fn availability_for_stats(
    status: &NodeStatus,
    circuit_breaker_state: &str,
) -> &'static str {
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

pub(crate) fn query_flag_enabled(value: Option<&str>) -> bool {
    matches!(value, Some("1")) || value.is_some_and(|flag| flag.eq_ignore_ascii_case("true"))
}

pub(crate) fn status_for_error(code: &ErrorCode) -> StatusCode {
    match code {
        ErrorCode::NodeInactive => StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::NoQuorum => StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::CircuitOpen => StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::KeyNotFound => StatusCode::NOT_FOUND,
        ErrorCode::WriteTimeout => StatusCode::GATEWAY_TIMEOUT,
        ErrorCode::RateLimited => StatusCode::TOO_MANY_REQUESTS,
        ErrorCode::NamespaceQuotaExceeded => StatusCode::TOO_MANY_REQUESTS,
        ErrorCode::AuthFailed => StatusCode::UNAUTHORIZED,
        ErrorCode::AccessDenied => StatusCode::FORBIDDEN,
        ErrorCode::UnsupportedRequest => StatusCode::NOT_IMPLEMENTED,
        ErrorCode::TypeMismatch | ErrorCode::Overflow => StatusCode::CONFLICT,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

pub(crate) fn batch_item_value(
    value: Option<&str>,
    value_base64: Option<&str>,
) -> Result<Bytes, String> {
    if let Some(value_base64) = value_base64 {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(value_base64.trim())
            .map_err(|e| format!("value_base64 is not valid base64: {e}"))?;
        return Ok(Bytes::from(bytes));
    }

    value
        .map(|value| Bytes::copy_from_slice(value.as_bytes()))
        .ok_or_else(|| "either value or value_base64 must be provided".to_string())
}
