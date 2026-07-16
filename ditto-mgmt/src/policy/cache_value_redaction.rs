use crate::config::CacheValuePolicy;
use serde::Serialize;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize)]
pub struct CacheValueResponse {
    pub key: String,
    pub namespace: Option<String>,
    pub length_bytes: usize,
    pub sha256: String,
    pub preview: String,
    pub masked: bool,
    pub reveal_allowed: bool,
    pub reveal_blocked_reason: Option<String>,
    pub upstream_version: Option<u64>,
    pub sensitive: bool,
    pub sensitive_reasons: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

pub fn build_cache_value_response(
    key: &str,
    namespace: Option<&str>,
    value: &str,
    reveal_requested: bool,
    upstream_version: Option<u64>,
    policy: &CacheValuePolicy,
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

pub fn parse_node_get_value(body: &str) -> (Option<String>, Option<u64>) {
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

fn sensitive_reasons(key: &str, value: &str, policy: &CacheValuePolicy) -> Vec<String> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_value_response_masks_and_flags_sensitive_values() {
        let policy = CacheValuePolicy::default();
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
        let policy = CacheValuePolicy {
            allow_value_reveal: true,
            ..Default::default()
        };

        let body = build_cache_value_response("profile", None, "plain-value", true, None, &policy);

        assert!(!body.masked);
        assert!(body.reveal_allowed);
        assert_eq!(body.value.as_deref(), Some("plain-value"));
    }

    #[test]
    fn cache_value_response_blocks_sensitive_reveal_without_sensitive_policy() {
        let policy = CacheValuePolicy {
            allow_value_reveal: true,
            ..Default::default()
        };
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

    #[test]
    fn parse_node_get_value_extracts_json_value_and_version() {
        let (value, version) = parse_node_get_value(r#"{"value":"stored-token","version":42}"#);

        assert_eq!(value.as_deref(), Some("stored-token"));
        assert_eq!(version, Some(42));

        let (value, version) = parse_node_get_value("legacy-raw-value");
        assert!(value.is_none());
        assert!(version.is_none());
    }

    fn test_jwt_like_value() -> String {
        [
            "eyJhbGciOiJIUzI1NiJ9",
            "eyJzdWIiOiIxMjM0NTY3ODkwIn0",
            "signature123",
        ]
        .join(".")
    }
}
