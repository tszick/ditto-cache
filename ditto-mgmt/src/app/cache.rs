use crate::api::SharedState;
use crate::node_client::{admin_rpc, http_authority_for_target, resolve_target};
use crate::policy::cache_value_redaction::{
    build_cache_value_response, parse_node_get_value,
};
use ditto_protocol::{AdminRequest, AdminResponse};
use reqwest::header::HeaderValue;
use reqwest::{RequestBuilder, Response};

pub fn is_valid_cache_key(key: &str) -> bool {
    if key.is_empty() {
        return false;
    }
    key.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':'))
}

pub fn normalize_namespace(ns: Option<String>) -> Option<String> {
    ns.map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
}

pub fn namespaced_key(namespace: &Option<String>, key: &str) -> String {
    match namespace {
        Some(ns) => format!("{}::{}", ns, key),
        None => key.to_string(),
    }
}

pub fn namespaced_pattern(namespace: &Option<String>, pattern: Option<String>) -> Option<String> {
    match (namespace, pattern) {
        (Some(ns), Some(p)) => Some(format!("{}::{}", ns, p)),
        (Some(ns), None) => Some(format!("{}::*", ns)),
        (None, p) => p,
    }
}

pub async fn resolve_cache_target_addrs(
    state: &SharedState,
    target: &str,
) -> Vec<std::net::SocketAddr> {
    if target == "all" {
        state.cluster_addrs().await
    } else {
        resolve_target(
            target,
            state.cfg.connection.cluster_port,
            &state.cfg.connection.seeds,
        )
        .await
    }
}

pub async fn collect_cache_stats(
    state: SharedState,
    target: &str,
) -> Vec<serde_json::Value> {
    let addrs = resolve_cache_target_addrs(&state, target).await;
    let mut results = Vec::new();
    for addr in addrs {
        match admin_rpc(addr, AdminRequest::GetStats, state.tls.as_ref()).await {
            Ok(AdminResponse::Stats(s)) => {
                let hit_rate = if s.hit_count + s.miss_count > 0 {
                    s.hit_count * 100 / (s.hit_count + s.miss_count)
                } else {
                    0
                };
                results.push(serde_json::json!({
                    "addr":               addr.to_string(),
                    "key_count":          s.key_count,
                    "memory_used_bytes":  s.memory_used_bytes,
                    "memory_max_bytes":   s.memory_max_bytes,
                    "evictions":          s.evictions,
                    "hit_count":          s.hit_count,
                    "miss_count":         s.miss_count,
                    "hit_rate_pct":       hit_rate,
                }));
            }
            Err(e) => {
                results.push(serde_json::json!({ "addr": addr.to_string(), "error": e.to_string() }))
            }
            _ => {}
        }
    }
    results
}

pub async fn collect_list_keys(
    state: SharedState,
    target: &str,
    pattern: Option<String>,
    namespace: &Option<String>,
) -> Vec<serde_json::Value> {
    let addrs = resolve_cache_target_addrs(&state, target).await;
    let mut results = Vec::new();
    for addr in addrs {
        match admin_rpc(
            addr,
            AdminRequest::ListKeys {
                pattern: pattern.clone(),
            },
            state.tls.as_ref(),
        )
        .await
        {
            Ok(AdminResponse::Keys(keys)) => {
                let keys: Vec<String> = keys
                    .into_iter()
                    .map(|k| strip_namespace_prefix(namespace, k))
                    .collect();
                results.push(serde_json::json!({ "addr": addr.to_string(), "keys": keys }));
            }
            Err(e) => {
                results.push(serde_json::json!({ "addr": addr.to_string(), "error": e.to_string() }))
            }
            _ => {}
        }
    }
    results
}

pub fn resolve_cache_http_authority(state: &SharedState, target: &str) -> Option<String> {
    http_authority_for_target(
        target,
        state.cfg.connection.cluster_port,
        &state.cfg.connection.seeds,
    )
}

fn invalid_target_json() -> serde_json::Value {
    serde_json::json!({ "error": "invalid target" })
}

fn cache_key_url(state: &SharedState, authority: &str, key: &str) -> String {
    format!(
        "{}://{}/key/{}",
        state.http_scheme(),
        authority,
        encode_key_for_url(key)
    )
}

async fn send_cache_request(
    req: RequestBuilder,
    state: &SharedState,
    namespace: Option<String>,
) -> Result<Response, (u16, String)> {
    node_http_request(req, state, namespace)
        .send()
        .await
        .map_err(|e| (502, e.to_string()))
}

async fn text_error_body(resp: Response) -> (u16, serde_json::Value) {
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    (status, serde_json::json!({ "error": body }))
}

pub async fn get_key(
    state: SharedState,
    target: &str,
    key: &str,
    namespace: Option<String>,
) -> (u16, String) {
    let Some(authority) = resolve_cache_http_authority(&state, target) else {
        return (400, "invalid target".to_string());
    };

    let url = cache_key_url(&state, &authority, key);

    match send_cache_request(state.http_client.get(&url), &state, namespace).await {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            (status, body)
        }
        Err(err) => err,
    }
}

pub async fn get_key_response(
    state: SharedState,
    target: &str,
    key: &str,
    namespace: Option<String>,
    reveal: bool,
) -> (u16, serde_json::Value) {
    let (status, body) = get_key(state.clone(), target, key, namespace.clone()).await;
    if status == 200 {
        let (parsed_value, upstream_version) = parse_node_get_value(&body);
        let value = parsed_value.as_deref().unwrap_or(&body);
        let value = build_cache_value_response(
            key,
            namespace.as_deref(),
            value,
            reveal,
            upstream_version,
            &state.cfg.cache_values,
        );
        (200, serde_json::to_value(value).unwrap_or_else(|_| serde_json::json!({ "error": "serialization failure" })))
    } else {
        (status, serde_json::json!({ "error": body }))
    }
}

pub async fn set_key(
    state: SharedState,
    target: &str,
    key: &str,
    value: &str,
    ttl_secs: Option<u64>,
    namespace: Option<String>,
) -> (u16, serde_json::Value) {
    let Some(authority) = resolve_cache_http_authority(&state, target) else {
        return (400, invalid_target_json());
    };

    let mut url = cache_key_url(&state, &authority, key);
    if let Some(ttl) = ttl_secs {
        url.push_str(&format!("?ttl={ttl}"));
    }

    match send_cache_request(
        state.http_client.put(&url).body(value.to_string()),
        &state,
        namespace,
    )
    .await
    {
        Ok(resp) if resp.status().is_success() => (200, serde_json::json!({ "ok": true })),
        Ok(resp) => text_error_body(resp).await,
        Err((status, message)) => (status, serde_json::json!({ "error": message })),
    }
}

pub async fn delete_key(
    state: SharedState,
    target: &str,
    key: &str,
    namespace: Option<String>,
) -> (u16, serde_json::Value) {
    let Some(authority) = resolve_cache_http_authority(&state, target) else {
        return (400, invalid_target_json());
    };

    let url = cache_key_url(&state, &authority, key);

    match send_cache_request(state.http_client.delete(&url), &state, namespace).await {
        Ok(resp) if resp.status().is_success() => (200, serde_json::json!({ "ok": true })),
        Ok(resp) => text_error_body(resp).await,
        Err((status, message)) => (status, serde_json::json!({ "error": message })),
    }
}

pub async fn flush_cache(state: SharedState, target: &str) -> Vec<serde_json::Value> {
    let addrs = resolve_cache_target_addrs(&state, target).await;
    let mut results = Vec::new();
    for addr in addrs {
        match admin_rpc(addr, AdminRequest::FlushCache, state.tls.as_ref()).await {
            Ok(_) => results.push(serde_json::json!({ "addr": addr.to_string(), "ok": true })),
            Err(e) => results.push(
                serde_json::json!({ "addr": addr.to_string(), "ok": false, "error": e.to_string() }),
            ),
        }
    }
    results
}

pub async fn flush_namespace(
    state: SharedState,
    target: &str,
    namespace: &str,
) -> Vec<serde_json::Value> {
    let addrs = resolve_cache_target_addrs(&state, target).await;
    let mut results = Vec::new();
    for addr in addrs {
        let authority = if target == "all" {
            cluster_addr_http_authority(addr)
        } else {
            resolve_cache_http_authority(&state, target).or_else(|| cluster_addr_http_authority(addr))
        };

        let Some(authority) = authority else {
            results.push(serde_json::json!({
                "addr": addr.to_string(),
                "ok": false,
                "deleted": 0,
                "namespace": namespace,
                "error": "invalid HTTP target"
            }));
            continue;
        };

        let url = format!("{}://{}/keys/delete-by-pattern", state.http_scheme(), authority);
        match node_http_request(
            state.http_client.post(&url),
            &state,
            Some(namespace.to_string()),
        )
        .json(&serde_json::json!({ "pattern": "*" }))
        .send()
        .await
        {
            Ok(resp) if resp.status().is_success() => {
                let body = resp.json::<serde_json::Value>().await.unwrap_or_default();
                results.push(serde_json::json!({
                    "addr": addr.to_string(),
                    "ok": true,
                    "deleted": body.get("deleted").and_then(|v| v.as_u64()).unwrap_or(0),
                    "namespace": namespace,
                    "error": null
                }));
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                results.push(serde_json::json!({
                    "addr": addr.to_string(),
                    "ok": false,
                    "deleted": 0,
                    "namespace": namespace,
                    "error": format!("HTTP {}: {}", status.as_u16(), body)
                }));
            }
            Err(e) => results.push(serde_json::json!({
                "addr": addr.to_string(),
                "ok": false,
                "deleted": 0,
                "namespace": namespace,
                "error": e.to_string()
            })),
        }
    }
    results
}

pub async fn set_compressed(
    state: SharedState,
    target: &str,
    key: &str,
    value: &str,
) -> Vec<serde_json::Value> {
    let addrs = resolve_cache_target_addrs(&state, target).await;
    let mut results = Vec::new();
    for addr in addrs {
        match admin_rpc(
            addr,
            AdminRequest::SetKeyProperty {
                key: key.to_string(),
                name: "compressed".into(),
                value: value.to_string(),
            },
            state.tls.as_ref(),
        )
        .await
        {
            Ok(AdminResponse::KeyPropertyUpdated) => results.push(serde_json::json!({
                "addr": addr.to_string(),
                "ok": true,
                "error": null
            })),
            Ok(AdminResponse::NotFound) => results.push(serde_json::json!({
                "addr": addr.to_string(),
                "ok": false,
                "error": "key not found"
            })),
            Ok(AdminResponse::Error { message }) => results.push(serde_json::json!({
                "addr": addr.to_string(),
                "ok": false,
                "error": message
            })),
            Err(e) => results.push(serde_json::json!({
                "addr": addr.to_string(),
                "ok": false,
                "error": e.to_string()
            })),
            _ => results.push(serde_json::json!({
                "addr": addr.to_string(),
                "ok": false,
                "error": "unexpected response"
            })),
        }
    }
    results
}

pub async fn set_keys_ttl(
    state: SharedState,
    target: &str,
    pattern: &str,
    ttl_secs: Option<u64>,
) -> Vec<serde_json::Value> {
    let addrs = resolve_cache_target_addrs(&state, target).await;
    let mut results = Vec::new();
    for addr in addrs {
        match admin_rpc(
            addr,
            AdminRequest::SetKeysTtl {
                pattern: pattern.to_string(),
                ttl_secs,
            },
            state.tls.as_ref(),
        )
        .await
        {
            Ok(AdminResponse::TtlUpdated { updated }) => results.push(serde_json::json!({
                "addr": addr.to_string(),
                "updated": updated,
                "error": null
            })),
            Err(e) => results.push(serde_json::json!({
                "addr": addr.to_string(),
                "updated": 0,
                "error": e.to_string()
            })),
            _ => results.push(serde_json::json!({
                "addr": addr.to_string(),
                "updated": 0,
                "error": "unexpected response"
            })),
        }
    }
    results
}

pub(crate) fn strip_namespace_prefix(namespace: &Option<String>, key: String) -> String {
    match namespace {
        Some(ns) => key
            .strip_prefix(&format!("{}::", ns))
            .unwrap_or(&key)
            .to_string(),
        None => key,
    }
}

fn cluster_addr_http_authority(addr: std::net::SocketAddr) -> Option<String> {
    addr.port()
        .checked_sub(1)
        .map(|http_port| format!("{}:{}", addr.ip(), http_port))
}

pub(crate) fn encode_key_for_url(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for b in key.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' => out.push(b as char),
            b => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

pub(crate) fn node_http_request(
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
            return req.header("x-ditto-namespace", v);
        }
    }
    req
}
