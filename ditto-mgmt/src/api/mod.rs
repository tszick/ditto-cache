//! Axum HTTP router and shared application state for `ditto-mgmt`.

pub mod cache;
pub mod cluster;
pub mod doctor;
pub mod nodes;

use crate::audit::audit_middleware;
use crate::auth::admin_auth_middleware;
use crate::config::MgmtConfig;
use crate::node_client::all_cluster_addrs;
use axum::{
    body::Body,
    http::{
        header::{self, HeaderName, HeaderValue},
        HeaderMap, Request,
    },
    middleware,
    middleware::Next,
    response::Response,
    routing::{get, post},
    Router,
};
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tokio_rustls::TlsConnector;

/// TTL for the cluster-address discovery cache.
/// `all_cluster_addrs()` makes a DNS lookup + one ClusterStatus RPC on every call;
/// caching avoids that overhead on every 3-second UI poll.
const ADDR_CACHE_TTL: Duration = Duration::from_secs(10);
const CONTENT_SECURITY_POLICY: &str = concat!(
    "default-src 'self'; ",
    "base-uri 'none'; ",
    "object-src 'none'; ",
    "frame-ancestors 'none'; ",
    "form-action 'self'; ",
    "connect-src 'self'; ",
    "img-src 'self' data:; ",
    "font-src 'self' data:; ",
    "style-src 'self' 'sha256-zout71JFYhls/gibmwN63ihb96tmhzhpWukdBnZh+Yc='; ",
    "script-src 'self' 'sha256-DalhQAcWf8gFr0EPEf+nU8O0ufgjGPVW0TnkraCMESE='"
);

/// Shared application state injected into every Axum handler via [`axum::extract::State`].
pub struct AppState {
    /// Loaded configuration (server, connection, TLS).
    pub cfg: Arc<MgmtConfig>,
    /// Optional mTLS connector for outbound admin RPC calls to nodes.
    pub tls: Option<TlsConnector>,
    /// Reusable HTTP client for proxying cache data requests to node HTTP ports.
    /// Configured with the CA cert trust store when TLS is enabled.
    pub http_client: reqwest::Client,
    /// Short-lived cache: (addresses, timestamp).  TTL = [`ADDR_CACHE_TTL`].
    /// Avoids a fresh DNS + ClusterStatus RPC on every `/api/nodes` poll.
    pub addr_cache: Mutex<Option<(Vec<SocketAddr>, Instant)>>,
}

impl AppState {
    /// Returns the cluster node addresses, using the cache when fresh.
    pub async fn cluster_addrs(&self) -> Vec<SocketAddr> {
        {
            let cache = self.addr_cache.lock().await;
            if let Some((ref addrs, at)) = *cache {
                if at.elapsed() < ADDR_CACHE_TTL {
                    return addrs.clone();
                }
            }
        }
        let addrs = all_cluster_addrs(
            &self.cfg.connection.seeds,
            self.cfg.connection.cluster_port,
            self.tls.as_ref(),
        )
        .await;
        *self.addr_cache.lock().await = Some((addrs.clone(), Instant::now()));
        addrs
    }

    /// URL scheme for proxying to dittod's HTTP REST port (7778).
    pub fn http_scheme(&self) -> &'static str {
        if self.cfg.tls.enabled {
            "https"
        } else {
            "http"
        }
    }
}

/// Type alias for the Arc-wrapped state passed to Axum handlers.
pub type SharedState = Arc<AppState>;

pub fn build_router(state: SharedState) -> Router {
    Router::new()
        // Node endpoints
        .route("/api/nodes", get(nodes::list_nodes))
        .route("/api/nodes/{target}/status", get(nodes::node_status))
        .route("/api/nodes/{target}/describe", get(nodes::node_describe))
        .route(
            "/api/nodes/{target}/property/{name}",
            get(nodes::get_property).post(nodes::set_property),
        )
        .route("/api/nodes/{target}/set-active", post(nodes::set_active))
        .route("/api/nodes/{target}/backup", post(nodes::backup_node))
        .route(
            "/api/nodes/{target}/restore-snapshot",
            post(nodes::restore_snapshot),
        )
        // Cluster endpoints
        .route("/api/cluster", get(cluster::cluster_status))
        .route("/api/cluster/primary", get(cluster::cluster_primary))
        .route("/api/doctor", get(doctor::doctor_status))
        // Cache admin endpoints (via admin TCP)
        .route("/api/cache/{target}/stats", get(cache::cache_stats))
        .route("/api/cache/{target}/flush", post(cache::flush_cache))
        .route("/api/cache/{target}/ttl", post(cache::set_keys_ttl))
        .route("/api/cache/{target}/keys", get(cache::list_keys))
        .route(
            "/api/cache/{target}/keys/{key}/compressed",
            post(cache::set_compressed),
        )
        // Cache data endpoints (proxied to node HTTP port)
        .route(
            "/api/cache/{target}/keys/{key}",
            get(cache::get_key)
                .put(cache::set_key)
                .delete(cache::delete_key),
        )
        // Web UI
        .route("/", get(crate::web::serve_index))
        .route("/ditto-logo.png", get(crate::web::serve_logo))
        .route("/assets/{*path}", get(crate::web::serve_asset))
        // Admin auth across all routes (no-op when [admin] auth is not configured).
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            admin_auth_middleware,
        ))
        .layer(middleware::from_fn(audit_middleware))
        .layer(middleware::from_fn(security_headers_middleware))
        .with_state(state)
}

async fn security_headers_middleware(req: Request<Body>, next: Next) -> Response {
    let mut response = next.run(req).await;
    apply_security_headers(response.headers_mut());
    response
}

fn apply_security_headers(headers: &mut HeaderMap) {
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, max-age=0"),
    );
    headers.insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::STRICT_TRANSPORT_SECURITY,
        HeaderValue::from_static("max-age=31536000; includeSubDomains"),
    );
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(CONTENT_SECURITY_POLICY),
    );
    headers.insert(
        HeaderName::from_static("x-frame-options"),
        HeaderValue::from_static("DENY"),
    );
    headers.insert(
        HeaderName::from_static("referrer-policy"),
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(
        HeaderName::from_static("permissions-policy"),
        HeaderValue::from_static("camera=(), microphone=(), geolocation=()"),
    );
    headers.insert(
        HeaderName::from_static("cross-origin-opener-policy"),
        HeaderValue::from_static("same-origin"),
    );
    headers.insert(
        HeaderName::from_static("cross-origin-resource-policy"),
        HeaderValue::from_static("same-origin"),
    );
    headers.insert(
        HeaderName::from_static("cross-origin-embedder-policy"),
        HeaderValue::from_static("credentialless"),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MgmtConfig;
    use base64::Engine;
    use sha2::{Digest, Sha256};

    fn test_state(mut cfg: MgmtConfig) -> SharedState {
        cfg.connection.seeds.clear();
        Arc::new(AppState {
            cfg: Arc::new(cfg),
            tls: None,
            http_client: reqwest::Client::new(),
            addr_cache: Mutex::new(None),
        })
    }

    #[test]
    fn http_scheme_tracks_tls_enabled_flag() {
        let mut cfg = MgmtConfig::default();
        let state = test_state(cfg.clone());
        assert_eq!(state.http_scheme(), "http");

        cfg.tls.enabled = true;
        let tls_state = test_state(cfg);
        assert_eq!(tls_state.http_scheme(), "https");
    }

    #[tokio::test]
    async fn cluster_addrs_falls_back_to_localhost_when_no_seeds_exist() {
        let state = test_state(MgmtConfig::default());
        let addrs = state.cluster_addrs().await;

        assert_eq!(addrs.len(), 1);
        assert_eq!(addrs[0].ip().to_string(), "127.0.0.1");
        assert_eq!(addrs[0].port(), 7779);
        assert!(state.addr_cache.lock().await.is_some());
    }

    #[tokio::test]
    async fn cluster_addrs_uses_fresh_cached_values() {
        let state = test_state(MgmtConfig::default());
        let cached: SocketAddr = "10.1.2.3:7779".parse().unwrap();
        *state.addr_cache.lock().await = Some((vec![cached], Instant::now()));

        let addrs = state.cluster_addrs().await;
        assert_eq!(addrs, vec![cached]);
    }

    #[test]
    fn build_router_constructs_all_routes_with_shared_state() {
        let state = test_state(MgmtConfig::default());
        let _router = build_router(state);
    }

    #[tokio::test]
    async fn security_headers_are_added_to_responses() {
        let mut headers = HeaderMap::new();
        apply_security_headers(&mut headers);

        assert_eq!(
            headers.get(header::X_CONTENT_TYPE_OPTIONS).unwrap(),
            "nosniff"
        );
        assert_eq!(headers.get("x-frame-options").unwrap(), "DENY");
        assert!(headers
            .get(header::CONTENT_SECURITY_POLICY)
            .unwrap()
            .to_str()
            .unwrap()
            .contains("frame-ancestors 'none'"));
    }

    #[test]
    fn content_security_policy_hashes_match_embedded_web_ui() {
        let html = include_str!("../web/index.html");
        let style = tag_body(html, "<style>", "</style>");
        let script = tag_body(html, "<script>", "</script>");
        let style_hash = csp_hash(style);
        let script_hash = csp_hash(script);

        assert!(CONTENT_SECURITY_POLICY.contains(&format!("'sha256-{style_hash}'")));
        assert!(CONTENT_SECURITY_POLICY.contains(&format!("'sha256-{script_hash}'")));
    }

    fn tag_body<'a>(input: &'a str, start: &str, end: &str) -> &'a str {
        let start_idx = input.find(start).unwrap() + start.len();
        let end_idx = input[start_idx..].find(end).unwrap() + start_idx;
        &input[start_idx..end_idx]
    }

    fn csp_hash(input: &str) -> String {
        let digest = Sha256::digest(input.as_bytes());
        base64::engine::general_purpose::STANDARD.encode(digest)
    }
}
