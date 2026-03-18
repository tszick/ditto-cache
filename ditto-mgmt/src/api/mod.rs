//! Axum HTTP router and shared application state for `ditto-mgmt`.

pub mod cache;
pub mod cluster;
pub mod nodes;

use crate::auth::basic_auth_middleware;
use crate::config::MgmtConfig;
use crate::node_client::all_cluster_addrs;
use axum::{
    Router,
    middleware,
    routing::{get, post},
};
use std::{net::SocketAddr, sync::Arc, time::{Duration, Instant}};
use tokio::sync::Mutex;
use tokio_rustls::TlsConnector;

/// TTL for the cluster-address discovery cache.
/// `all_cluster_addrs()` makes a DNS lookup + one ClusterStatus RPC on every call;
/// caching avoids that overhead on every 3-second UI poll.
const ADDR_CACHE_TTL: Duration = Duration::from_secs(10);

/// Shared application state injected into every Axum handler via [`axum::extract::State`].
pub struct AppState {
    /// Loaded configuration (server, connection, TLS).
    pub cfg:         Arc<MgmtConfig>,
    /// Optional mTLS connector for outbound admin RPC calls to nodes.
    pub tls:         Option<TlsConnector>,
    /// Reusable HTTP client for proxying cache data requests to node HTTP ports.
    /// Configured with the CA cert trust store when TLS is enabled.
    pub http_client: reqwest::Client,
    /// Short-lived cache: (addresses, timestamp).  TTL = [`ADDR_CACHE_TTL`].
    /// Avoids a fresh DNS + ClusterStatus RPC on every `/api/nodes` poll.
    pub addr_cache:  Mutex<Option<(Vec<SocketAddr>, Instant)>>,
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
        ).await;
        *self.addr_cache.lock().await = Some((addrs.clone(), Instant::now()));
        addrs
    }

    /// URL scheme for proxying to dittod's HTTP REST port (7778).
    pub fn http_scheme(&self) -> &'static str {
        if self.cfg.tls.enabled { "https" } else { "http" }
    }
}

/// Type alias for the Arc-wrapped state passed to Axum handlers.
pub type SharedState = Arc<AppState>;

pub fn build_router(state: SharedState) -> Router {
    Router::new()
        // Node endpoints
        .route("/api/nodes",                                   get(nodes::list_nodes))
        .route("/api/nodes/:target/status",                    get(nodes::node_status))
        .route("/api/nodes/:target/describe",                  get(nodes::node_describe))
        .route("/api/nodes/:target/property/:name",
               get(nodes::get_property).post(nodes::set_property))
        .route("/api/nodes/:target/set-active",                post(nodes::set_active))
        .route("/api/nodes/:target/backup",                    post(nodes::backup_node))
        // Cluster endpoints
        .route("/api/cluster",                                 get(cluster::cluster_status))
        .route("/api/cluster/primary",                         get(cluster::cluster_primary))
        // Cache admin endpoints (via admin TCP)
        .route("/api/cache/:target/stats",                     get(cache::cache_stats))
        .route("/api/cache/:target/flush",                     post(cache::flush_cache))
        .route("/api/cache/:target/ttl",                       post(cache::set_keys_ttl))
        .route("/api/cache/:target/keys",                      get(cache::list_keys))
        .route("/api/cache/:target/keys/:key/compressed",      post(cache::set_compressed))
        // Cache data endpoints (proxied to node HTTP port)
        .route("/api/cache/:target/keys/:key",
               get(cache::get_key).put(cache::set_key).delete(cache::delete_key))
        // Web UI
        .route("/",                                            get(crate::web::serve_index))
        // Basic Auth across all routes (no-op when [admin] not configured).
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            basic_auth_middleware,
        ))
        .with_state(state)
}
