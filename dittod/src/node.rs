mod admin;
mod background;
mod client;
mod cluster;
mod hot_key;
mod lifecycle;
mod observability;
mod read_repair;
mod replication_apply;
mod tenancy;
mod traffic_policy;
mod util;
mod write;

use crate::{
    config::Config,
    replication::{ActiveSet, WriteLog},
    store::KvStore,
};
use bytes::Bytes;
use ditto_protocol::ClientResponse;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize},
        Arc, Mutex,
    },
    time::Instant,
};
use tokio::sync::{broadcast, watch, Mutex as AsyncMutex};
use tokio_rustls::TlsConnector;
use traffic_policy::TokenBucket;
use util::stable_node_uuid;
use uuid::Uuid;

/// DITTO-02: watch event payload broadcast to all TCP connections.
/// `value = None` means the key was deleted.
pub type WatchEventPayload = (String, Option<Bytes>, u64);
const PROTOCOL_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

impl CircuitState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Open => "open",
            Self::HalfOpen => "half-open",
        }
    }
}

#[derive(Debug)]
struct CircuitRuntime {
    state: CircuitState,
    consecutive_failures: u64,
    open_until_ms: u64,
    half_open_successes: u64,
}

#[derive(Debug)]
struct GetFlight {
    tx: watch::Sender<Option<ClientResponse>>,
    waiters: AtomicUsize,
}

#[derive(Debug, Clone)]
struct HotStaleEntry {
    response: ClientResponse,
    recorded_at_ms: u64,
}

#[derive(Debug, Clone)]
struct HotKeyAdaptiveState {
    limit: usize,
    success_streak: u32,
    last_touched_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientRequestSource {
    Tcp,
    Http,
    Internal,
}

#[derive(Debug, Clone)]
enum CoordinatedWriteRequest {
    Set {
        value: Bytes,
        ttl_secs: Option<u64>,
    },
    Delete,
    SetNx {
        value: Bytes,
        ttl_secs: Option<u64>,
    },
    Incr {
        delta: i64,
        ttl_secs_on_create: Option<u64>,
    },
}

const NAMESPACE_LATENCY_TOP_LIMIT: usize = 5;
const NAMESPACE_LATENCY_STATE_MAX: usize = 256;
const HOT_KEY_TOP_LIMIT: usize = 10;
const HOT_KEY_STATE_MAX: usize = 1024;

#[derive(Debug, Default, Clone)]
struct NamespaceLatencyRuntime {
    request_total: u64,
    buckets: [u64; 6],
}

impl GetFlight {
    fn new() -> Self {
        let (tx, _rx) = watch::channel(None);
        Self {
            tx,
            waiters: AtomicUsize::new(0),
        }
    }
}

/// Shared state handle passed to every server task.
pub struct NodeHandle {
    pub id: Uuid,
    /// Runtime-mutable config (port changes write back to file).
    pub config: Mutex<Config>,
    /// Path to the node.toml config file for persistence.
    pub config_path: String,
    pub store: Arc<KvStore>,
    pub write_log: Arc<AsyncMutex<WriteLog>>,
    write_path_lock: Arc<AsyncMutex<()>>,
    pub active_set: Arc<AsyncMutex<ActiveSet>>,
    pub started_at: Instant,
    /// Runtime active/inactive toggle (set via SetProperty "active").
    /// Initialised from config.node.active; can be flipped without restart.
    pub active: AtomicBool,
    /// TLS connector for outbound cluster connections; None when TLS is disabled.
    pub tls_connector: Option<TlsConnector>,
    /// DITTO-02: broadcast channel for watch events.
    /// Each TCP connection subscribes and filters by its watched keys.
    pub watch_tx: broadcast::Sender<WatchEventPayload>,
    rate_bucket: Mutex<TokenBucket>,
    circuit: Mutex<CircuitRuntime>,
    rate_limited_requests_total: AtomicU64,
    circuit_breaker_open_total: AtomicU64,
    circuit_breaker_reject_total: AtomicU64,
    client_requests_total: AtomicU64,
    client_requests_tcp_total: AtomicU64,
    client_requests_http_total: AtomicU64,
    client_requests_internal_total: AtomicU64,
    client_request_latency_le_1ms_total: AtomicU64,
    client_request_latency_le_5ms_total: AtomicU64,
    client_request_latency_le_20ms_total: AtomicU64,
    client_request_latency_le_100ms_total: AtomicU64,
    client_request_latency_le_500ms_total: AtomicU64,
    client_request_latency_gt_500ms_total: AtomicU64,
    client_error_total: AtomicU64,
    client_errors_tcp_total: AtomicU64,
    client_errors_http_total: AtomicU64,
    client_errors_internal_total: AtomicU64,
    client_error_auth_total: AtomicU64,
    client_error_throttle_total: AtomicU64,
    client_error_availability_total: AtomicU64,
    client_error_validation_total: AtomicU64,
    client_error_internal_total: AtomicU64,
    client_error_other_total: AtomicU64,
    get_flights: AsyncMutex<HashMap<String, Arc<GetFlight>>>,
    hot_key_stale_cache: AsyncMutex<HashMap<String, HotStaleEntry>>,
    hot_key_adaptive_state: Mutex<HashMap<String, HotKeyAdaptiveState>>,
    hot_key_coalesced_hits_total: AtomicU64,
    hot_key_fallback_exec_total: AtomicU64,
    hot_key_wait_timeout_total: AtomicU64,
    hot_key_stale_served_total: AtomicU64,
    hot_key_adaptive_limit_increase_total: AtomicU64,
    hot_key_adaptive_limit_decrease_total: AtomicU64,
    namespace_latency_runtime: Mutex<HashMap<String, NamespaceLatencyRuntime>>,
    hot_key_usage_runtime: Mutex<HashMap<String, u64>>,
    read_repair_trigger_total: AtomicU64,
    read_repair_success_total: AtomicU64,
    read_repair_throttled_total: AtomicU64,
    read_repair_budget_exhausted_total: AtomicU64,
    read_repair_budget_window_start_ms: AtomicU64,
    read_repair_budget_window_count: AtomicU64,
    namespace_quota_reject_total: AtomicU64,
    namespace_quota_reject_last_total: AtomicU64,
    namespace_quota_reject_last_ts_ms: AtomicU64,
    last_read_repair_trigger_ms: AtomicU64,
    last_anti_entropy_repair_trigger_ms: AtomicU64,
    anti_entropy_runs_total: AtomicU64,
    anti_entropy_repair_trigger_total: AtomicU64,
    anti_entropy_repair_throttled_total: AtomicU64,
    anti_entropy_last_detected_lag: AtomicU64,
    anti_entropy_key_checks_total: AtomicU64,
    anti_entropy_key_mismatch_total: AtomicU64,
    anti_entropy_full_reconcile_runs_total: AtomicU64,
    anti_entropy_full_reconcile_key_checks_total: AtomicU64,
    anti_entropy_full_reconcile_mismatch_total: AtomicU64,
    anti_entropy_budget_exhausted_total: AtomicU64,
    mixed_version_probe_runs_total: AtomicU64,
    mixed_version_peers_detected_total: AtomicU64,
    mixed_version_probe_errors_total: AtomicU64,
    mixed_version_last_detected_peer_count: AtomicU64,
    snapshot_last_load_path: Mutex<Option<String>>,
    snapshot_last_load_duration_ms: AtomicU64,
    snapshot_last_load_entries: AtomicU64,
    snapshot_last_load_completed_at_ms: AtomicU64,
    snapshot_restore_attempt_total: AtomicU64,
    snapshot_restore_success_total: AtomicU64,
    snapshot_restore_failure_total: AtomicU64,
    snapshot_restore_not_found_total: AtomicU64,
    snapshot_restore_policy_block_total: AtomicU64,
}

impl NodeHandle {
    /// Construct a new [`NodeHandle`] wrapped in an [`Arc`].
    ///
    /// A stable UUID is derived from the configured node id.
    /// The [`ActiveSet`] is initialised with this node as the only known member.
    pub fn new(
        config: Config,
        config_path: String,
        store: Arc<KvStore>,
        tls_connector: Option<TlsConnector>,
        cluster_bind_ip: String,
    ) -> Arc<Self> {
        let id = stable_node_uuid(&config.node.id);
        let local_addr: SocketAddr = format!("{}:{}", cluster_bind_ip, config.node.cluster_port)
            .parse()
            .unwrap_or_else(|_| "0.0.0.0:7779".parse().unwrap());

        let active_set = ActiveSet::new(
            id,
            local_addr,
            config.node.cluster_port,
            config.replication.gossip_dead_ms,
            config.cluster.max_nodes,
        );

        let (watch_tx, _) = broadcast::channel(256);

        Arc::new(Self {
            id,
            active: AtomicBool::new(config.node.active),
            config: Mutex::new(config.clone()),
            config_path,
            store,
            write_log: Arc::new(AsyncMutex::new(WriteLog::new())),
            write_path_lock: Arc::new(AsyncMutex::new(())),
            active_set: Arc::new(AsyncMutex::new(active_set)),
            started_at: Instant::now(),
            tls_connector,
            watch_tx,
            rate_bucket: Mutex::new(TokenBucket::new(
                config.rate_limit.burst,
                config.rate_limit.requests_per_sec,
            )),
            circuit: Mutex::new(CircuitRuntime {
                state: CircuitState::Closed,
                consecutive_failures: 0,
                open_until_ms: 0,
                half_open_successes: 0,
            }),
            rate_limited_requests_total: AtomicU64::new(0),
            circuit_breaker_open_total: AtomicU64::new(0),
            circuit_breaker_reject_total: AtomicU64::new(0),
            client_requests_total: AtomicU64::new(0),
            client_requests_tcp_total: AtomicU64::new(0),
            client_requests_http_total: AtomicU64::new(0),
            client_requests_internal_total: AtomicU64::new(0),
            client_request_latency_le_1ms_total: AtomicU64::new(0),
            client_request_latency_le_5ms_total: AtomicU64::new(0),
            client_request_latency_le_20ms_total: AtomicU64::new(0),
            client_request_latency_le_100ms_total: AtomicU64::new(0),
            client_request_latency_le_500ms_total: AtomicU64::new(0),
            client_request_latency_gt_500ms_total: AtomicU64::new(0),
            client_error_total: AtomicU64::new(0),
            client_errors_tcp_total: AtomicU64::new(0),
            client_errors_http_total: AtomicU64::new(0),
            client_errors_internal_total: AtomicU64::new(0),
            client_error_auth_total: AtomicU64::new(0),
            client_error_throttle_total: AtomicU64::new(0),
            client_error_availability_total: AtomicU64::new(0),
            client_error_validation_total: AtomicU64::new(0),
            client_error_internal_total: AtomicU64::new(0),
            client_error_other_total: AtomicU64::new(0),
            get_flights: AsyncMutex::new(HashMap::new()),
            hot_key_stale_cache: AsyncMutex::new(HashMap::new()),
            hot_key_adaptive_state: Mutex::new(HashMap::new()),
            hot_key_coalesced_hits_total: AtomicU64::new(0),
            hot_key_fallback_exec_total: AtomicU64::new(0),
            hot_key_wait_timeout_total: AtomicU64::new(0),
            hot_key_stale_served_total: AtomicU64::new(0),
            hot_key_adaptive_limit_increase_total: AtomicU64::new(0),
            hot_key_adaptive_limit_decrease_total: AtomicU64::new(0),
            namespace_latency_runtime: Mutex::new(HashMap::new()),
            hot_key_usage_runtime: Mutex::new(HashMap::new()),
            read_repair_trigger_total: AtomicU64::new(0),
            read_repair_success_total: AtomicU64::new(0),
            read_repair_throttled_total: AtomicU64::new(0),
            read_repair_budget_exhausted_total: AtomicU64::new(0),
            read_repair_budget_window_start_ms: AtomicU64::new(0),
            read_repair_budget_window_count: AtomicU64::new(0),
            namespace_quota_reject_total: AtomicU64::new(0),
            namespace_quota_reject_last_total: AtomicU64::new(0),
            namespace_quota_reject_last_ts_ms: AtomicU64::new(0),
            last_read_repair_trigger_ms: AtomicU64::new(0),
            last_anti_entropy_repair_trigger_ms: AtomicU64::new(0),
            anti_entropy_runs_total: AtomicU64::new(0),
            anti_entropy_repair_trigger_total: AtomicU64::new(0),
            anti_entropy_repair_throttled_total: AtomicU64::new(0),
            anti_entropy_last_detected_lag: AtomicU64::new(0),
            anti_entropy_key_checks_total: AtomicU64::new(0),
            anti_entropy_key_mismatch_total: AtomicU64::new(0),
            anti_entropy_full_reconcile_runs_total: AtomicU64::new(0),
            anti_entropy_full_reconcile_key_checks_total: AtomicU64::new(0),
            anti_entropy_full_reconcile_mismatch_total: AtomicU64::new(0),
            anti_entropy_budget_exhausted_total: AtomicU64::new(0),
            mixed_version_probe_runs_total: AtomicU64::new(0),
            mixed_version_peers_detected_total: AtomicU64::new(0),
            mixed_version_probe_errors_total: AtomicU64::new(0),
            mixed_version_last_detected_peer_count: AtomicU64::new(0),
            snapshot_last_load_path: Mutex::new(None),
            snapshot_last_load_duration_ms: AtomicU64::new(0),
            snapshot_last_load_entries: AtomicU64::new(0),
            snapshot_last_load_completed_at_ms: AtomicU64::new(0),
            snapshot_restore_attempt_total: AtomicU64::new(0),
            snapshot_restore_success_total: AtomicU64::new(0),
            snapshot_restore_failure_total: AtomicU64::new(0),
            snapshot_restore_not_found_total: AtomicU64::new(0),
            snapshot_restore_policy_block_total: AtomicU64::new(0),
        })
    }
}

#[cfg(test)]
mod tests;
