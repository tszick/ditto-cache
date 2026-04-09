use crate::{
    config::Config,
    network::cluster_server::send_cluster,
    replication::{ActiveSet, WriteLog},
    store::{
        kv_store::{sanitize_for_log, LimitError},
        KvStore,
    },
};
use bytes::Bytes;
use ditto_protocol::{
    AdminRequest, AdminResponse, ClientRequest, ClientResponse, ClusterMessage, ErrorCode,
    NamespaceQuotaUsage, NodeStats, NodeStatus,
};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{broadcast, watch, Mutex as AsyncMutex};
use tokio_rustls::TlsConnector;
use tracing::{info, warn};
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
struct TokenBucket {
    capacity: f64,
    tokens: f64,
    refill_per_sec: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(capacity: u64, refill_per_sec: u64) -> Self {
        let cap = capacity.max(1) as f64;
        let rps = refill_per_sec.max(1) as f64;
        Self {
            capacity: cap,
            tokens: cap,
            refill_per_sec: rps,
            last_refill: Instant::now(),
        }
    }

    fn try_take(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;
        self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientRequestSource {
    Tcp,
    Http,
    Internal,
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
    hot_key_coalesced_hits_total: AtomicU64,
    hot_key_fallback_exec_total: AtomicU64,
    read_repair_trigger_total: AtomicU64,
    read_repair_success_total: AtomicU64,
    read_repair_throttled_total: AtomicU64,
    namespace_quota_reject_total: AtomicU64,
    namespace_quota_reject_last_total: AtomicU64,
    namespace_quota_reject_last_ts_ms: AtomicU64,
    last_read_repair_trigger_ms: AtomicU64,
    anti_entropy_runs_total: AtomicU64,
    anti_entropy_repair_trigger_total: AtomicU64,
    anti_entropy_last_detected_lag: AtomicU64,
    anti_entropy_key_checks_total: AtomicU64,
    anti_entropy_key_mismatch_total: AtomicU64,
    anti_entropy_full_reconcile_runs_total: AtomicU64,
    anti_entropy_full_reconcile_key_checks_total: AtomicU64,
    anti_entropy_full_reconcile_mismatch_total: AtomicU64,
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
    /// A fresh random UUID is assigned as the node identity.
    /// The [`ActiveSet`] is initialised with this node as the only known member.
    pub fn new(
        config: Config,
        config_path: String,
        store: Arc<KvStore>,
        tls_connector: Option<TlsConnector>,
        cluster_bind_ip: String,
    ) -> Arc<Self> {
        let id = Uuid::new_v4();
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
            hot_key_coalesced_hits_total: AtomicU64::new(0),
            hot_key_fallback_exec_total: AtomicU64::new(0),
            read_repair_trigger_total: AtomicU64::new(0),
            read_repair_success_total: AtomicU64::new(0),
            read_repair_throttled_total: AtomicU64::new(0),
            namespace_quota_reject_total: AtomicU64::new(0),
            namespace_quota_reject_last_total: AtomicU64::new(0),
            namespace_quota_reject_last_ts_ms: AtomicU64::new(0),
            last_read_repair_trigger_ms: AtomicU64::new(0),
            anti_entropy_runs_total: AtomicU64::new(0),
            anti_entropy_repair_trigger_total: AtomicU64::new(0),
            anti_entropy_last_detected_lag: AtomicU64::new(0),
            anti_entropy_key_checks_total: AtomicU64::new(0),
            anti_entropy_key_mismatch_total: AtomicU64::new(0),
            anti_entropy_full_reconcile_runs_total: AtomicU64::new(0),
            anti_entropy_full_reconcile_key_checks_total: AtomicU64::new(0),
            anti_entropy_full_reconcile_mismatch_total: AtomicU64::new(0),
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

    pub fn record_snapshot_restore(&self, path: String, entries: u64, duration_ms: u64) {
        *self.snapshot_last_load_path.lock().unwrap() = Some(path);
        self.snapshot_last_load_entries
            .store(entries, Ordering::Relaxed);
        self.snapshot_last_load_duration_ms
            .store(duration_ms, Ordering::Relaxed);
        self.snapshot_last_load_completed_at_ms
            .store(Self::now_millis(), Ordering::Relaxed);
    }

    // -----------------------------------------------------------------------
    // Client request handler (used by both TCP and HTTP servers)
    // -----------------------------------------------------------------------

    fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    fn namespace_context(
        &self,
        namespace: Option<String>,
    ) -> Result<Option<String>, ClientResponse> {
        let cfg = self.config.lock().unwrap();
        if !cfg.tenancy.enabled {
            return Ok(None);
        }
        let ns = namespace
            .unwrap_or_else(|| cfg.tenancy.default_namespace.clone())
            .trim()
            .to_string();
        if ns.is_empty() || ns.contains("::") {
            return Err(ClientResponse::Error {
                code: ErrorCode::InternalError,
                message: "Invalid namespace".into(),
            });
        }
        Ok(Some(ns))
    }

    fn namespaced_key(&self, namespace: &Option<String>, key: &str) -> String {
        match namespace {
            Some(ns) => format!("{}::{}", ns, key),
            None => key.to_string(),
        }
    }

    fn namespaced_pattern(&self, namespace: &Option<String>, pattern: &str) -> String {
        match namespace {
            Some(ns) => format!("{}::{}", ns, pattern),
            None => pattern.to_string(),
        }
    }

    fn namespace_key_count(&self, namespace: &str) -> usize {
        let pattern = format!("{}::*", namespace);
        self.store.keys(Some(&pattern)).len()
    }

    fn allow_by_rate_limit(&self) -> bool {
        let (enabled, burst, rps) = {
            let cfg = self.config.lock().unwrap();
            (
                cfg.rate_limit.enabled,
                cfg.rate_limit.burst.max(1),
                cfg.rate_limit.requests_per_sec.max(1),
            )
        };
        if !enabled {
            return true;
        }

        let mut bucket = self.rate_bucket.lock().unwrap();
        if (bucket.capacity as u64) != burst || (bucket.refill_per_sec as u64) != rps {
            *bucket = TokenBucket::new(burst, rps);
        }
        let allowed = bucket.try_take();
        if !allowed {
            self.rate_limited_requests_total
                .fetch_add(1, Ordering::Relaxed);
        }
        allowed
    }

    fn allow_by_circuit_breaker(&self) -> bool {
        let (enabled, open_ms, half_open_max_requests) = {
            let cfg = self.config.lock().unwrap();
            (
                cfg.circuit_breaker.enabled,
                cfg.circuit_breaker.open_ms.max(1),
                cfg.circuit_breaker.half_open_max_requests.max(1),
            )
        };
        if !enabled {
            return true;
        }

        let now_ms = Self::now_millis();
        let mut c = self.circuit.lock().unwrap();
        match c.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                if now_ms >= c.open_until_ms {
                    c.state = CircuitState::HalfOpen;
                    c.half_open_successes = 0;
                    true
                } else {
                    self.circuit_breaker_reject_total
                        .fetch_add(1, Ordering::Relaxed);
                    false
                }
            }
            CircuitState::HalfOpen => {
                if c.half_open_successes >= half_open_max_requests {
                    c.state = CircuitState::Closed;
                    c.consecutive_failures = 0;
                    c.half_open_successes = 0;
                    true
                } else {
                    let _ = open_ms; // keep config parity and avoid stale assumptions
                    true
                }
            }
        }
    }

    fn record_circuit_result(&self, response: &ClientResponse) {
        let (enabled, threshold, open_ms, half_open_max_requests) = {
            let cfg = self.config.lock().unwrap();
            (
                cfg.circuit_breaker.enabled,
                cfg.circuit_breaker.failure_threshold.max(1),
                cfg.circuit_breaker.open_ms.max(1),
                cfg.circuit_breaker.half_open_max_requests.max(1),
            )
        };
        if !enabled {
            return;
        }

        let failed = matches!(
            response,
            ClientResponse::Error {
                code: ErrorCode::WriteTimeout | ErrorCode::NoQuorum,
                ..
            }
        );
        let now_ms = Self::now_millis();
        let mut c = self.circuit.lock().unwrap();
        match c.state {
            CircuitState::Closed => {
                if failed {
                    c.consecutive_failures += 1;
                    if c.consecutive_failures >= threshold {
                        c.state = CircuitState::Open;
                        c.open_until_ms = now_ms.saturating_add(open_ms);
                        c.half_open_successes = 0;
                        self.circuit_breaker_open_total
                            .fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    c.consecutive_failures = 0;
                }
            }
            CircuitState::Open => {}
            CircuitState::HalfOpen => {
                if failed {
                    c.state = CircuitState::Open;
                    c.open_until_ms = now_ms.saturating_add(open_ms);
                    c.half_open_successes = 0;
                    c.consecutive_failures = threshold;
                    self.circuit_breaker_open_total
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    c.half_open_successes += 1;
                    if c.half_open_successes >= half_open_max_requests {
                        c.state = CircuitState::Closed;
                        c.consecutive_failures = 0;
                        c.half_open_successes = 0;
                    }
                }
            }
        }
    }

    fn observe_client_response_metrics(
        &self,
        source: ClientRequestSource,
        elapsed: Duration,
        response: &ClientResponse,
    ) {
        self.client_requests_total.fetch_add(1, Ordering::Relaxed);
        match source {
            ClientRequestSource::Tcp => {
                self.client_requests_tcp_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            ClientRequestSource::Http => {
                self.client_requests_http_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            ClientRequestSource::Internal => {
                self.client_requests_internal_total
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        let ms = elapsed.as_millis() as u64;
        if ms <= 1 {
            self.client_request_latency_le_1ms_total
                .fetch_add(1, Ordering::Relaxed);
        } else if ms <= 5 {
            self.client_request_latency_le_5ms_total
                .fetch_add(1, Ordering::Relaxed);
        } else if ms <= 20 {
            self.client_request_latency_le_20ms_total
                .fetch_add(1, Ordering::Relaxed);
        } else if ms <= 100 {
            self.client_request_latency_le_100ms_total
                .fetch_add(1, Ordering::Relaxed);
        } else if ms <= 500 {
            self.client_request_latency_le_500ms_total
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.client_request_latency_gt_500ms_total
                .fetch_add(1, Ordering::Relaxed);
        }

        if let ClientResponse::Error { code, .. } = response {
            self.client_error_total.fetch_add(1, Ordering::Relaxed);
            match source {
                ClientRequestSource::Tcp => {
                    self.client_errors_tcp_total.fetch_add(1, Ordering::Relaxed);
                }
                ClientRequestSource::Http => {
                    self.client_errors_http_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                ClientRequestSource::Internal => {
                    self.client_errors_internal_total
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            match code {
                ErrorCode::AuthFailed => {
                    self.client_error_auth_total.fetch_add(1, Ordering::Relaxed);
                }
                ErrorCode::RateLimited
                | ErrorCode::CircuitOpen
                | ErrorCode::NamespaceQuotaExceeded => {
                    self.client_error_throttle_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                ErrorCode::NodeInactive | ErrorCode::NoQuorum | ErrorCode::WriteTimeout => {
                    self.client_error_availability_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                ErrorCode::KeyNotFound | ErrorCode::ValueTooLarge | ErrorCode::KeyLimitReached => {
                    self.client_error_validation_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                ErrorCode::InternalError => {
                    self.client_error_internal_total
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    fn execute_get(&self, lookup_key: String, response_key: String) -> ClientResponse {
        match self.store.get(&lookup_key) {
            Some(entry) => ClientResponse::Value {
                key: response_key,
                value: entry.value,
                version: entry.version,
            },
            None => ClientResponse::NotFound,
        }
    }

    async fn handle_get_with_single_flight(
        &self,
        lookup_key: String,
        response_key: String,
    ) -> ClientResponse {
        let (enabled, max_waiters) = {
            let cfg = self.config.lock().unwrap();
            (cfg.hot_key.enabled, cfg.hot_key.max_waiters.max(1))
        };
        if !enabled {
            return self.execute_get(lookup_key, response_key);
        }

        enum JoinDecision {
            Leader(Arc<GetFlight>),
            Follower {
                flight: Arc<GetFlight>,
                rx: watch::Receiver<Option<ClientResponse>>,
            },
            Fallback,
        }

        let decision = {
            let mut flights = self.get_flights.lock().await;
            if let Some(existing) = flights.get(&lookup_key) {
                let current = existing.waiters.load(Ordering::Relaxed);
                if current >= max_waiters {
                    JoinDecision::Fallback
                } else {
                    existing.waiters.fetch_add(1, Ordering::Relaxed);
                    let rx = existing.tx.subscribe();
                    JoinDecision::Follower {
                        flight: Arc::clone(existing),
                        rx,
                    }
                }
            } else {
                let created = Arc::new(GetFlight::new());
                flights.insert(lookup_key.clone(), Arc::clone(&created));
                JoinDecision::Leader(created)
            }
        };

        match decision {
            JoinDecision::Fallback => {
                self.hot_key_fallback_exec_total
                    .fetch_add(1, Ordering::Relaxed);
                self.execute_get(lookup_key, response_key)
            }
            JoinDecision::Leader(flight) => {
                let response = self.execute_get(lookup_key.clone(), response_key.clone());
                let _ = flight.tx.send(Some(response.clone()));
                let mut flights = self.get_flights.lock().await;
                if flights
                    .get(&lookup_key)
                    .map(|f| Arc::ptr_eq(f, &flight))
                    .unwrap_or(false)
                {
                    flights.remove(&lookup_key);
                }
                response
            }
            JoinDecision::Follower { flight, mut rx } => {
                self.hot_key_coalesced_hits_total
                    .fetch_add(1, Ordering::Relaxed);
                let response = if rx.borrow().is_some() || rx.changed().await.is_ok() {
                    rx.borrow().clone()
                } else {
                    None
                };
                flight.waiters.fetch_sub(1, Ordering::Relaxed);
                match response {
                    Some(resp) => resp,
                    None => {
                        self.hot_key_fallback_exec_total
                            .fetch_add(1, Ordering::Relaxed);
                        self.execute_get(lookup_key, response_key)
                    }
                }
            }
        }
    }

    async fn maybe_read_repair_on_miss(
        self: &Arc<Self>,
        namespace: Option<String>,
        key: String,
    ) -> Option<ClientResponse> {
        let (enabled, min_interval_ms) = {
            let cfg = self.config.lock().unwrap();
            (
                cfg.replication.read_repair_on_miss_enabled,
                cfg.replication.read_repair_min_interval_ms.max(1),
            )
        };
        if !enabled {
            return None;
        }

        let (status, am_primary) = {
            let set = self.active_set.lock().await;
            (set.local_status(), set.is_primary())
        };
        if status != NodeStatus::Active || am_primary {
            return None;
        }

        let now_ms = Self::now_millis();
        let last_ms = self.last_read_repair_trigger_ms.load(Ordering::Relaxed);
        if now_ms < last_ms.saturating_add(min_interval_ms) {
            self.read_repair_throttled_total
                .fetch_add(1, Ordering::Relaxed);
            return None;
        }
        self.last_read_repair_trigger_ms
            .store(now_ms, Ordering::Relaxed);
        self.read_repair_trigger_total
            .fetch_add(1, Ordering::Relaxed);

        match self
            .forward_request_to_primary(ClientRequest::Get {
                key: key.clone(),
                namespace,
            })
            .await
        {
            v @ ClientResponse::Value { .. } => {
                self.read_repair_success_total
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    "Read-repair: key '{}' found on primary but missing locally; scheduling resync",
                    sanitize_for_log(&key),
                );
                tokio::spawn(Arc::clone(self).run_resync());
                Some(v)
            }
            ClientResponse::NotFound => Some(ClientResponse::NotFound),
            other => {
                tracing::warn!(
                    "Read-repair: primary query for '{}' returned non-repairable response: {:?}",
                    sanitize_for_log(&key),
                    other
                );
                None
            }
        }
    }

    /// Dispatch a client request to the appropriate handler.
    ///
    /// Returns `NodeInactive` immediately when the node is in maintenance mode.
    /// Reads are served locally; writes are coordinated or forwarded to the primary.
    pub async fn handle_client(self: &Arc<Self>, req: ClientRequest) -> ClientResponse {
        self.handle_client_with_source(req, ClientRequestSource::Internal)
            .await
    }

    pub async fn handle_client_tcp(self: &Arc<Self>, req: ClientRequest) -> ClientResponse {
        self.handle_client_with_source(req, ClientRequestSource::Tcp)
            .await
    }

    pub async fn handle_client_http(self: &Arc<Self>, req: ClientRequest) -> ClientResponse {
        self.handle_client_with_source(req, ClientRequestSource::Http)
            .await
    }

    async fn handle_client_with_source(
        self: &Arc<Self>,
        req: ClientRequest,
        source: ClientRequestSource,
    ) -> ClientResponse {
        let started = Instant::now();
        let mut should_record_circuit_result = false;
        let response = if !self.active.load(Ordering::Relaxed) {
            ClientResponse::Error {
                code: ErrorCode::NodeInactive,
                message: "Node is inactive (maintenance mode)".into(),
            }
        } else if !self.allow_by_rate_limit() {
            ClientResponse::Error {
                code: ErrorCode::RateLimited,
                message: "Request throttled by rate limiter".into(),
            }
        } else if !self.allow_by_circuit_breaker() {
            ClientResponse::Error {
                code: ErrorCode::CircuitOpen,
                message: "Request rejected by circuit breaker".into(),
            }
        } else {
            should_record_circuit_result = true;
            match req {
                ClientRequest::Ping => ClientResponse::Pong,
                ClientRequest::Auth { .. } => ClientResponse::Error {
                    code: ErrorCode::InternalError,
                    message: "Authentication is already complete or invalid in this context".into(),
                },

                ClientRequest::Get { key, namespace } => match self.namespace_context(namespace) {
                    Ok(ns) => {
                        let namespaced_key = self.namespaced_key(&ns, &key);
                        let local = self
                            .handle_get_with_single_flight(namespaced_key, key.clone())
                            .await;
                        if matches!(local, ClientResponse::NotFound) {
                            self.maybe_read_repair_on_miss(ns, key)
                                .await
                                .unwrap_or(local)
                        } else {
                            local
                        }
                    }
                    Err(err) => err,
                },

                ClientRequest::Set {
                    key,
                    value,
                    ttl_secs,
                    namespace,
                } => match self.namespace_context(namespace) {
                    Ok(ns) => {
                        let namespaced_key = self.namespaced_key(&ns, &key);
                        if let Some(ref namespace_name) = ns {
                            let max = self.config.lock().unwrap().tenancy.max_keys_per_namespace;
                            if max > 0
                                && self.store.get(&namespaced_key).is_none()
                                && self.namespace_key_count(namespace_name) >= max
                            {
                                self.namespace_quota_reject_total
                                    .fetch_add(1, Ordering::Relaxed);
                                ClientResponse::Error {
                                    code: ErrorCode::NamespaceQuotaExceeded,
                                    message: format!(
                                        "Namespace '{}' reached key limit ({})",
                                        namespace_name, max
                                    ),
                                }
                            } else {
                                match self.store.check_limits(&namespaced_key, &value) {
                                    Err(LimitError::ValueTooLarge) => ClientResponse::Error {
                                        code: ErrorCode::ValueTooLarge,
                                        message: format!(
                                            "Value size {} bytes exceeds the configured limit",
                                            value.len()
                                        ),
                                    },
                                    Err(LimitError::KeyLimitReached) => ClientResponse::Error {
                                        code: ErrorCode::KeyLimitReached,
                                        message: "Cache is at the maximum key count limit".into(),
                                    },
                                    Ok(()) => {
                                        self.coordinate_write(namespaced_key, Some(value), ttl_secs)
                                            .await
                                    }
                                }
                            }
                        } else {
                            match self.store.check_limits(&namespaced_key, &value) {
                                Err(LimitError::ValueTooLarge) => ClientResponse::Error {
                                    code: ErrorCode::ValueTooLarge,
                                    message: format!(
                                        "Value size {} bytes exceeds the configured limit",
                                        value.len()
                                    ),
                                },
                                Err(LimitError::KeyLimitReached) => ClientResponse::Error {
                                    code: ErrorCode::KeyLimitReached,
                                    message: "Cache is at the maximum key count limit".into(),
                                },
                                Ok(()) => {
                                    self.coordinate_write(namespaced_key, Some(value), ttl_secs)
                                        .await
                                }
                            }
                        }
                    }
                    Err(err) => err,
                },

                ClientRequest::Delete { key, namespace } => {
                    match self.namespace_context(namespace) {
                        Ok(ns) => {
                            self.coordinate_write(self.namespaced_key(&ns, &key), None, None)
                                .await
                        }
                        Err(err) => err,
                    }
                }

                ClientRequest::DeleteByPattern { pattern, namespace } => {
                    match self.namespace_context(namespace) {
                        Ok(ns) => {
                            self.delete_by_pattern(self.namespaced_pattern(&ns, &pattern))
                                .await
                        }
                        Err(err) => err,
                    }
                }

                ClientRequest::SetTtlByPattern {
                    pattern,
                    ttl_secs,
                    namespace,
                } => match self.namespace_context(namespace) {
                    Ok(ns) => {
                        self.set_ttl_by_pattern(self.namespaced_pattern(&ns, &pattern), ttl_secs)
                            .await
                    }
                    Err(err) => err,
                },

                // Watch/Unwatch are handled at the TCP connection level (tcp_server.rs)
                // and should never reach handle_client. Guard against misuse.
                ClientRequest::Watch { .. } | ClientRequest::Unwatch { .. } => {
                    ClientResponse::Error {
                        code: ErrorCode::InternalError,
                        message: "Watch/Unwatch must be handled at the connection level".into(),
                    }
                }
            }
        };
        if should_record_circuit_result {
            self.record_circuit_result(&response);
        }
        self.observe_client_response_metrics(source, started.elapsed(), &response);
        response
    }

    async fn delete_by_pattern(&self, pattern: String) -> ClientResponse {
        let keys = self.store.keys(Some(&pattern));
        let mut deleted = 0usize;
        for key in keys {
            let resp = self.coordinate_write(key, None, None).await;
            match resp {
                ClientResponse::Deleted => deleted += 1,
                ClientResponse::NotFound => {}
                ClientResponse::Error { .. } => return resp,
                _ => {
                    return ClientResponse::Error {
                        code: ErrorCode::InternalError,
                        message: "Unexpected response while deleting by pattern".into(),
                    };
                }
            }
        }
        ClientResponse::PatternDeleted { deleted }
    }

    async fn set_ttl_by_pattern(&self, pattern: String, ttl_secs: Option<u64>) -> ClientResponse {
        let keys = self.store.keys(Some(&pattern));
        let mut updated = 0usize;
        for key in keys {
            let Some(entry) = self.store.get(&key) else {
                continue;
            };
            let resp = self
                .coordinate_write(key, Some(entry.value), ttl_secs)
                .await;
            match resp {
                ClientResponse::Ok { .. } => updated += 1,
                ClientResponse::NotFound => {}
                ClientResponse::Error { .. } => return resp,
                _ => {
                    return ClientResponse::Error {
                        code: ErrorCode::InternalError,
                        message: "Unexpected response while updating ttl by pattern".into(),
                    };
                }
            }
        }
        ClientResponse::PatternTtlUpdated { updated }
    }

    // -----------------------------------------------------------------------
    // Write coordination (Active Set two-phase)
    // -----------------------------------------------------------------------

    async fn coordinate_write(
        &self,
        key: String,
        value: Option<Bytes>,
        ttl_secs: Option<u64>,
    ) -> ClientResponse {
        if !self.active_set.lock().await.is_primary() {
            let resp = self
                .forward_to_primary(key.clone(), value.clone(), ttl_secs)
                .await;
            // forward_to_primary marks the old primary as Inactive when it refuses.
            // Re-check whether we became primary; if so, fall through to the write path.
            if !matches!(
                &resp,
                ClientResponse::Error {
                    code: ErrorCode::NodeInactive,
                    ..
                }
            ) || !self.active_set.lock().await.is_primary()
            {
                return resp;
            }
            // We are now the primary after the stale primary stepped down; fall through.
            info!("Took over as primary after stale primary refused write; retrying write locally");
        }

        // --- PREPARE phase ---
        let log_index = {
            let mut log = self.write_log.lock().await;
            log.append(key.clone(), value.clone(), ttl_secs)
        };

        let peers: Vec<SocketAddr> = {
            let set = self.active_set.lock().await;
            set.active_peers()
                .into_iter()
                .map(|n| SocketAddr::new(n.addr.ip(), n.cluster_port))
                .collect()
        };

        let prepare_msg = ClusterMessage::Prepare {
            log_index,
            key: key.clone(),
            value: value.clone(),
            ttl_secs,
        };

        let timeout =
            Duration::from_millis(self.config.lock().unwrap().replication.write_timeout_ms);
        let all_acked = self.broadcast_and_wait(&peers, &prepare_msg, timeout).await;

        if !all_acked {
            warn!("PREPARE timed out for log_index={}", log_index);
            return ClientResponse::Error {
                code: ErrorCode::WriteTimeout,
                message: format!("Write timed out at PREPARE (index {})", log_index),
            };
        }

        // Apply locally.
        let version = self.apply_locally(&key, value.clone(), ttl_secs, log_index);

        // --- COMMIT phase ---
        let commit_msg = ClusterMessage::Commit { log_index };
        let _ = self.broadcast_and_wait(&peers, &commit_msg, timeout).await;

        self.write_log.lock().await.commit(log_index);
        self.active_set.lock().await.set_local_applied(log_index);

        match value {
            Some(_) => ClientResponse::Ok { version },
            None => ClientResponse::Deleted,
        }
    }

    async fn forward_request_to_primary(&self, request: ClientRequest) -> ClientResponse {
        let (primary_id, primary_addr) = {
            let set = self.active_set.lock().await;
            let primary_id = match set.primary_id() {
                Some(id) => id,
                None => {
                    return ClientResponse::Error {
                        code: ErrorCode::NoQuorum,
                        message: "No primary elected".into(),
                    }
                }
            };
            let addr = set
                .all_nodes()
                .into_iter()
                .find(|n| n.id == primary_id)
                .map(|n| SocketAddr::new(n.addr.ip(), n.cluster_port));
            (primary_id, addr)
        };

        let addr = match primary_addr {
            Some(a) => a,
            None => {
                return ClientResponse::Error {
                    code: ErrorCode::NoQuorum,
                    message: "Primary address unknown".into(),
                }
            }
        };

        let forward = ClusterMessage::Forward {
            request,
            origin_node: self.id,
        };

        let resp = match send_cluster(addr, &forward, self.tls_connector.as_ref()).await {
            Ok(Some(ClusterMessage::ForwardResponse(r))) => r,
            Err(e) => ClientResponse::Error {
                code: ErrorCode::InternalError,
                message: e.to_string(),
            },
            _ => ClientResponse::Error {
                code: ErrorCode::InternalError,
                message: "No ForwardResponse from primary".into(),
            },
        };

        // If the supposed primary is actually inactive, update our local view immediately
        // so the next primary election will choose a different node without waiting for gossip.
        if matches!(
            &resp,
            ClientResponse::Error {
                code: ErrorCode::NodeInactive,
                ..
            }
        ) {
            let mut set = self.active_set.lock().await;
            if let Some(mut info) = set.snapshot().into_iter().find(|n| n.id == primary_id) {
                info.status = NodeStatus::Inactive;
                set.upsert(info);
                info!(
                    "Primary {} reported NodeInactive; updated local view, triggering re-election",
                    primary_id
                );
            }
        }

        resp
    }

    async fn forward_to_primary(
        &self,
        key: String,
        value: Option<Bytes>,
        ttl_secs: Option<u64>,
    ) -> ClientResponse {
        let req = if let Some(v) = value {
            ClientRequest::Set {
                key,
                value: v,
                ttl_secs,
                namespace: None,
            }
        } else {
            ClientRequest::Delete {
                key,
                namespace: None,
            }
        };
        self.forward_request_to_primary(req).await
    }

    fn apply_locally(
        &self,
        key: &str,
        value: Option<Bytes>,
        ttl_secs: Option<u64>,
        log_index: u64,
    ) -> u64 {
        let version = match value {
            Some(v) => {
                // Enforce size/key-count limits on the cluster replication path as well.
                // A compromised primary could otherwise send an arbitrarily large value
                // that bypasses the check_limits() guard that protects the client path.
                if let Err(e) = self.store.check_limits(key, &v) {
                    tracing::warn!(
                        key = sanitize_for_log(key).as_str(),
                        value_bytes = v.len(),
                        log_index,
                        error = ?e,
                        "apply_locally: skipping entry that violates store limits \
                         received via cluster replication"
                    );
                    return log_index;
                }
                let stored_value = v.clone();
                let ver = self.store.set(key.to_string(), v, log_index, ttl_secs);
                // DITTO-02: notify watchers (fire-and-forget; ignore if no receivers).
                let _ = self
                    .watch_tx
                    .send((key.to_string(), Some(stored_value), ver));
                ver
            }
            None => {
                self.store.delete(key);
                // DITTO-02: notify watchers that the key was deleted (value = None).
                let _ = self.watch_tx.send((key.to_string(), None, log_index));
                log_index
            }
        };
        version
    }

    async fn broadcast_and_wait(
        &self,
        peers: &[SocketAddr],
        msg: &ClusterMessage,
        timeout: Duration,
    ) -> bool {
        if peers.is_empty() {
            return true;
        }

        let handles: Vec<_> = peers
            .iter()
            .map(|&addr| {
                let msg = msg.clone();
                let tls = self.tls_connector.clone();
                tokio::spawn(async move {
                    tokio::time::timeout(timeout, send_cluster(addr, &msg, tls.as_ref())).await
                })
            })
            .collect();

        let mut all_ok = true;
        for h in handles {
            if !matches!(h.await, Ok(Ok(Ok(_)))) {
                all_ok = false;
            }
        }
        all_ok
    }

    // -----------------------------------------------------------------------
    // Cluster message handler
    // -----------------------------------------------------------------------

    /// Handle an inbound cluster-protocol message.
    ///
    /// Returns `Some(response)` for messages that require a reply
    /// (e.g. `Prepare` → `PrepareAck`), or `None` for fire-and-forget messages.
    pub async fn handle_cluster(self: Arc<Self>, msg: ClusterMessage) -> Option<ClusterMessage> {
        match msg {
            ClusterMessage::Prepare {
                log_index,
                key,
                value,
                ttl_secs,
            } => {
                self.write_log
                    .lock()
                    .await
                    .append_at(log_index, key, value, ttl_secs);
                Some(ClusterMessage::PrepareAck {
                    log_index,
                    node_id: self.id,
                })
            }

            ClusterMessage::Commit { log_index } => {
                let entry_data = self.write_log.lock().await.get_entry(log_index);

                if let Some((key, value, ttl)) = entry_data {
                    self.apply_locally(&key, value, ttl, log_index);
                    self.write_log.lock().await.commit(log_index);
                }
                self.active_set.lock().await.set_local_applied(log_index);
                Some(ClusterMessage::CommitAck {
                    log_index,
                    node_id: self.id,
                })
            }

            ClusterMessage::Forward { request, .. } => {
                let response = self
                    .handle_client_with_source(request, ClientRequestSource::Internal)
                    .await;
                Some(ClusterMessage::ForwardResponse(response))
            }

            ClusterMessage::RequestLog { from_index } => {
                let entries = self.write_log.lock().await.entries_since(from_index);
                Some(ClusterMessage::LogEntries { entries })
            }

            ClusterMessage::LogEntries { entries } => {
                for entry in entries {
                    self.apply_locally(
                        &entry.key,
                        entry.value.clone(),
                        entry.ttl_secs,
                        entry.index,
                    );
                    self.write_log.lock().await.commit(entry.index);
                    self.active_set.lock().await.set_local_applied(entry.index);
                }
                None
            }

            ClusterMessage::Synced {
                node_id,
                last_applied,
            } => {
                let mut set = self.active_set.lock().await;
                if let Some(mut info) = set.snapshot().into_iter().find(|n| n.id == node_id) {
                    info.status = NodeStatus::Active;
                    info.last_applied = last_applied;
                    set.upsert(info);
                }
                None
            }

            ClusterMessage::ForcePrimary { node_id } => {
                self.active_set.lock().await.set_pinned_primary(node_id);
                tracing::info!("Primary pinned to {} by ForcePrimary broadcast", node_id);
                None
            }

            ClusterMessage::Admin(admin_req) => {
                let response = Arc::clone(&self).handle_admin(admin_req).await;
                // Wrap response back into AdminResponse envelope.
                Some(ClusterMessage::AdminResponse(response))
            }

            _ => None,
        }
    }

    // -----------------------------------------------------------------------
    // Admin handler
    // -----------------------------------------------------------------------

    fn persistence_states(cfg: &Config) -> (bool, bool, bool, bool, bool, bool) {
        let platform = cfg.persistence.platform_allowed;
        let runtime = cfg.persistence.runtime_enabled;
        let enabled = platform && runtime;
        let backup_enabled = enabled && cfg.persistence.backup_allowed;
        let export_enabled = enabled && cfg.persistence.export_allowed;
        let import_enabled = enabled && cfg.persistence.import_allowed;
        (
            platform,
            runtime,
            enabled,
            backup_enabled,
            export_enabled,
            import_enabled,
        )
    }

    // Build the full list of node properties as key-value pairs.
    async fn all_properties(&self) -> Vec<(String, String)> {
        let stats = self.stats().await;
        let set = self.active_set.lock().await;
        let cfg = self.config.lock().unwrap();
        vec![
            // --- read-only ---
            ("id".into(), cfg.node.id.clone()),
            ("protocol-version".into(), PROTOCOL_VERSION.to_string()),
            ("committed-index".into(), stats.committed_index.to_string()),
            ("uptime".into(), format!("{}s", stats.uptime_secs)),
            // --- read-write ---
            ("status".into(), format!("{:?}", set.local_status())),
            ("primary".into(), set.is_primary().to_string()),
            ("bind-addr".into(), cfg.node.bind_addr.clone()),
            (
                "cluster-bind-addr".into(),
                cfg.node.cluster_bind_addr.clone(),
            ),
            ("client-port".into(), cfg.node.client_port.to_string()),
            ("http-port".into(), cfg.node.http_port.to_string()),
            ("cluster-port".into(), cfg.node.cluster_port.to_string()),
            ("gossip-port".into(), cfg.node.gossip_port.to_string()),
            (
                "frame-read-timeout-ms".into(),
                cfg.node.frame_read_timeout_ms.to_string(),
            ),
            (
                "max-memory".into(),
                format!("{}mb", cfg.cache.max_memory_mb),
            ),
            (
                "default-ttl".into(),
                format!("{}s", cfg.cache.default_ttl_secs),
            ),
            (
                "value-size-limit".into(),
                if stats.value_size_limit_bytes == 0 {
                    "unlimited".into()
                } else {
                    format!("{}b", stats.value_size_limit_bytes)
                },
            ),
            (
                "max-keys".into(),
                if stats.max_keys_limit == 0 {
                    "unlimited".into()
                } else {
                    stats.max_keys_limit.to_string()
                },
            ),
            (
                "compression-enabled".into(),
                stats.compression_enabled.to_string(),
            ),
            (
                "compression-threshold".into(),
                format!("{}b", stats.compression_threshold_bytes),
            ),
            (
                "write-timeout-ms".into(),
                cfg.replication.write_timeout_ms.to_string(),
            ),
            (
                "gossip-interval-ms".into(),
                cfg.replication.gossip_interval_ms.to_string(),
            ),
            (
                "gossip-dead-ms".into(),
                cfg.replication.gossip_dead_ms.to_string(),
            ),
            (
                "version-check-interval".into(),
                format!("{}ms", cfg.replication.version_check_interval_ms),
            ),
            (
                "anti-entropy-enabled".into(),
                cfg.replication.anti_entropy_enabled.to_string(),
            ),
            (
                "anti-entropy-interval-ms".into(),
                cfg.replication.anti_entropy_interval_ms.to_string(),
            ),
            (
                "anti-entropy-lag-threshold".into(),
                cfg.replication.anti_entropy_lag_threshold.to_string(),
            ),
            (
                "anti-entropy-key-sample-size".into(),
                cfg.replication.anti_entropy_key_sample_size.to_string(),
            ),
            (
                "anti-entropy-full-reconcile-every".into(),
                cfg.replication
                    .anti_entropy_full_reconcile_every
                    .to_string(),
            ),
            (
                "anti-entropy-full-reconcile-max-keys".into(),
                cfg.replication
                    .anti_entropy_full_reconcile_max_keys
                    .to_string(),
            ),
            (
                "anti-entropy-runs-total".into(),
                stats.anti_entropy_runs_total.to_string(),
            ),
            (
                "anti-entropy-repair-trigger-total".into(),
                stats.anti_entropy_repair_trigger_total.to_string(),
            ),
            (
                "anti-entropy-last-detected-lag".into(),
                stats.anti_entropy_last_detected_lag.to_string(),
            ),
            (
                "anti-entropy-key-checks-total".into(),
                stats.anti_entropy_key_checks_total.to_string(),
            ),
            (
                "anti-entropy-key-mismatch-total".into(),
                stats.anti_entropy_key_mismatch_total.to_string(),
            ),
            (
                "anti-entropy-full-reconcile-runs-total".into(),
                stats.anti_entropy_full_reconcile_runs_total.to_string(),
            ),
            (
                "anti-entropy-full-reconcile-key-checks-total".into(),
                stats
                    .anti_entropy_full_reconcile_key_checks_total
                    .to_string(),
            ),
            (
                "anti-entropy-full-reconcile-mismatch-total".into(),
                stats.anti_entropy_full_reconcile_mismatch_total.to_string(),
            ),
            (
                "mixed-version-probe-enabled".into(),
                cfg.replication.mixed_version_probe_enabled.to_string(),
            ),
            (
                "mixed-version-probe-interval-ms".into(),
                cfg.replication.mixed_version_probe_interval_ms.to_string(),
            ),
            (
                "mixed-version-probe-runs-total".into(),
                stats.mixed_version_probe_runs_total.to_string(),
            ),
            (
                "mixed-version-peers-detected-total".into(),
                stats.mixed_version_peers_detected_total.to_string(),
            ),
            (
                "mixed-version-probe-errors-total".into(),
                stats.mixed_version_probe_errors_total.to_string(),
            ),
            (
                "mixed-version-last-detected-peer-count".into(),
                stats.mixed_version_last_detected_peer_count.to_string(),
            ),
            (
                "read-repair-on-miss-enabled".into(),
                cfg.replication.read_repair_on_miss_enabled.to_string(),
            ),
            (
                "read-repair-min-interval-ms".into(),
                cfg.replication.read_repair_min_interval_ms.to_string(),
            ),
            (
                "read-repair-trigger-total".into(),
                stats.read_repair_trigger_total.to_string(),
            ),
            (
                "read-repair-success-total".into(),
                stats.read_repair_success_total.to_string(),
            ),
            (
                "read-repair-throttled-total".into(),
                stats.read_repair_throttled_total.to_string(),
            ),
            (
                "namespace-quota-reject-total".into(),
                stats.namespace_quota_reject_total.to_string(),
            ),
            (
                "namespace-quota-reject-rate-per-min".into(),
                stats.namespace_quota_reject_rate_per_min.to_string(),
            ),
            (
                "namespace-quota-reject-trend".into(),
                stats.namespace_quota_reject_trend.clone(),
            ),
            (
                "namespace-quota-top-usage".into(),
                Self::format_namespace_quota_top_usage(&stats.namespace_quota_top_usage),
            ),
            (
                "persistence-platform-allowed".into(),
                stats.persistence_platform_allowed.to_string(),
            ),
            (
                "persistence-runtime-enabled".into(),
                stats.persistence_runtime_enabled.to_string(),
            ),
            (
                "persistence-enabled".into(),
                stats.persistence_enabled.to_string(),
            ),
            ("tenancy-enabled".into(), stats.tenancy_enabled.to_string()),
            (
                "tenancy-default-namespace".into(),
                stats.tenancy_default_namespace.clone(),
            ),
            (
                "tenancy-max-keys-per-namespace".into(),
                stats.tenancy_max_keys_per_namespace.to_string(),
            ),
            (
                "persistence-backup-platform-allowed".into(),
                cfg.persistence.backup_allowed.to_string(),
            ),
            (
                "persistence-export-platform-allowed".into(),
                cfg.persistence.export_allowed.to_string(),
            ),
            (
                "persistence-import-platform-allowed".into(),
                cfg.persistence.import_allowed.to_string(),
            ),
            (
                "persistence-backup-enabled".into(),
                stats.persistence_backup_enabled.to_string(),
            ),
            (
                "persistence-export-enabled".into(),
                stats.persistence_export_enabled.to_string(),
            ),
            (
                "persistence-import-enabled".into(),
                stats.persistence_import_enabled.to_string(),
            ),
            (
                "snapshot-restore-on-start".into(),
                cfg.backup.restore_on_start.to_string(),
            ),
            (
                "snapshot-last-load-path".into(),
                stats.snapshot_last_load_path.clone().unwrap_or_default(),
            ),
            (
                "snapshot-last-load-duration-ms".into(),
                stats.snapshot_last_load_duration_ms.to_string(),
            ),
            (
                "snapshot-last-load-entries".into(),
                stats.snapshot_last_load_entries.to_string(),
            ),
            (
                "snapshot-last-load-age-secs".into(),
                stats
                    .snapshot_last_load_age_secs
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "0".to_string()),
            ),
            (
                "snapshot-restore-attempt-total".into(),
                stats.snapshot_restore_attempt_total.to_string(),
            ),
            (
                "snapshot-restore-success-total".into(),
                stats.snapshot_restore_success_total.to_string(),
            ),
            (
                "snapshot-restore-failure-total".into(),
                stats.snapshot_restore_failure_total.to_string(),
            ),
            (
                "snapshot-restore-not-found-total".into(),
                stats.snapshot_restore_not_found_total.to_string(),
            ),
            (
                "snapshot-restore-policy-block-total".into(),
                stats.snapshot_restore_policy_block_total.to_string(),
            ),
            (
                "rate-limit-enabled".into(),
                stats.rate_limit_enabled.to_string(),
            ),
            (
                "rate-limit-requests-per-sec".into(),
                cfg.rate_limit.requests_per_sec.to_string(),
            ),
            ("rate-limit-burst".into(), cfg.rate_limit.burst.to_string()),
            (
                "rate-limited-requests-total".into(),
                stats.rate_limited_requests_total.to_string(),
            ),
            ("hot-key-enabled".into(), stats.hot_key_enabled.to_string()),
            (
                "hot-key-max-waiters".into(),
                cfg.hot_key.max_waiters.to_string(),
            ),
            (
                "hot-key-coalesced-hits-total".into(),
                stats.hot_key_coalesced_hits_total.to_string(),
            ),
            (
                "hot-key-fallback-exec-total".into(),
                stats.hot_key_fallback_exec_total.to_string(),
            ),
            (
                "hot-key-inflight-keys".into(),
                stats.hot_key_inflight_keys.to_string(),
            ),
            (
                "circuit-breaker-enabled".into(),
                stats.circuit_breaker_enabled.to_string(),
            ),
            (
                "circuit-breaker-failure-threshold".into(),
                cfg.circuit_breaker.failure_threshold.to_string(),
            ),
            (
                "circuit-breaker-open-ms".into(),
                cfg.circuit_breaker.open_ms.to_string(),
            ),
            (
                "circuit-breaker-half-open-max-requests".into(),
                cfg.circuit_breaker.half_open_max_requests.to_string(),
            ),
            (
                "circuit-breaker-state".into(),
                stats.circuit_breaker_state.clone(),
            ),
            (
                "circuit-breaker-open-total".into(),
                stats.circuit_breaker_open_total.to_string(),
            ),
            (
                "circuit-breaker-reject-total".into(),
                stats.circuit_breaker_reject_total.to_string(),
            ),
            (
                "client-requests-total".into(),
                stats.client_requests_total.to_string(),
            ),
            (
                "client-requests-tcp-total".into(),
                stats.client_requests_tcp_total.to_string(),
            ),
            (
                "client-requests-http-total".into(),
                stats.client_requests_http_total.to_string(),
            ),
            (
                "client-requests-internal-total".into(),
                stats.client_requests_internal_total.to_string(),
            ),
            (
                "client-request-latency-buckets".into(),
                Self::format_request_latency_buckets(&stats),
            ),
            (
                "client-latency-p50-estimate-ms".into(),
                stats
                    .client_latency_p50_estimate_ms
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "n/a".to_string()),
            ),
            (
                "client-latency-p90-estimate-ms".into(),
                stats
                    .client_latency_p90_estimate_ms
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "n/a".to_string()),
            ),
            (
                "client-latency-p95-estimate-ms".into(),
                stats
                    .client_latency_p95_estimate_ms
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "n/a".to_string()),
            ),
            (
                "client-latency-p99-estimate-ms".into(),
                stats
                    .client_latency_p99_estimate_ms
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "n/a".to_string()),
            ),
            (
                "client-error-total".into(),
                stats.client_error_total.to_string(),
            ),
            (
                "client-errors-tcp-total".into(),
                stats.client_errors_tcp_total.to_string(),
            ),
            (
                "client-errors-http-total".into(),
                stats.client_errors_http_total.to_string(),
            ),
            (
                "client-errors-internal-total".into(),
                stats.client_errors_internal_total.to_string(),
            ),
            (
                "client-error-auth-total".into(),
                stats.client_error_auth_total.to_string(),
            ),
            (
                "client-error-throttle-total".into(),
                stats.client_error_throttle_total.to_string(),
            ),
            (
                "client-error-availability-total".into(),
                stats.client_error_availability_total.to_string(),
            ),
            (
                "client-error-validation-total".into(),
                stats.client_error_validation_total.to_string(),
            ),
            (
                "client-error-internal-total".into(),
                stats.client_error_internal_total.to_string(),
            ),
            (
                "client-error-other-total".into(),
                stats.client_error_other_total.to_string(),
            ),
        ]
    }

    async fn handle_admin(self: Arc<Self>, req: AdminRequest) -> AdminResponse {
        match req {
            AdminRequest::Describe => AdminResponse::Properties(self.all_properties().await),

            AdminRequest::GetStats => AdminResponse::Stats(self.stats().await),

            AdminRequest::ListKeys { pattern } => {
                AdminResponse::Keys(self.store.keys(pattern.as_deref()))
            }

            AdminRequest::GetKeyInfo { key } => {
                // Read the raw compressed flag before get() transparently decompresses.
                let compressed = self.store.is_compressed(&key).unwrap_or(false);
                match self.store.get(&key) {
                    Some(entry) => {
                        let ttl = entry.ttl_remaining_secs();
                        let freq = entry.freq_count;
                        let ver = entry.version;
                        AdminResponse::KeyInfo {
                            key,
                            value: entry.value,
                            version: ver,
                            ttl_remaining_secs: ttl,
                            freq_count: freq,
                            compressed,
                        }
                    }
                    None => AdminResponse::NotFound,
                }
            }

            AdminRequest::SetKeyProperty { key, name, value } => {
                match name.as_str() {
                    "compressed" => {
                        let compress = value.trim().eq_ignore_ascii_case("true");
                        match self.store.set_key_compressed(&key, compress) {
                            Ok(()) => {
                                // Use structured fields so that a user-controlled key
                                // cannot inject newlines into the log stream.
                                tracing::info!(
                                    key = sanitize_for_log(&key).as_str(),
                                    compressed = compress,
                                    "SetKeyProperty updated"
                                );
                                AdminResponse::KeyPropertyUpdated
                            }
                            Err(msg) if msg == "key not found" => AdminResponse::NotFound,
                            Err(msg) => AdminResponse::Error {
                                message: msg.to_string(),
                            },
                        }
                    }
                    other => AdminResponse::Error {
                        message: format!("Unknown key property '{}'", other),
                    },
                }
            }

            AdminRequest::FlushCache => {
                let status = self.active_set.lock().await.local_status();
                if matches!(status, NodeStatus::Active | NodeStatus::Syncing) {
                    AdminResponse::Error {
                        message: "node must be Inactive before flush; use set-active false first"
                            .into(),
                    }
                } else {
                    self.store.flush();
                    self.write_log.lock().await.reset();
                    AdminResponse::Flushed
                }
            }

            AdminRequest::ClusterStatus => {
                let nodes = self.active_set.lock().await.snapshot();
                AdminResponse::ClusterView(nodes)
            }

            AdminRequest::SetKeysTtl { pattern, ttl_secs } => {
                let updated = self.store.set_ttl_by_pattern(&pattern, ttl_secs);
                // Use structured fields so that a user-controlled pattern value
                // cannot inject newlines or control characters into the log stream.
                tracing::info!(
                    pattern = sanitize_for_log(&pattern).as_str(),
                    ttl_secs = ?ttl_secs,
                    updated,
                    "SetKeysTtl updated"
                );
                AdminResponse::TtlUpdated { updated }
            }

            AdminRequest::BackupNow => {
                let (backup_enabled, cfg) = {
                    let cfg_guard = self.config.lock().unwrap();
                    let (_, _, _, backup_enabled, _, _) = Self::persistence_states(&cfg_guard);
                    (backup_enabled, cfg_guard.backup.clone())
                };
                if !backup_enabled {
                    return AdminResponse::Error {
                        message: "Backup is disabled by persistence policy. Require DITTO_PERSISTENCE_PLATFORM_ALLOWED=true, DITTO_PERSISTENCE_BACKUP_ALLOWED=true and runtime property persistence-runtime-enabled=true.".into(),
                    };
                }
                match crate::backup::run_backup(Arc::clone(&self), &cfg).await {
                    Ok(path) => {
                        AdminResponse::BackupResult {
                            path,
                            bytes: 0, // actual size logged server-side
                        }
                    }
                    Err(e) => AdminResponse::Error {
                        message: e.to_string(),
                    },
                }
            }
            AdminRequest::RestoreLatestSnapshot => {
                self.snapshot_restore_attempt_total
                    .fetch_add(1, Ordering::Relaxed);
                let (import_enabled, cfg) = {
                    let cfg_guard = self.config.lock().unwrap();
                    let (_, _, _, _, _, import_enabled) = Self::persistence_states(&cfg_guard);
                    (import_enabled, cfg_guard.backup.clone())
                };
                if !import_enabled {
                    self.snapshot_restore_failure_total
                        .fetch_add(1, Ordering::Relaxed);
                    self.snapshot_restore_policy_block_total
                        .fetch_add(1, Ordering::Relaxed);
                    return AdminResponse::Error {
                        message: "Restore is disabled by persistence policy. Require DITTO_PERSISTENCE_PLATFORM_ALLOWED=true, DITTO_PERSISTENCE_IMPORT_ALLOWED=true and runtime property persistence-runtime-enabled=true.".into(),
                    };
                }
                match crate::backup::restore_latest_snapshot(&*self, &cfg) {
                    Ok(Some(loaded)) => {
                        self.record_snapshot_restore(
                            loaded.path.clone(),
                            loaded.entries as u64,
                            loaded.duration_ms,
                        );
                        self.snapshot_restore_success_total
                            .fetch_add(1, Ordering::Relaxed);
                        AdminResponse::RestoreResult {
                            path: loaded.path,
                            entries: loaded.entries as u64,
                            duration_ms: loaded.duration_ms,
                        }
                    }
                    Ok(None) => {
                        self.snapshot_restore_not_found_total
                            .fetch_add(1, Ordering::Relaxed);
                        AdminResponse::NotFound
                    }
                    Err(e) => AdminResponse::Error {
                        message: {
                            self.snapshot_restore_failure_total
                                .fetch_add(1, Ordering::Relaxed);
                            e.to_string()
                        },
                    },
                }
            }

            AdminRequest::GetProperty { name } => {
                let props = self.all_properties().await;
                let filtered: Vec<_> = props.into_iter().filter(|(k, _)| k == &name).collect();
                AdminResponse::Properties(filtered)
            }

            AdminRequest::SetProperty { name, value } => {
                match name.as_str() {
                    // ── active / status ──────────────────────────────────────
                    "active" | "status" => {
                        let val = matches!(
                            value.trim().to_ascii_lowercase().as_str(),
                            "true" | "active"
                        );
                        if val {
                            // Reactivating: do NOT accept client requests yet.
                            // run_resync will sync from primary first, then set active=true.
                            tracing::info!("Node reactivating — spawning re-sync pass (active until sync completes).");
                            tokio::spawn(Arc::clone(&self).run_resync());
                        } else {
                            self.active.store(false, Ordering::Relaxed);
                            self.active_set.lock().await.set_local_status(NodeStatus::Inactive);
                            tracing::info!("Node status → Inactive");
                        }
                    }

                    // ── primary (force-elect) ─────────────────────────────────
                    "primary" => {
                        let val = value.trim().eq_ignore_ascii_case("true");
                        if val {
                            self.active_set.lock().await.set_pinned_primary(self.id);
                            // Broadcast to all active peers.
                            let peers: Vec<SocketAddr> = {
                                let set = self.active_set.lock().await;
                                set.active_peers()
                                    .into_iter()
                                    .map(|n| SocketAddr::new(n.addr.ip(), n.cluster_port))
                                    .collect()
                            };
                            let msg = ClusterMessage::ForcePrimary { node_id: self.id };
                            for addr in peers {
                                let _ = send_cluster(addr, &msg, self.tls_connector.as_ref()).await;
                            }
                            tracing::info!("Force-elected self ({}) as primary", self.id);
                        } else {
                            self.active_set.lock().await.clear_pinned_primary();
                            tracing::info!("Primary pin cleared; reverting to automatic election");
                        }
                    }

                    // ── bind addresses (node must already be Inactive) ────────
                    "bind-addr" | "cluster-bind-addr" => {
                        let status = self.active_set.lock().await.local_status();
                        if status != NodeStatus::Inactive {
                            return AdminResponse::Error {
                                message: format!(
                                    "Node must be Inactive before changing bind addresses. \
                                     Use set-active false first (current status: {:?}).",
                                    status
                                ),
                            };
                        }
                        let v = value.trim().to_string();
                        let mut cfg = self.config.lock().unwrap();
                        match name.as_str() {
                            "bind-addr"         => cfg.node.bind_addr         = v.clone(),
                            _                   => cfg.node.cluster_bind_addr = v.clone(),
                        }
                        let _ = cfg.save(&self.config_path);
                        tracing::info!("{} → {} (saved; restart node to apply)", name, v);
                    }

                    // ── ports (node must already be Inactive) ─────────────────
                    "client-port" | "http-port" | "cluster-port" | "gossip-port" => {
                        let status = self.active_set.lock().await.local_status();
                        if status != NodeStatus::Inactive {
                            return AdminResponse::Error {
                                message: format!(
                                    "Node must be Inactive before changing ports.                                      Use set-active false first (current status: {:?}).",
                                    status
                                ),
                            };
                        }
                        match value.trim().parse::<u16>() {
                            Ok(port) => {
                                let mut cfg = self.config.lock().unwrap();
                                match name.as_str() {
                                    "client-port"  => cfg.node.client_port  = port,
                                    "http-port"    => cfg.node.http_port    = port,
                                    "cluster-port" => cfg.node.cluster_port = port,
                                    _              => cfg.node.gossip_port  = port,
                                }
                                let _ = cfg.save(&self.config_path);
                                tracing::info!(
                                    "{} → {} (saved; restart node to apply)",
                                    name, port
                                );
                            }
                            Err(_) => tracing::warn!("SetProperty {}: invalid port '{}'" , name, value),
                        }
                    }

                    // ── max-memory ────────────────────────────────────────────
                    "max-memory" => {
                        let trimmed = value.trim().to_ascii_lowercase();
                        let digits  = trimmed.trim_end_matches("mb").trim();
                        match digits.parse::<u64>() {
                            Ok(mb) => {
                                self.store.set_max_memory_mb(mb);
                                self.config.lock().unwrap().cache.max_memory_mb = mb;
                                tracing::info!("max-memory → {}mb", mb);
                            }
                            Err(_) => tracing::warn!("SetProperty max-memory: invalid value '{}'", value),
                        }
                    }

                    // ── default-ttl ────────────────────────────────────────────
                    "default-ttl" => {
                        match value.trim().parse::<u64>() {
                            Ok(secs) => {
                                self.store.set_default_ttl_secs(secs);
                                self.config.lock().unwrap().cache.default_ttl_secs = secs;
                                tracing::info!("default-ttl → {}s", secs);
                            }
                            Err(_) => tracing::warn!("SetProperty default-ttl: invalid value '{}'", value),
                        }
                    }

                    // ── value-size-limit ──────────────────────────────────────
                    "value-size-limit" => {
                        match value.trim().parse::<u64>() {
                            Ok(bytes) => {
                                self.store.set_value_size_limit(bytes);
                                self.config.lock().unwrap().cache.value_size_limit_bytes = bytes;
                                tracing::info!("value-size-limit → {} bytes", bytes);
                            }
                            Err(_) => tracing::warn!("SetProperty value-size-limit: invalid value '{}'", value),
                        }
                    }

                    // ── max-keys ──────────────────────────────────────────────
                    "max-keys" => {
                        match value.trim().parse::<usize>() {
                            Ok(n) => {
                                self.store.set_max_keys(n);
                                self.config.lock().unwrap().cache.max_keys = n;
                                tracing::info!("max-keys → {}", n);
                            }
                            Err(_) => tracing::warn!("SetProperty max-keys: invalid value '{}'", value),
                        }
                    }

                    // ── compression-enabled ───────────────────────────────────
                    "compression-enabled" => {
                        let val = value.trim().eq_ignore_ascii_case("true");
                        self.store.set_compression_enabled(val);
                        self.config.lock().unwrap().compression.enabled = val;
                        tracing::info!("compression-enabled → {}", val);
                    }

                    // ── compression-threshold ─────────────────────────────────
                    "compression-threshold" => {
                        match value.trim().parse::<u64>() {
                            Ok(bytes) => {
                                match self.store.set_compression_threshold(bytes) {
                                    Ok(new_val) => {
                                        self.config.lock().unwrap().compression.threshold_bytes = new_val;
                                        tracing::info!("compression-threshold → {} bytes", new_val);
                                    }
                                    Err(msg) => {
                                        tracing::warn!("SetProperty compression-threshold: {}", msg);
                                        return AdminResponse::Error { message: msg.to_string() };
                                    }
                                }
                            }
                            Err(_) => tracing::warn!("SetProperty compression-threshold: invalid value '{}'", value),
                        }
                    }

                    // ── version-check-interval ────────────────────────────────
                    "version-check-interval" => {
                        match value.trim().parse::<u64>() {
                            Ok(ms) => {
                                self.config.lock().unwrap().replication.version_check_interval_ms = ms;
                                tracing::info!("version-check-interval → {}ms", ms);
                            }
                            Err(_) => tracing::warn!("SetProperty version-check-interval: invalid value '{}'", value),
                        }
                    }
                    "read-repair-on-miss-enabled" => {
                        let val = value.trim().eq_ignore_ascii_case("true");
                        self.config.lock().unwrap().replication.read_repair_on_miss_enabled = val;
                        tracing::info!("read-repair-on-miss-enabled -> {}", val);
                    }
                    "read-repair-min-interval-ms" => {
                        match value.trim().parse::<u64>() {
                            Ok(ms) if ms > 0 => {
                                self.config.lock().unwrap().replication.read_repair_min_interval_ms = ms;
                                tracing::info!("read-repair-min-interval-ms -> {}", ms);
                            }
                            _ => tracing::warn!(
                                "SetProperty read-repair-min-interval-ms: invalid value '{}'",
                                value
                            ),
                        }
                    }

                    // ── persistence runtime gate ───────────────────────────────
                    "anti-entropy-enabled" => {
                        let val = value.trim().eq_ignore_ascii_case("true");
                        self.config.lock().unwrap().replication.anti_entropy_enabled = val;
                        tracing::info!("anti-entropy-enabled -> {}", val);
                    }
                    "anti-entropy-interval-ms" => match value.trim().parse::<u64>() {
                        Ok(ms) if ms > 0 => {
                            self.config.lock().unwrap().replication.anti_entropy_interval_ms = ms;
                            tracing::info!("anti-entropy-interval-ms -> {}", ms);
                        }
                        _ => tracing::warn!(
                            "SetProperty anti-entropy-interval-ms: invalid value '{}'",
                            value
                        ),
                    },
                    "anti-entropy-lag-threshold" => match value.trim().parse::<u64>() {
                        Ok(v) if v > 0 => {
                            self.config.lock().unwrap().replication.anti_entropy_lag_threshold = v;
                            tracing::info!("anti-entropy-lag-threshold -> {}", v);
                        }
                        _ => tracing::warn!(
                            "SetProperty anti-entropy-lag-threshold: invalid value '{}'",
                            value
                        ),
                    },
                    "anti-entropy-key-sample-size" => match value.trim().parse::<usize>() {
                        Ok(v) => {
                            self.config.lock().unwrap().replication.anti_entropy_key_sample_size = v;
                            tracing::info!("anti-entropy-key-sample-size -> {}", v);
                        }
                        _ => tracing::warn!(
                            "SetProperty anti-entropy-key-sample-size: invalid value '{}'",
                            value
                        ),
                    },
                    "anti-entropy-full-reconcile-every" => match value.trim().parse::<u64>() {
                        Ok(v) => {
                            self.config.lock().unwrap().replication.anti_entropy_full_reconcile_every = v;
                            tracing::info!("anti-entropy-full-reconcile-every -> {}", v);
                        }
                        _ => tracing::warn!(
                            "SetProperty anti-entropy-full-reconcile-every: invalid value '{}'",
                            value
                        ),
                    },
                    "anti-entropy-full-reconcile-max-keys" => match value.trim().parse::<usize>()
                    {
                        Ok(v) => {
                            self.config.lock().unwrap().replication.anti_entropy_full_reconcile_max_keys = v;
                            tracing::info!("anti-entropy-full-reconcile-max-keys -> {}", v);
                        }
                        _ => tracing::warn!(
                            "SetProperty anti-entropy-full-reconcile-max-keys: invalid value '{}'",
                            value
                        ),
                    },
                    "mixed-version-probe-enabled" => {
                        let val = value.trim().eq_ignore_ascii_case("true");
                        self.config.lock().unwrap().replication.mixed_version_probe_enabled = val;
                        tracing::info!("mixed-version-probe-enabled -> {}", val);
                    }
                    "mixed-version-probe-interval-ms" => match value.trim().parse::<u64>() {
                        Ok(ms) if ms > 0 => {
                            self.config.lock().unwrap().replication.mixed_version_probe_interval_ms = ms;
                            tracing::info!("mixed-version-probe-interval-ms -> {}", ms);
                        }
                        _ => tracing::warn!(
                            "SetProperty mixed-version-probe-interval-ms: invalid value '{}'",
                            value
                        ),
                    },
                    "persistence-runtime-enabled" | "persistence-enabled" => {
                        let val = value.trim().eq_ignore_ascii_case("true");
                        self.config.lock().unwrap().persistence.runtime_enabled = val;
                        tracing::info!("persistence-runtime-enabled → {}", val);
                    }

                    "tenancy-enabled" => {
                        let val = value.trim().eq_ignore_ascii_case("true");
                        self.config.lock().unwrap().tenancy.enabled = val;
                        tracing::info!("tenancy-enabled -> {}", val);
                    }
                    "tenancy-default-namespace" => {
                        let v = value.trim();
                        if v.is_empty() || v.contains("::") {
                            tracing::warn!(
                                "SetProperty tenancy-default-namespace: invalid value '{}'",
                                value
                            );
                        } else {
                            self.config.lock().unwrap().tenancy.default_namespace = v.to_string();
                            tracing::info!("tenancy-default-namespace -> {}", v);
                        }
                    }
                    "tenancy-max-keys-per-namespace" => match value.trim().parse::<usize>() {
                        Ok(v) => {
                            self.config.lock().unwrap().tenancy.max_keys_per_namespace = v;
                            tracing::info!("tenancy-max-keys-per-namespace -> {}", v);
                        }
                        _ => tracing::warn!(
                            "SetProperty tenancy-max-keys-per-namespace: invalid value '{}'",
                            value
                        ),
                    },

                    // rate limiter
                    "rate-limit-enabled" => {
                        let val = value.trim().eq_ignore_ascii_case("true");
                        self.config.lock().unwrap().rate_limit.enabled = val;
                        tracing::info!("rate-limit-enabled → {}", val);
                    }
                    "rate-limit-requests-per-sec" => {
                        match value.trim().parse::<u64>() {
                            Ok(v) if v > 0 => {
                                self.config.lock().unwrap().rate_limit.requests_per_sec = v;
                                tracing::info!("rate-limit-requests-per-sec → {}", v);
                            }
                            _ => tracing::warn!("SetProperty rate-limit-requests-per-sec: invalid value '{}'", value),
                        }
                    }
                    "rate-limit-burst" => {
                        match value.trim().parse::<u64>() {
                            Ok(v) if v > 0 => {
                                self.config.lock().unwrap().rate_limit.burst = v;
                                tracing::info!("rate-limit-burst → {}", v);
                            }
                            _ => tracing::warn!("SetProperty rate-limit-burst: invalid value '{}'", value),
                        }
                    }

                    "hot-key-enabled" => {
                        let val = value.trim().eq_ignore_ascii_case("true");
                        self.config.lock().unwrap().hot_key.enabled = val;
                        tracing::info!("hot-key-enabled -> {}", val);
                    }
                    "hot-key-max-waiters" => {
                        match value.trim().parse::<usize>() {
                            Ok(v) if v > 0 => {
                                self.config.lock().unwrap().hot_key.max_waiters = v;
                                tracing::info!("hot-key-max-waiters -> {}", v);
                            }
                            _ => tracing::warn!("SetProperty hot-key-max-waiters: invalid value '{}'", value),
                        }
                    }

                    // circuit breaker
                    "circuit-breaker-enabled" => {
                        let val = value.trim().eq_ignore_ascii_case("true");
                        self.config.lock().unwrap().circuit_breaker.enabled = val;
                        tracing::info!("circuit-breaker-enabled → {}", val);
                    }
                    "circuit-breaker-failure-threshold" => {
                        match value.trim().parse::<u64>() {
                            Ok(v) if v > 0 => {
                                self.config.lock().unwrap().circuit_breaker.failure_threshold = v;
                                tracing::info!("circuit-breaker-failure-threshold → {}", v);
                            }
                            _ => tracing::warn!("SetProperty circuit-breaker-failure-threshold: invalid value '{}'", value),
                        }
                    }
                    "circuit-breaker-open-ms" => {
                        match value.trim().parse::<u64>() {
                            Ok(v) if v > 0 => {
                                self.config.lock().unwrap().circuit_breaker.open_ms = v;
                                tracing::info!("circuit-breaker-open-ms → {}", v);
                            }
                            _ => tracing::warn!("SetProperty circuit-breaker-open-ms: invalid value '{}'", value),
                        }
                    }
                    "circuit-breaker-half-open-max-requests" => {
                        match value.trim().parse::<u64>() {
                            Ok(v) if v > 0 => {
                                self.config.lock().unwrap().circuit_breaker.half_open_max_requests = v;
                                tracing::info!("circuit-breaker-half-open-max-requests → {}", v);
                            }
                            _ => tracing::warn!("SetProperty circuit-breaker-half-open-max-requests: invalid value '{}'", value),
                        }
                    }

                    other => tracing::warn!("SetProperty: unknown property '{}'", other),
                }
                AdminResponse::Ok
            }
        }
    }

    // -----------------------------------------------------------------------
    // Node recovery (Syncing → Active)
    // -----------------------------------------------------------------------

    /// Run the startup recovery pass.
    ///
    /// Called once after all server tasks are launched. Waits briefly for gossip
    /// to discover peers, then fetches any missing log entries from the primary node
    /// (falling back to the peer with the highest committed index if the primary is
    /// not yet known). Transitions through Syncing to Active on success, or sets
    /// the node to Inactive if the sync fails.
    ///
    /// If no peers are found (single-node cluster) the node starts active immediately.
    pub async fn run_recovery(self: Arc<Self>) {
        // Give gossip time to discover peers.
        tokio::time::sleep(Duration::from_millis(600)).await;

        let peers: Vec<SocketAddr> = {
            let set = self.active_set.lock().await;
            set.all_nodes()
                .into_iter()
                .filter(|n| n.id != set.local_id())
                .map(|n| SocketAddr::new(n.addr.ip(), n.cluster_port))
                .collect()
        };

        if peers.is_empty() {
            tracing::info!("Recovery: no peers found, assuming single-node cluster.");
            return;
        }

        let our_index = self.write_log.lock().await.committed_index();

        // Prefer the primary; fall back to the peer furthest ahead in the log.
        let sync_addr = {
            let set = self.active_set.lock().await;
            let primary_id = set.primary_id();
            let am_primary = primary_id == Some(set.local_id());

            if am_primary {
                tracing::info!("Recovery: we are the primary, no sync needed.");
                return;
            }

            // Try primary first.
            let primary_addr = primary_id.and_then(|pid| {
                set.all_nodes()
                    .into_iter()
                    .find(|n| n.id == pid)
                    .map(|n| SocketAddr::new(n.addr.ip(), n.cluster_port))
            });

            // Fallback: peer furthest ahead.
            let fallback_addr = set
                .all_nodes()
                .into_iter()
                .filter(|n| n.id != set.local_id() && n.last_applied > our_index)
                .max_by_key(|n| n.last_applied)
                .map(|n| SocketAddr::new(n.addr.ip(), n.cluster_port));

            primary_addr.or(fallback_addr)
        };

        let peer_addr = match sync_addr {
            Some(a) => a,
            None => {
                tracing::info!("Recovery: already up-to-date (index={}).", our_index);
                return;
            }
        };

        tracing::info!(
            "Recovery: syncing from {} (our index={}).",
            peer_addr,
            our_index
        );

        // Set ourselves to Syncing so we are excluded from write quorum.
        self.active_set
            .lock()
            .await
            .set_local_status(NodeStatus::Syncing);

        let req = ClusterMessage::RequestLog {
            from_index: our_index,
        };
        match send_cluster(peer_addr, &req, self.tls_connector.as_ref()).await {
            Ok(Some(ClusterMessage::LogEntries { entries })) => {
                let count = entries.len();
                for entry in entries {
                    self.apply_locally(
                        &entry.key,
                        entry.value.clone(),
                        entry.ttl_secs,
                        entry.index,
                    );
                    self.write_log.lock().await.commit(entry.index);
                    self.active_set.lock().await.set_local_applied(entry.index);
                }
                tracing::info!("Recovery: applied {} entries.", count);
            }
            Err(e) => {
                tracing::error!(
                    "Recovery: failed to sync from {}: {}. Setting node Inactive.",
                    peer_addr,
                    e
                );
                self.active.store(false, Ordering::Relaxed);
                self.active_set
                    .lock()
                    .await
                    .set_local_status(NodeStatus::Inactive);
                return;
            }
            _ => {}
        }

        // Re-join the active set.
        self.active_set
            .lock()
            .await
            .set_local_status(NodeStatus::Active);
        let final_index = self.write_log.lock().await.committed_index();
        tracing::info!("Recovery complete. committed_index={}", final_index);

        // Announce to peers that we are synced.
        let synced_msg = ClusterMessage::Synced {
            node_id: self.id,
            last_applied: final_index,
        };
        for addr in peers {
            let _ = send_cluster(addr, &synced_msg, self.tls_connector.as_ref()).await;
        }
    }

    // Runtime re-sync (Inactive → Active transition)
    // -----------------------------------------------------------------------

    /// Re-sync from peers after a runtime Inactive → Active transition.

    /// Re-sync from the primary after a runtime Inactive to Active transition,
    /// after a backup, or when the version-check detects lag.
    ///
    /// Always syncs from the primary node. On success: sets active = true and
    /// NodeStatus::Active. On failure: sets active = false and NodeStatus::Inactive.
    pub async fn run_resync(self: Arc<Self>) {
        let our_index = self.write_log.lock().await.committed_index();

        // Mark as Syncing so we are excluded from the write quorum during catch-up.
        self.active_set
            .lock()
            .await
            .set_local_status(NodeStatus::Syncing);

        // Snapshot peer addresses for the Synced broadcast later.
        let peers: Vec<SocketAddr> = {
            let set = self.active_set.lock().await;
            set.all_nodes()
                .into_iter()
                .filter(|n| n.id != set.local_id())
                .map(|n| SocketAddr::new(n.addr.ip(), n.cluster_port))
                .collect()
        };

        // Find the primary cluster address.
        let primary_addr = {
            let set = self.active_set.lock().await;
            let primary_id = match set.primary_id() {
                Some(id) if id == set.local_id() => {
                    tracing::info!("Resync: we are the primary, rejoining active set.");
                    self.active.store(true, Ordering::Relaxed);
                    drop(set);
                    self.active_set
                        .lock()
                        .await
                        .set_local_status(NodeStatus::Active);
                    let final_index = self.write_log.lock().await.committed_index();
                    let synced_msg = ClusterMessage::Synced {
                        node_id: self.id,
                        last_applied: final_index,
                    };
                    for addr in peers {
                        let _ = send_cluster(addr, &synced_msg, self.tls_connector.as_ref()).await;
                    }
                    return;
                }
                Some(id) => id,
                None => {
                    tracing::warn!("Resync: no primary elected. Setting node Inactive.");
                    self.active.store(false, Ordering::Relaxed);
                    drop(set);
                    self.active_set
                        .lock()
                        .await
                        .set_local_status(NodeStatus::Inactive);
                    return;
                }
            };
            set.all_nodes()
                .into_iter()
                .find(|n| n.id == primary_id)
                .map(|n| SocketAddr::new(n.addr.ip(), n.cluster_port))
        };

        let peer_addr = match primary_addr {
            Some(a) => a,
            None => {
                tracing::warn!("Resync: primary address unknown. Setting node Inactive.");
                self.active.store(false, Ordering::Relaxed);
                self.active_set
                    .lock()
                    .await
                    .set_local_status(NodeStatus::Inactive);
                return;
            }
        };

        tracing::info!(
            "Resync: at index={}, fetching missed entries from primary {}.",
            our_index,
            peer_addr
        );

        let req = ClusterMessage::RequestLog {
            from_index: our_index,
        };
        match send_cluster(peer_addr, &req, self.tls_connector.as_ref()).await {
            Ok(Some(ClusterMessage::LogEntries { entries })) => {
                let count = entries.len();
                for entry in entries {
                    self.apply_locally(
                        &entry.key,
                        entry.value.clone(),
                        entry.ttl_secs,
                        entry.index,
                    );
                    self.write_log.lock().await.commit(entry.index);
                    self.active_set.lock().await.set_local_applied(entry.index);
                }
                tracing::info!("Resync: applied {} entries.", count);
            }
            Err(e) => {
                tracing::warn!(
                    "Resync: failed to sync from primary {}: {}. Setting node Inactive.",
                    peer_addr,
                    e
                );
                self.active.store(false, Ordering::Relaxed);
                self.active_set
                    .lock()
                    .await
                    .set_local_status(NodeStatus::Inactive);
                return;
            }
            _ => {}
        }

        // Success: rejoin the active set and allow client requests.
        self.active.store(true, Ordering::Relaxed);
        self.active_set
            .lock()
            .await
            .set_local_status(NodeStatus::Active);
        let final_index = self.write_log.lock().await.committed_index();
        tracing::info!(
            "Resync complete. Node is Active. committed_index={}",
            final_index
        );

        // Broadcast Synced so all peers update their view of us immediately.
        let synced_msg = ClusterMessage::Synced {
            node_id: self.id,
            last_applied: final_index,
        };
        for addr in peers {
            let _ = send_cluster(addr, &synced_msg, self.tls_connector.as_ref()).await;
        }
    }

    // -----------------------------------------------------------------------
    // Log compaction
    // -----------------------------------------------------------------------

    /// Spawn a background task that periodically compacts the write log.
    ///
    /// The *safe index* is the minimum `last_applied` across all active nodes —
    /// entries at or below that index are no longer needed for recovery and can
    /// be discarded.  Runs every 30 seconds.
    pub fn start_log_compaction(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(30));
            loop {
                ticker.tick().await;
                let safe_index = self.active_set.lock().await.min_active_applied();
                let mut log = self.write_log.lock().await;
                let before = log.committed_index();
                log.compact(safe_index);
                tracing::debug!(
                    "Log compaction: safe_index={} committed_index={}",
                    safe_index,
                    before
                );
            }
        });
    }

    /// Spawn a background task that periodically checks whether this node's
    /// committed index lags behind the primary's.
    ///
    /// If lag is detected, run_resync is triggered to re-synchronise from the
    /// primary. The check interval is read from config on every iteration so that
    /// runtime changes via SetProperty version-check-interval take effect
    /// without a restart. A value of 0 disables the check entirely.
    pub fn start_version_check(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                let interval_ms = self
                    .config
                    .lock()
                    .unwrap()
                    .replication
                    .version_check_interval_ms;
                if interval_ms == 0 {
                    // Disabled -- sleep briefly and re-check in case it gets enabled.
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
                tokio::time::sleep(Duration::from_millis(interval_ms)).await;

                // Only check when Active and not the primary.
                let (status, am_primary, our_index, primary_index) = {
                    let set = self.active_set.lock().await;
                    let status = set.local_status();
                    let am_primary = set.is_primary();
                    let our_index = set
                        .all_nodes()
                        .iter()
                        .find(|n| n.id == set.local_id())
                        .map(|n| n.last_applied)
                        .unwrap_or(0);
                    let primary_index = {
                        let all = set.all_nodes();
                        set.primary_id()
                            .and_then(|pid| all.into_iter().find(|n| n.id == pid))
                            .map(|n| n.last_applied)
                            .unwrap_or(0)
                    };
                    (status, am_primary, our_index, primary_index)
                };

                if status != NodeStatus::Active || am_primary {
                    continue;
                }

                if our_index < primary_index {
                    tracing::warn!(
                        "Version check: lag detected (our_index={}, primary_index={}). Triggering resync.",
                        our_index, primary_index
                    );
                    Arc::clone(&self).run_resync().await;
                } else {
                    tracing::debug!("Version check: in sync (index={}).", our_index);
                }
            }
        });
    }

    async fn admin_request_to_addr(
        &self,
        addr: SocketAddr,
        req: AdminRequest,
    ) -> Option<AdminResponse> {
        let msg = ClusterMessage::Admin(req);
        match send_cluster(addr, &msg, self.tls_connector.as_ref()).await {
            Ok(Some(ClusterMessage::AdminResponse(resp))) => Some(resp),
            _ => None,
        }
    }

    fn should_run_full_reconcile(run_no: u64, every: u64) -> bool {
        every > 0 && run_no > 0 && run_no.is_multiple_of(every)
    }

    fn classify_mixed_version_response(resp: Option<AdminResponse>) -> (bool, bool) {
        match resp {
            Some(AdminResponse::Properties(entries)) => {
                let expected = PROTOCOL_VERSION.to_string();
                let peer_version = entries
                    .into_iter()
                    .find(|(k, _)| k == "protocol-version")
                    .map(|(_, v)| v);
                match peer_version {
                    Some(v) if v.trim() == expected => (false, false),
                    Some(_) => (true, false),
                    None => (true, false),
                }
            }
            _ => (false, true),
        }
    }

    async fn anti_entropy_full_reconcile_once(
        &self,
        primary_addr: SocketAddr,
        max_keys: usize,
    ) -> Option<(u64, u64)> {
        let primary_keys = match self
            .admin_request_to_addr(primary_addr, AdminRequest::ListKeys { pattern: None })
            .await
        {
            Some(AdminResponse::Keys(mut keys)) => {
                keys.sort_unstable();
                keys
            }
            _ => return None,
        };
        let mut local_keys = self.store.keys(None);
        local_keys.sort_unstable();

        let mut i = 0usize;
        let mut j = 0usize;
        let mut checked = 0u64;
        let mut mismatches = 0u64;

        while i < local_keys.len() || j < primary_keys.len() {
            if max_keys > 0 && checked >= max_keys as u64 {
                break;
            }

            match (local_keys.get(i), primary_keys.get(j)) {
                (Some(lk), Some(pk)) if lk == pk => {
                    checked = checked.saturating_add(1);
                    let local_version = self.store.get(lk).map(|e| e.version);
                    let primary_version = match self
                        .admin_request_to_addr(
                            primary_addr,
                            AdminRequest::GetKeyInfo { key: lk.clone() },
                        )
                        .await
                    {
                        Some(AdminResponse::KeyInfo { version, .. }) => Some(version),
                        Some(AdminResponse::NotFound) => None,
                        _ => None,
                    };
                    if local_version != primary_version {
                        mismatches = mismatches.saturating_add(1);
                    }
                    i += 1;
                    j += 1;
                }
                (Some(lk), Some(pk)) => {
                    checked = checked.saturating_add(1);
                    if lk < pk {
                        mismatches = mismatches.saturating_add(1);
                        i += 1;
                    } else {
                        mismatches = mismatches.saturating_add(1);
                        j += 1;
                    }
                }
                (Some(_), None) => {
                    checked = checked.saturating_add(1);
                    mismatches = mismatches.saturating_add(1);
                    i += 1;
                }
                (None, Some(_)) => {
                    checked = checked.saturating_add(1);
                    mismatches = mismatches.saturating_add(1);
                    j += 1;
                }
                (None, None) => break,
            }
        }

        Some((checked, mismatches))
    }

    /// Spawn a periodic anti-entropy task.
    ///
    /// 1) lag-threshold check against primary index
    /// 2) optional key/version sample reconciliation against primary
    /// 3) optional bounded full keyspace reconciliation against primary
    pub fn start_anti_entropy(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                let (
                    enabled,
                    interval_ms,
                    lag_threshold,
                    key_sample_size,
                    full_reconcile_every,
                    full_reconcile_max_keys,
                ) = {
                    let cfg = self.config.lock().unwrap();
                    (
                        cfg.replication.anti_entropy_enabled,
                        cfg.replication.anti_entropy_interval_ms.max(1),
                        cfg.replication.anti_entropy_lag_threshold.max(1),
                        cfg.replication.anti_entropy_key_sample_size,
                        cfg.replication.anti_entropy_full_reconcile_every,
                        cfg.replication.anti_entropy_full_reconcile_max_keys,
                    )
                };

                if !enabled {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }

                tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                let run_no = self
                    .anti_entropy_runs_total
                    .fetch_add(1, Ordering::Relaxed)
                    .saturating_add(1);

                // Only check when Active and not the primary.
                let (status, am_primary, our_index, primary_index, primary_addr) = {
                    let set = self.active_set.lock().await;
                    let status = set.local_status();
                    let am_primary = set.is_primary();
                    let our_index = set
                        .all_nodes()
                        .iter()
                        .find(|n| n.id == set.local_id())
                        .map(|n| n.last_applied)
                        .unwrap_or(0);
                    let primary = {
                        let all = set.all_nodes();
                        set.primary_id()
                            .and_then(|pid| all.into_iter().find(|n| n.id == pid))
                            .map(|n| (n.last_applied, SocketAddr::new(n.addr.ip(), n.cluster_port)))
                    };
                    let (primary_index, primary_addr) =
                        primary.unwrap_or((0, "0.0.0.0:0".parse().unwrap()));
                    (status, am_primary, our_index, primary_index, primary_addr)
                };

                if status != NodeStatus::Active || am_primary || primary_addr.port() == 0 {
                    continue;
                }

                let lag = primary_index.saturating_sub(our_index);
                self.anti_entropy_last_detected_lag
                    .store(lag, Ordering::Relaxed);
                if lag >= lag_threshold {
                    self.anti_entropy_repair_trigger_total
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        "Anti-entropy: lag={} (threshold={}) detected; triggering resync.",
                        lag,
                        lag_threshold
                    );
                    Arc::clone(&self).run_resync().await;
                    continue;
                }

                if Self::should_run_full_reconcile(run_no, full_reconcile_every) {
                    self.anti_entropy_full_reconcile_runs_total
                        .fetch_add(1, Ordering::Relaxed);
                    if let Some((checked, mismatches)) = self
                        .anti_entropy_full_reconcile_once(primary_addr, full_reconcile_max_keys)
                        .await
                    {
                        self.anti_entropy_full_reconcile_key_checks_total
                            .fetch_add(checked, Ordering::Relaxed);
                        if mismatches > 0 {
                            self.anti_entropy_full_reconcile_mismatch_total
                                .fetch_add(mismatches, Ordering::Relaxed);
                            self.anti_entropy_repair_trigger_total
                                .fetch_add(1, Ordering::Relaxed);
                            tracing::warn!(
                                "Anti-entropy: full reconcile found {} mismatches (checked={}, run_no={}); triggering resync.",
                                mismatches,
                                checked,
                                run_no
                            );
                            Arc::clone(&self).run_resync().await;
                            continue;
                        }
                    }
                }

                if key_sample_size == 0 {
                    continue;
                }

                let keys = match self
                    .admin_request_to_addr(primary_addr, AdminRequest::ListKeys { pattern: None })
                    .await
                {
                    Some(AdminResponse::Keys(keys)) => keys,
                    _ => continue,
                };

                let mut checked = 0u64;
                let mut mismatches = 0u64;
                for key in keys.into_iter().take(key_sample_size) {
                    checked = checked.saturating_add(1);
                    let local_version = self.store.get(&key).map(|e| e.version);
                    let primary_version = match self
                        .admin_request_to_addr(
                            primary_addr,
                            AdminRequest::GetKeyInfo { key: key.clone() },
                        )
                        .await
                    {
                        Some(AdminResponse::KeyInfo { version, .. }) => Some(version),
                        Some(AdminResponse::NotFound) => None,
                        _ => continue,
                    };
                    if local_version != primary_version {
                        mismatches = mismatches.saturating_add(1);
                    }
                }

                self.anti_entropy_key_checks_total
                    .fetch_add(checked, Ordering::Relaxed);
                if mismatches > 0 {
                    self.anti_entropy_key_mismatch_total
                        .fetch_add(mismatches, Ordering::Relaxed);
                    self.anti_entropy_repair_trigger_total
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        "Anti-entropy: detected {} key-version mismatches in sample (checked={}); triggering resync.",
                        mismatches,
                        checked
                    );
                    Arc::clone(&self).run_resync().await;
                }
            }
        });
    }

    /// Periodic rolling-upgrade compatibility probe.
    ///
    /// Queries peer nodes for `protocol-version` via admin RPC and tracks mixed
    /// version observations without affecting write/read paths.
    pub fn start_mixed_version_probe(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                let (enabled, interval_ms) = {
                    let cfg = self.config.lock().unwrap();
                    (
                        cfg.replication.mixed_version_probe_enabled,
                        cfg.replication.mixed_version_probe_interval_ms.max(1),
                    )
                };

                if !enabled {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }

                tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                self.mixed_version_probe_runs_total
                    .fetch_add(1, Ordering::Relaxed);

                let peers: Vec<SocketAddr> = {
                    let set = self.active_set.lock().await;
                    set.all_nodes()
                        .into_iter()
                        .filter(|n| n.id != set.local_id())
                        .map(|n| SocketAddr::new(n.addr.ip(), n.cluster_port))
                        .collect()
                };

                let mut detected = 0u64;
                let mut errors = 0u64;
                for addr in peers {
                    let resp = self
                        .admin_request_to_addr(
                            addr,
                            AdminRequest::GetProperty {
                                name: "protocol-version".into(),
                            },
                        )
                        .await;
                    let (is_mixed, is_error) = Self::classify_mixed_version_response(resp);
                    if is_mixed {
                        detected = detected.saturating_add(1);
                    }
                    if is_error {
                        errors = errors.saturating_add(1);
                    }
                }

                self.mixed_version_last_detected_peer_count
                    .store(detected, Ordering::Relaxed);
                if detected > 0 {
                    self.mixed_version_peers_detected_total
                        .fetch_add(detected, Ordering::Relaxed);
                    tracing::warn!(
                        "Mixed-version probe: detected {} peer(s) not matching protocol-version={}.",
                        detected,
                        PROTOCOL_VERSION
                    );
                }
                if errors > 0 {
                    self.mixed_version_probe_errors_total
                        .fetch_add(errors, Ordering::Relaxed);
                }
            }
        });
    }

    fn namespace_quota_top_usage(
        &self,
        tenancy_enabled: bool,
        max_keys_per_namespace: usize,
        top_n: usize,
    ) -> Vec<NamespaceQuotaUsage> {
        if !tenancy_enabled || max_keys_per_namespace == 0 || top_n == 0 {
            return Vec::new();
        }

        let mut counts: HashMap<String, u64> = HashMap::new();
        for key in self.store.keys(None) {
            if let Some((namespace, _)) = key.split_once("::") {
                *counts.entry(namespace.to_string()).or_insert(0) += 1;
            }
        }

        let quota_limit = max_keys_per_namespace as u64;
        let mut usage: Vec<NamespaceQuotaUsage> = counts
            .into_iter()
            .map(|(namespace, key_count)| {
                let usage_pct = if quota_limit == 0 {
                    0
                } else {
                    key_count.saturating_mul(100) / quota_limit
                };
                let remaining_keys = quota_limit.saturating_sub(key_count);
                NamespaceQuotaUsage {
                    namespace,
                    key_count,
                    quota_limit,
                    usage_pct,
                    remaining_keys,
                }
            })
            .collect();

        usage.sort_by(|a, b| {
            b.usage_pct
                .cmp(&a.usage_pct)
                .then_with(|| b.key_count.cmp(&a.key_count))
                .then_with(|| a.namespace.cmp(&b.namespace))
        });
        usage.truncate(top_n);
        usage
    }

    fn namespace_quota_reject_velocity(&self, current_total: u64) -> (u64, String) {
        let now_ms = Self::now_millis();
        let prev_total = self
            .namespace_quota_reject_last_total
            .swap(current_total, Ordering::Relaxed);
        let prev_ts = self
            .namespace_quota_reject_last_ts_ms
            .swap(now_ms, Ordering::Relaxed);

        if prev_ts == 0 || now_ms <= prev_ts {
            return (0, "steady".to_string());
        }

        let delta_total = current_total.saturating_sub(prev_total);
        let delta_ms = now_ms.saturating_sub(prev_ts).max(1);
        let rate_per_min = delta_total.saturating_mul(60_000) / delta_ms;
        let trend = if rate_per_min >= 10 {
            "surging"
        } else if rate_per_min > 0 {
            "rising"
        } else {
            "steady"
        };
        (rate_per_min, trend.to_string())
    }

    fn format_namespace_quota_top_usage(usage: &[NamespaceQuotaUsage]) -> String {
        if usage.is_empty() {
            return "-".to_string();
        }
        usage
            .iter()
            .map(|u| {
                format!(
                    "{}:{}/{}({}%)",
                    u.namespace, u.key_count, u.quota_limit, u.usage_pct
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    fn format_request_latency_buckets(stats: &NodeStats) -> String {
        format!(
            "<=1ms:{};<=5ms:{};<=20ms:{};<=100ms:{};<=500ms:{};>500ms:{}",
            stats.client_request_latency_le_1ms_total,
            stats.client_request_latency_le_5ms_total,
            stats.client_request_latency_le_20ms_total,
            stats.client_request_latency_le_100ms_total,
            stats.client_request_latency_le_500ms_total,
            stats.client_request_latency_gt_500ms_total
        )
    }

    fn estimated_latency_percentile_ms(counts: [u64; 6], percentile: u64) -> Option<u64> {
        if percentile == 0 || percentile > 100 {
            return None;
        }
        let total: u64 = counts.iter().sum();
        if total == 0 {
            return None;
        }
        let target = ((total * percentile).saturating_add(99)) / 100;
        let mut cumulative = 0u64;
        for (idx, c) in counts.into_iter().enumerate() {
            cumulative = cumulative.saturating_add(c);
            if cumulative >= target {
                return Some(match idx {
                    0 => 1,
                    1 => 5,
                    2 => 20,
                    3 => 100,
                    4 => 500,
                    _ => 1000,
                });
            }
        }
        Some(1000)
    }

    /// Collect a point-in-time [`NodeStats`] snapshot.
    ///
    /// Acquires short-lived locks on the store, active-set, write-log, and config.
    /// Used by both the admin `GetStats` RPC and the HTTP `/health` endpoint.
    pub async fn stats(&self) -> NodeStats {
        let s = self.store.stats();
        let set = self.active_set.lock().await;
        let log = self.write_log.lock().await;
        let hot_key_inflight_keys = self.get_flights.lock().await.len() as u64;
        let cfg = self.config.lock().unwrap();
        let backup_dir_bytes = dir_size_bytes(&cfg.backup.path);
        let snapshot_last_load_path = self.snapshot_last_load_path.lock().unwrap().clone();
        let snapshot_last_load_age_secs = if snapshot_last_load_path.is_some() {
            let completed_ms = self
                .snapshot_last_load_completed_at_ms
                .load(Ordering::Relaxed);
            if completed_ms > 0 {
                Some(Self::now_millis().saturating_sub(completed_ms) / 1000)
            } else {
                Some(0)
            }
        } else {
            None
        };
        let (
            persistence_platform_allowed,
            persistence_runtime_enabled,
            persistence_enabled,
            persistence_backup_enabled,
            persistence_export_enabled,
            persistence_import_enabled,
        ) = Self::persistence_states(&cfg);
        let rate_limit_enabled = cfg.rate_limit.enabled;
        let circuit_breaker_enabled = cfg.circuit_breaker.enabled;
        let hot_key_enabled = cfg.hot_key.enabled;
        let read_repair_enabled = cfg.replication.read_repair_on_miss_enabled;
        let tenancy_enabled = cfg.tenancy.enabled;
        let tenancy_default_namespace = cfg.tenancy.default_namespace.clone();
        let tenancy_max_keys_per_namespace = cfg.tenancy.max_keys_per_namespace;
        let namespace_quota_top_usage =
            self.namespace_quota_top_usage(tenancy_enabled, tenancy_max_keys_per_namespace, 3);
        let namespace_quota_reject_total =
            self.namespace_quota_reject_total.load(Ordering::Relaxed);
        let (namespace_quota_reject_rate_per_min, namespace_quota_reject_trend) =
            self.namespace_quota_reject_velocity(namespace_quota_reject_total);
        let circuit_breaker_state = if circuit_breaker_enabled {
            self.circuit.lock().unwrap().state.as_str().to_string()
        } else {
            "disabled".to_string()
        };
        let client_request_latency_buckets = [
            self.client_request_latency_le_1ms_total
                .load(Ordering::Relaxed),
            self.client_request_latency_le_5ms_total
                .load(Ordering::Relaxed),
            self.client_request_latency_le_20ms_total
                .load(Ordering::Relaxed),
            self.client_request_latency_le_100ms_total
                .load(Ordering::Relaxed),
            self.client_request_latency_le_500ms_total
                .load(Ordering::Relaxed),
            self.client_request_latency_gt_500ms_total
                .load(Ordering::Relaxed),
        ];
        let client_latency_p50_estimate_ms =
            Self::estimated_latency_percentile_ms(client_request_latency_buckets, 50);
        let client_latency_p90_estimate_ms =
            Self::estimated_latency_percentile_ms(client_request_latency_buckets, 90);
        let client_latency_p95_estimate_ms =
            Self::estimated_latency_percentile_ms(client_request_latency_buckets, 95);
        let client_latency_p99_estimate_ms =
            Self::estimated_latency_percentile_ms(client_request_latency_buckets, 99);
        NodeStats {
            node_id: self.id,
            status: set.local_status(),
            is_primary: set.is_primary(),
            committed_index: log.committed_index(),
            key_count: s.key_count,
            memory_used_bytes: s.memory_used_bytes,
            memory_max_bytes: s.memory_max_bytes,
            evictions: s.evictions,
            hit_count: s.hit_count,
            miss_count: s.miss_count,
            uptime_secs: self.started_at.elapsed().as_secs(),
            value_size_limit_bytes: s.value_size_limit_bytes,
            max_keys_limit: s.max_keys_limit,
            compression_enabled: s.compression_enabled,
            compression_threshold_bytes: s.compression_threshold_bytes,
            node_name: cfg.node.id.clone(),
            backup_dir_bytes,
            snapshot_last_load_path,
            snapshot_last_load_duration_ms: self
                .snapshot_last_load_duration_ms
                .load(Ordering::Relaxed),
            snapshot_last_load_entries: self.snapshot_last_load_entries.load(Ordering::Relaxed),
            snapshot_last_load_age_secs,
            snapshot_restore_attempt_total: self
                .snapshot_restore_attempt_total
                .load(Ordering::Relaxed),
            snapshot_restore_success_total: self
                .snapshot_restore_success_total
                .load(Ordering::Relaxed),
            snapshot_restore_failure_total: self
                .snapshot_restore_failure_total
                .load(Ordering::Relaxed),
            snapshot_restore_not_found_total: self
                .snapshot_restore_not_found_total
                .load(Ordering::Relaxed),
            snapshot_restore_policy_block_total: self
                .snapshot_restore_policy_block_total
                .load(Ordering::Relaxed),
            persistence_platform_allowed,
            persistence_runtime_enabled,
            persistence_enabled,
            persistence_backup_enabled,
            persistence_export_enabled,
            persistence_import_enabled,
            tenancy_enabled,
            tenancy_default_namespace,
            tenancy_max_keys_per_namespace,
            rate_limit_enabled,
            rate_limited_requests_total: self.rate_limited_requests_total.load(Ordering::Relaxed),
            circuit_breaker_enabled,
            hot_key_enabled,
            read_repair_enabled,
            hot_key_coalesced_hits_total: self.hot_key_coalesced_hits_total.load(Ordering::Relaxed),
            hot_key_fallback_exec_total: self.hot_key_fallback_exec_total.load(Ordering::Relaxed),
            hot_key_inflight_keys,
            read_repair_trigger_total: self.read_repair_trigger_total.load(Ordering::Relaxed),
            read_repair_success_total: self.read_repair_success_total.load(Ordering::Relaxed),
            read_repair_throttled_total: self.read_repair_throttled_total.load(Ordering::Relaxed),
            namespace_quota_reject_total,
            namespace_quota_reject_rate_per_min,
            namespace_quota_reject_trend,
            namespace_quota_top_usage,
            anti_entropy_runs_total: self.anti_entropy_runs_total.load(Ordering::Relaxed),
            anti_entropy_repair_trigger_total: self
                .anti_entropy_repair_trigger_total
                .load(Ordering::Relaxed),
            anti_entropy_last_detected_lag: self
                .anti_entropy_last_detected_lag
                .load(Ordering::Relaxed),
            anti_entropy_key_checks_total: self
                .anti_entropy_key_checks_total
                .load(Ordering::Relaxed),
            anti_entropy_key_mismatch_total: self
                .anti_entropy_key_mismatch_total
                .load(Ordering::Relaxed),
            anti_entropy_full_reconcile_runs_total: self
                .anti_entropy_full_reconcile_runs_total
                .load(Ordering::Relaxed),
            anti_entropy_full_reconcile_key_checks_total: self
                .anti_entropy_full_reconcile_key_checks_total
                .load(Ordering::Relaxed),
            anti_entropy_full_reconcile_mismatch_total: self
                .anti_entropy_full_reconcile_mismatch_total
                .load(Ordering::Relaxed),
            mixed_version_probe_runs_total: self
                .mixed_version_probe_runs_total
                .load(Ordering::Relaxed),
            mixed_version_peers_detected_total: self
                .mixed_version_peers_detected_total
                .load(Ordering::Relaxed),
            mixed_version_probe_errors_total: self
                .mixed_version_probe_errors_total
                .load(Ordering::Relaxed),
            mixed_version_last_detected_peer_count: self
                .mixed_version_last_detected_peer_count
                .load(Ordering::Relaxed),
            circuit_breaker_state,
            circuit_breaker_open_total: self.circuit_breaker_open_total.load(Ordering::Relaxed),
            circuit_breaker_reject_total: self.circuit_breaker_reject_total.load(Ordering::Relaxed),
            client_requests_total: self.client_requests_total.load(Ordering::Relaxed),
            client_requests_tcp_total: self.client_requests_tcp_total.load(Ordering::Relaxed),
            client_requests_http_total: self.client_requests_http_total.load(Ordering::Relaxed),
            client_requests_internal_total: self
                .client_requests_internal_total
                .load(Ordering::Relaxed),
            client_request_latency_le_1ms_total: client_request_latency_buckets[0],
            client_request_latency_le_5ms_total: client_request_latency_buckets[1],
            client_request_latency_le_20ms_total: client_request_latency_buckets[2],
            client_request_latency_le_100ms_total: client_request_latency_buckets[3],
            client_request_latency_le_500ms_total: client_request_latency_buckets[4],
            client_request_latency_gt_500ms_total: client_request_latency_buckets[5],
            client_latency_p50_estimate_ms,
            client_latency_p90_estimate_ms,
            client_latency_p95_estimate_ms,
            client_latency_p99_estimate_ms,
            client_error_total: self.client_error_total.load(Ordering::Relaxed),
            client_errors_tcp_total: self.client_errors_tcp_total.load(Ordering::Relaxed),
            client_errors_http_total: self.client_errors_http_total.load(Ordering::Relaxed),
            client_errors_internal_total: self.client_errors_internal_total.load(Ordering::Relaxed),
            client_error_auth_total: self.client_error_auth_total.load(Ordering::Relaxed),
            client_error_throttle_total: self.client_error_throttle_total.load(Ordering::Relaxed),
            client_error_availability_total: self
                .client_error_availability_total
                .load(Ordering::Relaxed),
            client_error_validation_total: self
                .client_error_validation_total
                .load(Ordering::Relaxed),
            client_error_internal_total: self.client_error_internal_total.load(Ordering::Relaxed),
            client_error_other_total: self.client_error_other_total.load(Ordering::Relaxed),
        }
    }
}

/// Returns the total size (in bytes) of all regular files in `path`.
/// Returns 0 if the directory does not exist or cannot be read.
fn dir_size_bytes(path: &str) -> u64 {
    std::fs::read_dir(path)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter_map(|e| e.metadata().ok())
                .filter(|m| m.is_file())
                .map(|m| m.len())
                .sum()
        })
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{
        CircuitState, ClientRequestSource, GetFlight, NodeHandle, TokenBucket, PROTOCOL_VERSION,
    };
    use crate::config::Config;
    use crate::store::kv_store::ExportEntry;
    use crate::store::KvStore;
    use bytes::Bytes;
    use ditto_protocol::{AdminRequest, AdminResponse, ClientRequest, ClientResponse, ErrorCode};
    use std::sync::{atomic::Ordering, Arc};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn test_node(mut cfg: Config) -> Arc<NodeHandle> {
        cfg.node.id = "test-node".into();
        let store = Arc::new(KvStore::new(
            cfg.cache.max_memory_mb,
            cfg.cache.default_ttl_secs,
            cfg.cache.value_size_limit_bytes,
            cfg.cache.max_keys,
            cfg.compression.enabled,
            cfg.compression.threshold_bytes,
        ));
        NodeHandle::new(
            cfg,
            "test-node.toml".into(),
            store,
            None,
            "127.0.0.1".into(),
        )
    }

    fn temp_backup_dir(tag: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let mut path = std::env::temp_dir();
        path.push(format!("ditto-{}-{}-{}", tag, std::process::id(), nanos));
        std::fs::create_dir_all(&path).expect("create temp backup dir");
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn persistence_states_default_is_fully_disabled() {
        let cfg = Config::default();
        let (platform, runtime, enabled, backup, export, import) =
            NodeHandle::persistence_states(&cfg);
        assert!(!platform);
        assert!(!runtime);
        assert!(!enabled);
        assert!(!backup);
        assert!(!export);
        assert!(!import);
    }

    #[test]
    fn persistence_states_require_platform_and_runtime() {
        let mut cfg = Config::default();
        cfg.persistence.platform_allowed = true;
        cfg.persistence.runtime_enabled = false;
        cfg.persistence.backup_allowed = true;
        cfg.persistence.export_allowed = true;
        cfg.persistence.import_allowed = true;

        let (_platform, _runtime, enabled, backup, export, import) =
            NodeHandle::persistence_states(&cfg);
        assert!(!enabled);
        assert!(!backup);
        assert!(!export);
        assert!(!import);
    }

    #[test]
    fn persistence_states_apply_feature_flags_after_global_enable() {
        let mut cfg = Config::default();
        cfg.persistence.platform_allowed = true;
        cfg.persistence.runtime_enabled = true;
        cfg.persistence.backup_allowed = true;
        cfg.persistence.export_allowed = false;
        cfg.persistence.import_allowed = true;

        let (_platform, _runtime, enabled, backup, export, import) =
            NodeHandle::persistence_states(&cfg);
        assert!(enabled);
        assert!(backup);
        assert!(!export);
        assert!(import);
    }

    #[test]
    fn anti_entropy_full_reconcile_schedule_helper() {
        assert!(!NodeHandle::should_run_full_reconcile(0, 10));
        assert!(!NodeHandle::should_run_full_reconcile(9, 10));
        assert!(NodeHandle::should_run_full_reconcile(10, 10));
        assert!(NodeHandle::should_run_full_reconcile(20, 10));
        assert!(!NodeHandle::should_run_full_reconcile(20, 0));
    }

    #[test]
    fn mixed_version_response_classifier() {
        let ok = Some(AdminResponse::Properties(vec![(
            "protocol-version".into(),
            PROTOCOL_VERSION.to_string(),
        )]));
        assert_eq!(
            NodeHandle::classify_mixed_version_response(ok),
            (false, false)
        );

        let mismatch = Some(AdminResponse::Properties(vec![(
            "protocol-version".into(),
            (PROTOCOL_VERSION + 1).to_string(),
        )]));
        assert_eq!(
            NodeHandle::classify_mixed_version_response(mismatch),
            (true, false)
        );

        let missing = Some(AdminResponse::Properties(vec![(
            "other".into(),
            "x".into(),
        )]));
        assert_eq!(
            NodeHandle::classify_mixed_version_response(missing),
            (true, false)
        );

        let error = Some(AdminResponse::Error {
            message: "boom".into(),
        });
        assert_eq!(
            NodeHandle::classify_mixed_version_response(error),
            (false, true)
        );
    }

    #[tokio::test]
    async fn restore_snapshot_is_blocked_when_import_gate_is_disabled() {
        let backup_dir = temp_backup_dir("restore-gate");
        let mut cfg = Config::default();
        cfg.backup.path = backup_dir.clone();
        cfg.persistence.platform_allowed = true;
        cfg.persistence.runtime_enabled = true;
        cfg.persistence.import_allowed = false;
        let node = test_node(cfg);

        let resp = Arc::clone(&node)
            .handle_admin(AdminRequest::RestoreLatestSnapshot)
            .await;
        match resp {
            AdminResponse::Error { message } => {
                assert!(message.contains("Restore is disabled by persistence policy"));
            }
            other => panic!("expected AdminResponse::Error, got {:?}", other),
        }
        let stats = node.stats().await;
        assert_eq!(stats.snapshot_restore_attempt_total, 1);
        assert_eq!(stats.snapshot_restore_success_total, 0);
        assert_eq!(stats.snapshot_restore_failure_total, 1);
        assert_eq!(stats.snapshot_restore_not_found_total, 0);
        assert_eq!(stats.snapshot_restore_policy_block_total, 1);

        let _ = std::fs::remove_dir_all(backup_dir);
    }

    #[tokio::test]
    async fn restore_snapshot_loads_entries_and_updates_stats() {
        let backup_dir = temp_backup_dir("restore-success");
        let mut cfg = Config::default();
        cfg.backup.path = backup_dir.clone();
        cfg.persistence.platform_allowed = true;
        cfg.persistence.runtime_enabled = true;
        cfg.persistence.import_allowed = true;
        let node = test_node(cfg);

        let snapshot_entries = vec![(
            "snap:key".to_string(),
            ExportEntry {
                value: b"snap-value".to_vec(),
                version: 42,
                expires_at_ms: None,
            },
        )];
        let data = bincode::serialize(&snapshot_entries).expect("serialize snapshot");
        let file =
            std::path::Path::new(&backup_dir).join("test-node_backup_2099.01.01_00-00-00_UTC.bin");
        std::fs::write(&file, data).expect("write snapshot file");

        let resp = Arc::clone(&node)
            .handle_admin(AdminRequest::RestoreLatestSnapshot)
            .await;
        match resp {
            AdminResponse::RestoreResult { entries, .. } => assert_eq!(entries, 1),
            other => panic!("expected AdminResponse::RestoreResult, got {:?}", other),
        }

        let restored = node.store.get("snap:key").expect("restored key missing");
        assert_eq!(restored.value, Bytes::from_static(b"snap-value"));
        assert_eq!(restored.version, 42);

        let stats = node.stats().await;
        assert_eq!(stats.snapshot_last_load_entries, 1);
        assert!(stats.snapshot_last_load_path.is_some());
        assert!(stats.snapshot_last_load_age_secs.is_some());
        assert_eq!(stats.snapshot_restore_attempt_total, 1);
        assert_eq!(stats.snapshot_restore_success_total, 1);
        assert_eq!(stats.snapshot_restore_failure_total, 0);
        assert_eq!(stats.snapshot_restore_not_found_total, 0);
        assert_eq!(stats.snapshot_restore_policy_block_total, 0);

        let _ = std::fs::remove_dir_all(backup_dir);
    }

    #[tokio::test]
    async fn restore_snapshot_invalid_file_increments_failure_counter() {
        let backup_dir = temp_backup_dir("restore-invalid");
        let mut cfg = Config::default();
        cfg.backup.path = backup_dir.clone();
        cfg.persistence.platform_allowed = true;
        cfg.persistence.runtime_enabled = true;
        cfg.persistence.import_allowed = true;
        let node = test_node(cfg);

        let file =
            std::path::Path::new(&backup_dir).join("test-node_backup_2099.01.01_00-00-00_UTC.bin");
        std::fs::write(&file, b"not-bincode").expect("write invalid snapshot file");

        let resp = Arc::clone(&node)
            .handle_admin(AdminRequest::RestoreLatestSnapshot)
            .await;
        match resp {
            AdminResponse::Error { .. } => {}
            other => panic!("expected AdminResponse::Error, got {:?}", other),
        }

        let stats = node.stats().await;
        assert_eq!(stats.snapshot_restore_attempt_total, 1);
        assert_eq!(stats.snapshot_restore_success_total, 0);
        assert_eq!(stats.snapshot_restore_failure_total, 1);
        assert_eq!(stats.snapshot_restore_not_found_total, 0);
        assert_eq!(stats.snapshot_restore_policy_block_total, 0);

        let _ = std::fs::remove_dir_all(backup_dir);
    }

    #[test]
    fn token_bucket_limits_and_refills() {
        let mut bucket = TokenBucket::new(2, 2);
        assert!(bucket.try_take());
        assert!(bucket.try_take());
        assert!(!bucket.try_take());
        std::thread::sleep(Duration::from_millis(600));
        assert!(bucket.try_take());
    }

    #[test]
    fn token_bucket_burst_cap_under_load() {
        let mut bucket = TokenBucket::new(10, 10);
        let mut allowed = 0usize;
        for _ in 0..100 {
            if bucket.try_take() {
                allowed += 1;
            }
        }
        assert_eq!(allowed, 10);
    }

    #[tokio::test]
    async fn client_observability_counters_track_latency_and_error_categories() {
        let node = test_node(Config::default());

        node.observe_client_response_metrics(
            ClientRequestSource::Tcp,
            Duration::from_millis(1),
            &ClientResponse::Pong,
        );
        node.observe_client_response_metrics(
            ClientRequestSource::Http,
            Duration::from_millis(12),
            &ClientResponse::Error {
                code: ErrorCode::RateLimited,
                message: "synthetic".into(),
            },
        );
        node.observe_client_response_metrics(
            ClientRequestSource::Internal,
            Duration::from_millis(250),
            &ClientResponse::Error {
                code: ErrorCode::NoQuorum,
                message: "synthetic".into(),
            },
        );
        node.observe_client_response_metrics(
            ClientRequestSource::Http,
            Duration::from_millis(750),
            &ClientResponse::Error {
                code: ErrorCode::InternalError,
                message: "synthetic".into(),
            },
        );

        let stats = node.stats().await;
        assert_eq!(stats.client_requests_total, 4);
        assert_eq!(stats.client_requests_tcp_total, 1);
        assert_eq!(stats.client_requests_http_total, 2);
        assert_eq!(stats.client_requests_internal_total, 1);
        assert_eq!(stats.client_request_latency_le_1ms_total, 1);
        assert_eq!(stats.client_request_latency_le_20ms_total, 1);
        assert_eq!(stats.client_request_latency_le_500ms_total, 1);
        assert_eq!(stats.client_request_latency_gt_500ms_total, 1);
        assert_eq!(stats.client_latency_p50_estimate_ms, Some(20));
        assert_eq!(stats.client_latency_p90_estimate_ms, Some(1000));
        assert_eq!(stats.client_error_total, 3);
        assert_eq!(stats.client_errors_tcp_total, 0);
        assert_eq!(stats.client_errors_http_total, 2);
        assert_eq!(stats.client_errors_internal_total, 1);
        assert_eq!(stats.client_error_throttle_total, 1);
        assert_eq!(stats.client_error_availability_total, 1);
        assert_eq!(stats.client_error_internal_total, 1);
    }

    #[test]
    fn rate_limiter_denials_increment_counter() {
        let mut cfg = Config::default();
        cfg.rate_limit.enabled = true;
        cfg.rate_limit.burst = 1;
        cfg.rate_limit.requests_per_sec = 1;
        let node = test_node(cfg);

        assert!(node.allow_by_rate_limit());
        assert!(!node.allow_by_rate_limit());
        assert_eq!(node.rate_limited_requests_total.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn circuit_breaker_opens_on_threshold_and_rejects_while_open() {
        let mut cfg = Config::default();
        cfg.circuit_breaker.enabled = true;
        cfg.circuit_breaker.failure_threshold = 2;
        cfg.circuit_breaker.open_ms = 60_000;
        cfg.circuit_breaker.half_open_max_requests = 1;
        let node = test_node(cfg);

        node.record_circuit_result(&ClientResponse::Error {
            code: ErrorCode::NoQuorum,
            message: "synthetic".into(),
        });
        node.record_circuit_result(&ClientResponse::Error {
            code: ErrorCode::WriteTimeout,
            message: "synthetic".into(),
        });

        {
            let c = node.circuit.lock().unwrap();
            assert_eq!(c.state, CircuitState::Open);
        }
        assert_eq!(node.circuit_breaker_open_total.load(Ordering::Relaxed), 1);
        assert!(!node.allow_by_circuit_breaker());
        assert_eq!(node.circuit_breaker_reject_total.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn circuit_breaker_half_open_closes_after_success_quota() {
        let mut cfg = Config::default();
        cfg.circuit_breaker.enabled = true;
        cfg.circuit_breaker.failure_threshold = 1;
        cfg.circuit_breaker.open_ms = 1;
        cfg.circuit_breaker.half_open_max_requests = 2;
        let node = test_node(cfg);

        {
            let mut c = node.circuit.lock().unwrap();
            c.state = CircuitState::Open;
            c.open_until_ms = NodeHandle::now_millis().saturating_sub(1);
            c.half_open_successes = 0;
            c.consecutive_failures = 1;
        }

        assert!(node.allow_by_circuit_breaker());
        {
            let c = node.circuit.lock().unwrap();
            assert_eq!(c.state, CircuitState::HalfOpen);
            assert_eq!(c.half_open_successes, 0);
        }

        node.record_circuit_result(&ClientResponse::Pong);
        {
            let c = node.circuit.lock().unwrap();
            assert_eq!(c.state, CircuitState::HalfOpen);
            assert_eq!(c.half_open_successes, 1);
        }

        node.record_circuit_result(&ClientResponse::Pong);
        {
            let c = node.circuit.lock().unwrap();
            assert_eq!(c.state, CircuitState::Closed);
            assert_eq!(c.consecutive_failures, 0);
            assert_eq!(c.half_open_successes, 0);
        }
    }

    #[tokio::test]
    async fn hot_key_waiter_uses_coalesced_response() {
        let mut cfg = Config::default();
        cfg.hot_key.enabled = true;
        cfg.hot_key.max_waiters = 8;
        let node = test_node(cfg);
        let key = "hot:key".to_string();

        let flight = Arc::new(GetFlight::new());
        {
            let mut flights = node.get_flights.lock().await;
            flights.insert(key.clone(), Arc::clone(&flight));
        }

        let flight_for_sender = Arc::clone(&flight);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(15)).await;
            let _ = flight_for_sender.tx.send(Some(ClientResponse::Value {
                key: "hot:key".into(),
                value: Bytes::from("v1"),
                version: 7,
            }));
        });

        let resp = node.handle_get_with_single_flight(key.clone(), key).await;
        assert!(matches!(resp, ClientResponse::Value { version: 7, .. }));
        assert_eq!(node.hot_key_coalesced_hits_total.load(Ordering::Relaxed), 1);
        assert_eq!(node.hot_key_fallback_exec_total.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn hot_key_waiter_cap_triggers_fallback() {
        let mut cfg = Config::default();
        cfg.hot_key.enabled = true;
        cfg.hot_key.max_waiters = 1;
        let node = test_node(cfg);
        let key = "hot:cap".to_string();

        let flight = Arc::new(GetFlight::new());
        flight.waiters.store(1, Ordering::Relaxed);
        {
            let mut flights = node.get_flights.lock().await;
            flights.insert(key.clone(), Arc::clone(&flight));
        }

        let resp = node.handle_get_with_single_flight(key.clone(), key).await;
        assert!(matches!(resp, ClientResponse::NotFound));
        assert_eq!(node.hot_key_fallback_exec_total.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn namespace_quota_top_usage_empty_when_quota_disabled() {
        let mut cfg = Config::default();
        cfg.tenancy.enabled = true;
        cfg.tenancy.max_keys_per_namespace = 0;
        let node = test_node(cfg);

        let stats = node.stats().await;
        assert!(stats.namespace_quota_top_usage.is_empty());
        assert_eq!(stats.namespace_quota_reject_rate_per_min, 0);
        assert_eq!(stats.namespace_quota_reject_trend, "steady");
    }

    #[tokio::test]
    async fn namespace_quota_top_usage_partial_pressure_sorted() {
        let mut cfg = Config::default();
        cfg.tenancy.enabled = true;
        cfg.tenancy.max_keys_per_namespace = 10;
        let node = test_node(cfg);

        let _ = node
            .handle_client(ClientRequest::Set {
                key: "k1".into(),
                value: Bytes::from("v"),
                ttl_secs: None,
                namespace: Some("tenant-a".into()),
            })
            .await;
        let _ = node
            .handle_client(ClientRequest::Set {
                key: "k1".into(),
                value: Bytes::from("v"),
                ttl_secs: None,
                namespace: Some("tenant-b".into()),
            })
            .await;
        let _ = node
            .handle_client(ClientRequest::Set {
                key: "k2".into(),
                value: Bytes::from("v"),
                ttl_secs: None,
                namespace: Some("tenant-b".into()),
            })
            .await;

        let stats = node.stats().await;
        assert_eq!(stats.namespace_quota_top_usage.len(), 2);
        assert_eq!(stats.namespace_quota_top_usage[0].namespace, "tenant-b");
        assert_eq!(stats.namespace_quota_top_usage[0].key_count, 2);
        assert_eq!(stats.namespace_quota_top_usage[0].usage_pct, 20);
        assert_eq!(stats.namespace_quota_top_usage[1].namespace, "tenant-a");
        assert_eq!(stats.namespace_quota_top_usage[1].key_count, 1);
        assert_eq!(stats.namespace_quota_top_usage[1].usage_pct, 10);
    }

    #[tokio::test]
    async fn namespace_quota_top_usage_full_pressure_and_reject_trend() {
        let mut cfg = Config::default();
        cfg.tenancy.enabled = true;
        cfg.tenancy.max_keys_per_namespace = 2;
        let node = test_node(cfg);

        let _ = node
            .handle_client(ClientRequest::Set {
                key: "k1".into(),
                value: Bytes::from("v"),
                ttl_secs: None,
                namespace: Some("tenant-full".into()),
            })
            .await;
        let _ = node
            .handle_client(ClientRequest::Set {
                key: "k2".into(),
                value: Bytes::from("v"),
                ttl_secs: None,
                namespace: Some("tenant-full".into()),
            })
            .await;

        let blocked = node
            .handle_client(ClientRequest::Set {
                key: "k3".into(),
                value: Bytes::from("v"),
                ttl_secs: None,
                namespace: Some("tenant-full".into()),
            })
            .await;
        assert!(matches!(blocked, ClientResponse::Error { .. }));

        node.namespace_quota_reject_last_total
            .store(0, Ordering::Relaxed);
        node.namespace_quota_reject_last_ts_ms.store(
            NodeHandle::now_millis().saturating_sub(1_000),
            Ordering::Relaxed,
        );

        let stats = node.stats().await;
        let top = &stats.namespace_quota_top_usage[0];
        assert_eq!(top.namespace, "tenant-full");
        assert_eq!(top.key_count, 2);
        assert_eq!(top.usage_pct, 100);
        assert_eq!(top.remaining_keys, 0);
        assert!(stats.namespace_quota_reject_rate_per_min >= 1);
        assert!(matches!(
            stats.namespace_quota_reject_trend.as_str(),
            "rising" | "surging"
        ));
    }
}
