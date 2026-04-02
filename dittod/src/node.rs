use crate::{
    config::Config,
    replication::{ActiveSet, WriteLog},
    store::{kv_store::{LimitError, sanitize_for_log}, KvStore},
    network::cluster_server::send_cluster,
};
use bytes::Bytes;
use ditto_protocol::{
    AdminRequest, AdminResponse, ClientRequest, ClientResponse,
    ClusterMessage, ErrorCode, NodeStats, NodeStatus,
};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{broadcast, Mutex as AsyncMutex};
use tokio_rustls::TlsConnector;
use tracing::{info, warn};
use uuid::Uuid;

/// DITTO-02: watch event payload broadcast to all TCP connections.
/// `value = None` means the key was deleted.
pub type WatchEventPayload = (String, Option<Bytes>, u64);

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

/// Shared state handle passed to every server task.
pub struct NodeHandle {
    pub id:           Uuid,
    /// Runtime-mutable config (port changes write back to file).
    pub config:       Mutex<Config>,
    /// Path to the node.toml config file for persistence.
    pub config_path:  String,
    pub store:        Arc<KvStore>,
    pub write_log:    Arc<AsyncMutex<WriteLog>>,
    pub active_set:   Arc<AsyncMutex<ActiveSet>>,
    pub started_at:   Instant,
    /// Runtime active/inactive toggle (set via SetProperty "active").
    /// Initialised from config.node.active; can be flipped without restart.
    pub active:       AtomicBool,
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
        let local_addr: SocketAddr = format!(
            "{}:{}",
            cluster_bind_ip, config.node.cluster_port
        )
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
            active:        AtomicBool::new(config.node.active),
            config:        Mutex::new(config.clone()),
            config_path,
            store,
            write_log:     Arc::new(AsyncMutex::new(WriteLog::new())),
            active_set:    Arc::new(AsyncMutex::new(active_set)),
            started_at:    Instant::now(),
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
        })
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
            self.rate_limited_requests_total.fetch_add(1, Ordering::Relaxed);
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
                    self.circuit_breaker_reject_total.fetch_add(1, Ordering::Relaxed);
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
                        self.circuit_breaker_open_total.fetch_add(1, Ordering::Relaxed);
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
                    self.circuit_breaker_open_total.fetch_add(1, Ordering::Relaxed);
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

    /// Dispatch a client request to the appropriate handler.
    ///
    /// Returns `NodeInactive` immediately when the node is in maintenance mode.
    /// Reads are served locally; writes are coordinated or forwarded to the primary.
    pub async fn handle_client(&self, req: ClientRequest) -> ClientResponse {
        if !self.active.load(Ordering::Relaxed) {
            return ClientResponse::Error {
                code:    ErrorCode::NodeInactive,
                message: "Node is inactive (maintenance mode)".into(),
            };
        }
        if !self.allow_by_rate_limit() {
            return ClientResponse::Error {
                code: ErrorCode::RateLimited,
                message: "Request throttled by rate limiter".into(),
            };
        }
        if !self.allow_by_circuit_breaker() {
            return ClientResponse::Error {
                code: ErrorCode::CircuitOpen,
                message: "Request rejected by circuit breaker".into(),
            };
        }
        let response = match req {
            ClientRequest::Ping => ClientResponse::Pong,
            ClientRequest::Auth { .. } => ClientResponse::Error {
                code: ErrorCode::InternalError,
                message: "Authentication is already complete or invalid in this context".into(),
            },

            ClientRequest::Get { key } => {
                match self.store.get(&key) {
                    Some(entry) => ClientResponse::Value {
                        key,
                        value:   entry.value,
                        version: entry.version,
                    },
                    None => ClientResponse::NotFound,
                }
            }

            ClientRequest::Set { key, value, ttl_secs } => {
                match self.store.check_limits(&key, &value) {
                    Err(LimitError::ValueTooLarge) => ClientResponse::Error {
                        code:    ErrorCode::ValueTooLarge,
                        message: format!(
                            "Value size {} bytes exceeds the configured limit",
                            value.len()
                        ),
                    },
                    Err(LimitError::KeyLimitReached) => ClientResponse::Error {
                        code:    ErrorCode::KeyLimitReached,
                        message: "Cache is at the maximum key count limit".into(),
                    },
                    Ok(()) => self.coordinate_write(key, Some(value), ttl_secs).await,
                }
            }

            ClientRequest::Delete { key } => {
                self.coordinate_write(key, None, None).await
            }

            ClientRequest::DeleteByPattern { pattern } => {
                self.delete_by_pattern(pattern).await
            }

            ClientRequest::SetTtlByPattern { pattern, ttl_secs } => {
                self.set_ttl_by_pattern(pattern, ttl_secs).await
            }

            // Watch/Unwatch are handled at the TCP connection level (tcp_server.rs)
            // and should never reach handle_client. Guard against misuse.
            ClientRequest::Watch { .. } | ClientRequest::Unwatch { .. } => {
                ClientResponse::Error {
                    code:    ErrorCode::InternalError,
                    message: "Watch/Unwatch must be handled at the connection level".into(),
                }
            }
        };
        self.record_circuit_result(&response);
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

    async fn set_ttl_by_pattern(
        &self,
        pattern: String,
        ttl_secs: Option<u64>,
    ) -> ClientResponse {
        let keys = self.store.keys(Some(&pattern));
        let mut updated = 0usize;
        for key in keys {
            let Some(entry) = self.store.get(&key) else {
                continue;
            };
            let resp = self.coordinate_write(key, Some(entry.value), ttl_secs).await;
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
            let resp = self.forward_to_primary(key.clone(), value.clone(), ttl_secs).await;
            // forward_to_primary marks the old primary as Inactive when it refuses.
            // Re-check whether we became primary; if so, fall through to the write path.
            if !matches!(&resp, ClientResponse::Error { code: ErrorCode::NodeInactive, .. })
                || !self.active_set.lock().await.is_primary()
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
            key:      key.clone(),
            value:    value.clone(),
            ttl_secs,
        };

        let timeout = Duration::from_millis(self.config.lock().unwrap().replication.write_timeout_ms);
        let all_acked = self.broadcast_and_wait(&peers, &prepare_msg, timeout).await;

        if !all_acked {
            warn!("PREPARE timed out for log_index={}", log_index);
            return ClientResponse::Error {
                code:    ErrorCode::WriteTimeout,
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
            None    => ClientResponse::Deleted,
        }
    }

    async fn forward_to_primary(
        &self,
        key: String,
        value: Option<Bytes>,
        ttl_secs: Option<u64>,
    ) -> ClientResponse {
        let (primary_id, primary_addr) = {
            let set = self.active_set.lock().await;
            let primary_id = match set.primary_id() {
                Some(id) => id,
                None => return ClientResponse::Error {
                    code:    ErrorCode::NoQuorum,
                    message: "No primary elected".into(),
                },
            };
            let addr = set.all_nodes()
                .into_iter()
                .find(|n| n.id == primary_id)
                .map(|n| SocketAddr::new(n.addr.ip(), n.cluster_port));
            (primary_id, addr)
        };

        let addr = match primary_addr {
            Some(a) => a,
            None    => return ClientResponse::Error {
                code:    ErrorCode::NoQuorum,
                message: "Primary address unknown".into(),
            },
        };

        let req = if let Some(v) = value {
            ClientRequest::Set { key, value: v, ttl_secs }
        } else {
            ClientRequest::Delete { key }
        };

        let forward = ClusterMessage::Forward {
            request:     req,
            origin_node: self.id,
        };

        let resp = match send_cluster(addr, &forward, self.tls_connector.as_ref()).await {
            Ok(Some(ClusterMessage::ForwardResponse(r))) => r,
            Err(e) => ClientResponse::Error {
                code:    ErrorCode::InternalError,
                message: e.to_string(),
            },
            _ => ClientResponse::Error {
                code:    ErrorCode::InternalError,
                message: "No ForwardResponse from primary".into(),
            },
        };

        // If the supposed primary is actually inactive, update our local view immediately
        // so the next primary election will choose a different node without waiting for gossip.
        if matches!(&resp, ClientResponse::Error { code: ErrorCode::NodeInactive, .. }) {
            let mut set = self.active_set.lock().await;
            if let Some(mut info) = set.snapshot().into_iter().find(|n| n.id == primary_id) {
                info.status = NodeStatus::Inactive;
                set.upsert(info);
                info!("Primary {} reported NodeInactive; updated local view, triggering re-election", primary_id);
            }
        }

        resp
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
                let _ = self.watch_tx.send((key.to_string(), Some(stored_value), ver));
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

        let handles: Vec<_> = peers.iter().map(|&addr| {
            let msg = msg.clone();
            let tls = self.tls_connector.clone();
            tokio::spawn(async move {
                tokio::time::timeout(timeout, send_cluster(addr, &msg, tls.as_ref())).await
            })
        }).collect();

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
            ClusterMessage::Prepare { log_index, key, value, ttl_secs } => {
                self.write_log.lock().await.append_at(log_index, key, value, ttl_secs);
                Some(ClusterMessage::PrepareAck { log_index, node_id: self.id })
            }

            ClusterMessage::Commit { log_index } => {
                let entry_data = self.write_log.lock().await.get_entry(log_index);

                if let Some((key, value, ttl)) = entry_data {
                    self.apply_locally(&key, value, ttl, log_index);
                    self.write_log.lock().await.commit(log_index);
                }
                self.active_set.lock().await.set_local_applied(log_index);
                Some(ClusterMessage::CommitAck { log_index, node_id: self.id })
            }

            ClusterMessage::Forward { request, .. } => {
                let response = self.handle_client(request).await;
                Some(ClusterMessage::ForwardResponse(response))
            }

            ClusterMessage::RequestLog { from_index } => {
                let entries = self.write_log.lock().await.entries_since(from_index);
                Some(ClusterMessage::LogEntries { entries })
            }

            ClusterMessage::LogEntries { entries } => {
                for entry in entries {
                    self.apply_locally(&entry.key, entry.value.clone(), entry.ttl_secs, entry.index);
                    self.write_log.lock().await.commit(entry.index);
                    self.active_set.lock().await.set_local_applied(entry.index);
                }
                None
            }

            ClusterMessage::Synced { node_id, last_applied } => {
                let mut set = self.active_set.lock().await;
                if let Some(mut info) = set.snapshot().into_iter().find(|n| n.id == node_id) {
                    info.status       = NodeStatus::Active;
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
        let stats  = self.stats().await;
        let set    = self.active_set.lock().await;
        let cfg    = self.config.lock().unwrap();
        vec![
            // --- read-only ---
            ("id".into(),                   cfg.node.id.clone()),
            ("committed-index".into(),      stats.committed_index.to_string()),
            ("uptime".into(),               format!("{}s", stats.uptime_secs)),
            // --- read-write ---
            ("status".into(),               format!("{:?}", set.local_status())),
            ("primary".into(),              set.is_primary().to_string()),
            ("bind-addr".into(),            cfg.node.bind_addr.clone()),
            ("cluster-bind-addr".into(),    cfg.node.cluster_bind_addr.clone()),
            ("client-port".into(),          cfg.node.client_port.to_string()),
            ("http-port".into(),            cfg.node.http_port.to_string()),
            ("cluster-port".into(),         cfg.node.cluster_port.to_string()),
            ("gossip-port".into(),          cfg.node.gossip_port.to_string()),
            ("frame-read-timeout-ms".into(),cfg.node.frame_read_timeout_ms.to_string()),
            ("max-memory".into(),           format!("{}mb", cfg.cache.max_memory_mb)),
            ("default-ttl".into(),          format!("{}s", cfg.cache.default_ttl_secs)),
            ("value-size-limit".into(),     if stats.value_size_limit_bytes == 0 { "unlimited".into() } else { format!("{}b", stats.value_size_limit_bytes) }),
            ("max-keys".into(),             if stats.max_keys_limit == 0 { "unlimited".into() } else { stats.max_keys_limit.to_string() }),
            ("compression-enabled".into(),  stats.compression_enabled.to_string()),
            ("compression-threshold".into(),format!("{}b", stats.compression_threshold_bytes)),
            ("version-check-interval".into(), format!("{}ms", cfg.replication.version_check_interval_ms)),
            ("persistence-platform-allowed".into(), stats.persistence_platform_allowed.to_string()),
            ("persistence-runtime-enabled".into(), stats.persistence_runtime_enabled.to_string()),
            ("persistence-enabled".into(), stats.persistence_enabled.to_string()),
            ("persistence-backup-platform-allowed".into(), cfg.persistence.backup_allowed.to_string()),
            ("persistence-export-platform-allowed".into(), cfg.persistence.export_allowed.to_string()),
            ("persistence-import-platform-allowed".into(), cfg.persistence.import_allowed.to_string()),
            ("persistence-backup-enabled".into(), stats.persistence_backup_enabled.to_string()),
            ("persistence-export-enabled".into(), stats.persistence_export_enabled.to_string()),
            ("persistence-import-enabled".into(), stats.persistence_import_enabled.to_string()),
            ("rate-limit-enabled".into(), stats.rate_limit_enabled.to_string()),
            ("rate-limit-requests-per-sec".into(), cfg.rate_limit.requests_per_sec.to_string()),
            ("rate-limit-burst".into(), cfg.rate_limit.burst.to_string()),
            ("rate-limited-requests-total".into(), stats.rate_limited_requests_total.to_string()),
            ("circuit-breaker-enabled".into(), stats.circuit_breaker_enabled.to_string()),
            ("circuit-breaker-failure-threshold".into(), cfg.circuit_breaker.failure_threshold.to_string()),
            ("circuit-breaker-open-ms".into(), cfg.circuit_breaker.open_ms.to_string()),
            ("circuit-breaker-half-open-max-requests".into(), cfg.circuit_breaker.half_open_max_requests.to_string()),
            ("circuit-breaker-state".into(), stats.circuit_breaker_state.clone()),
            ("circuit-breaker-open-total".into(), stats.circuit_breaker_open_total.to_string()),
            ("circuit-breaker-reject-total".into(), stats.circuit_breaker_reject_total.to_string()),
        ]
    }

    async fn handle_admin(self: Arc<Self>, req: AdminRequest) -> AdminResponse {
        match req {
            AdminRequest::Describe => {
                AdminResponse::Properties(self.all_properties().await)
            }

            AdminRequest::GetStats => AdminResponse::Stats(self.stats().await),

            AdminRequest::ListKeys { pattern } => {
                AdminResponse::Keys(self.store.keys(pattern.as_deref()))
            }

            AdminRequest::GetKeyInfo { key } => {
                // Read the raw compressed flag before get() transparently decompresses.
                let compressed = self.store.is_compressed(&key).unwrap_or(false);
                match self.store.get(&key) {
                    Some(entry) => {
                        let ttl  = entry.ttl_remaining_secs();
                        let freq = entry.freq_count;
                        let ver  = entry.version;
                        AdminResponse::KeyInfo {
                            key,
                            value:              entry.value,
                            version:            ver,
                            ttl_remaining_secs: ttl,
                            freq_count:         freq,
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
                            Err(msg) => AdminResponse::Error { message: msg.to_string() },
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
                    Err(e) => AdminResponse::Error { message: e.to_string() },
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

                    // ── persistence runtime gate ───────────────────────────────
                    "persistence-runtime-enabled" | "persistence-enabled" => {
                        let val = value.trim().eq_ignore_ascii_case("true");
                        self.config.lock().unwrap().persistence.runtime_enabled = val;
                        tracing::info!("persistence-runtime-enabled → {}", val);
                    }

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
            let fallback_addr = set.all_nodes()
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
            peer_addr, our_index
        );

        // Set ourselves to Syncing so we are excluded from write quorum.
        self.active_set.lock().await.set_local_status(NodeStatus::Syncing);

        let req = ClusterMessage::RequestLog { from_index: our_index };
        match send_cluster(peer_addr, &req, self.tls_connector.as_ref()).await {
            Ok(Some(ClusterMessage::LogEntries { entries })) => {
                let count = entries.len();
                for entry in entries {
                    self.apply_locally(&entry.key, entry.value.clone(), entry.ttl_secs, entry.index);
                    self.write_log.lock().await.commit(entry.index);
                    self.active_set.lock().await.set_local_applied(entry.index);
                }
                tracing::info!("Recovery: applied {} entries.", count);
            }
            Err(e) => {
                tracing::error!(
                    "Recovery: failed to sync from {}: {}. Setting node Inactive.",
                    peer_addr, e
                );
                self.active.store(false, Ordering::Relaxed);
                self.active_set.lock().await.set_local_status(NodeStatus::Inactive);
                return;
            }
            _ => {}
        }

        // Re-join the active set.
        self.active_set.lock().await.set_local_status(NodeStatus::Active);
        let final_index = self.write_log.lock().await.committed_index();
        tracing::info!("Recovery complete. committed_index={}", final_index);

        // Announce to peers that we are synced.
        let synced_msg = ClusterMessage::Synced {
            node_id:      self.id,
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
        self.active_set.lock().await.set_local_status(NodeStatus::Syncing);

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
                    self.active_set.lock().await.set_local_status(NodeStatus::Active);
                    let final_index = self.write_log.lock().await.committed_index();
                    let synced_msg = ClusterMessage::Synced {
                        node_id:      self.id,
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
                    self.active_set.lock().await.set_local_status(NodeStatus::Inactive);
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
                self.active_set.lock().await.set_local_status(NodeStatus::Inactive);
                return;
            }
        };

        tracing::info!(
            "Resync: at index={}, fetching missed entries from primary {}.",
            our_index, peer_addr
        );

        let req = ClusterMessage::RequestLog { from_index: our_index };
        match send_cluster(peer_addr, &req, self.tls_connector.as_ref()).await {
            Ok(Some(ClusterMessage::LogEntries { entries })) => {
                let count = entries.len();
                for entry in entries {
                    self.apply_locally(&entry.key, entry.value.clone(), entry.ttl_secs, entry.index);
                    self.write_log.lock().await.commit(entry.index);
                    self.active_set.lock().await.set_local_applied(entry.index);
                }
                tracing::info!("Resync: applied {} entries.", count);
            }
            Err(e) => {
                tracing::warn!(
                    "Resync: failed to sync from primary {}: {}. Setting node Inactive.",
                    peer_addr, e
                );
                self.active.store(false, Ordering::Relaxed);
                self.active_set.lock().await.set_local_status(NodeStatus::Inactive);
                return;
            }
            _ => {}
        }

        // Success: rejoin the active set and allow client requests.
        self.active.store(true, Ordering::Relaxed);
        self.active_set.lock().await.set_local_status(NodeStatus::Active);
        let final_index = self.write_log.lock().await.committed_index();
        tracing::info!("Resync complete. Node is Active. committed_index={}", final_index);

        // Broadcast Synced so all peers update their view of us immediately.
        let synced_msg = ClusterMessage::Synced {
            node_id:      self.id,
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
                    safe_index, before
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
                let interval_ms = self.config.lock().unwrap().replication.version_check_interval_ms;
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
                    let our_index = set.all_nodes()
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
                    tracing::debug!(
                        "Version check: in sync (index={}).", our_index
                    );
                }
            }
        });
    }

    /// Collect a point-in-time [`NodeStats`] snapshot.
    ///
    /// Acquires short-lived locks on the store, active-set, write-log, and config.
    /// Used by both the admin `GetStats` RPC and the HTTP `/health` endpoint.
    pub async fn stats(&self) -> NodeStats {
        let s   = self.store.stats();
        let set = self.active_set.lock().await;
        let log = self.write_log.lock().await;
        let cfg = self.config.lock().unwrap();
        let backup_dir_bytes = dir_size_bytes(&cfg.backup.path);
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
        let circuit_breaker_state = if circuit_breaker_enabled {
            self.circuit.lock().unwrap().state.as_str().to_string()
        } else {
            "disabled".to_string()
        };
        NodeStats {
            node_id:           self.id,
            status:            set.local_status(),
            is_primary:        set.is_primary(),
            committed_index:   log.committed_index(),
            key_count:         s.key_count,
            memory_used_bytes: s.memory_used_bytes,
            memory_max_bytes:  s.memory_max_bytes,
            evictions:         s.evictions,
            hit_count:         s.hit_count,
            miss_count:        s.miss_count,
            uptime_secs:       self.started_at.elapsed().as_secs(),
            value_size_limit_bytes:      s.value_size_limit_bytes,
            max_keys_limit:              s.max_keys_limit,
            compression_enabled:         s.compression_enabled,
            compression_threshold_bytes: s.compression_threshold_bytes,
            node_name:        cfg.node.id.clone(),
            backup_dir_bytes,
            persistence_platform_allowed,
            persistence_runtime_enabled,
            persistence_enabled,
            persistence_backup_enabled,
            persistence_export_enabled,
            persistence_import_enabled,
            rate_limit_enabled,
            rate_limited_requests_total: self.rate_limited_requests_total.load(Ordering::Relaxed),
            circuit_breaker_enabled,
            circuit_breaker_state,
            circuit_breaker_open_total: self.circuit_breaker_open_total.load(Ordering::Relaxed),
            circuit_breaker_reject_total: self.circuit_breaker_reject_total.load(Ordering::Relaxed),
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
    use super::{NodeHandle, TokenBucket};
    use crate::config::Config;
    use std::time::Duration;

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
}
