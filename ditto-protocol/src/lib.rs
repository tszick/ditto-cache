//! Ditto wire protocol — shared types and framing helpers.
//!
//! All four protocol layers are defined here:
//! - **Client protocol** (port 7777 TCP / 7778 HTTP): `ClientRequest`, `ClientResponse`
//! - **Cluster protocol** (port 7779 TCP): `ClusterMessage`, `LogEntry`
//! - **Gossip protocol** (port 7780 UDP): `GossipMessage`
//! - **Admin protocol** (embedded in `ClusterMessage::Admin`): `AdminRequest`, `AdminResponse`
//!
//! Messages are serialised with `bincode` and framed with a 4-byte big-endian length prefix
//! using the [`encode`] / [`decode`] helpers.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Common
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: Uuid,
    pub addr: SocketAddr,
    pub cluster_port: u16,
    pub status: NodeStatus,
    pub last_applied: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Participating in writes and serving reads.
    Active,
    /// Online but catching up on missed log entries; no reads/writes.
    Syncing,
    /// Unreachable (gossip timeout).
    Offline,
    /// Manually deactivated via admin CLI; rejects client requests.
    Inactive,
}

// ---------------------------------------------------------------------------
// Client protocol  (port 7777 TCP  |  port 7778 HTTP)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientRequest {
    Get {
        key: String,
        namespace: Option<String>,
    },
    Set {
        key: String,
        value: Bytes,
        ttl_secs: Option<u64>,
        namespace: Option<String>,
    },
    Delete {
        key: String,
        namespace: Option<String>,
    },
    Ping,
    Auth {
        token: String,
    },
    // DITTO-02: pub/sub watch API (variants 5, 6)
    Watch {
        key: String,
        namespace: Option<String>,
    },
    Unwatch {
        key: String,
        namespace: Option<String>,
    },
    /// Delete all keys matching a glob-style pattern (`*` wildcard).
    DeleteByPattern {
        pattern: String,
        namespace: Option<String>,
    },
    /// Update TTL for all keys matching a glob-style pattern.
    /// `ttl_secs = None` removes TTL for matched keys.
    SetTtlByPattern {
        pattern: String,
        ttl_secs: Option<u64>,
        namespace: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientResponse {
    Value {
        key: String,
        value: Bytes,
        version: u64,
    },
    Ok {
        version: u64,
    },
    Deleted,
    NotFound,
    Pong,
    AuthOk,
    Error {
        code: ErrorCode,
        message: String,
    },
    // DITTO-02: pub/sub watch API (variants 7, 8, 9)
    /// Acknowledge to a Watch request.
    Watching,
    /// Acknowledge to an Unwatch request.
    Unwatched,
    /// Server-push: a watched key was committed (value = None means the key was deleted).
    WatchEvent {
        key: String,
        value: Option<Bytes>,
        version: u64,
    },
    /// Number of keys deleted by a pattern-based delete operation.
    PatternDeleted {
        deleted: usize,
    },
    /// Number of keys updated by a pattern-based TTL operation.
    PatternTtlUpdated {
        updated: usize,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ErrorCode {
    /// Node set inactive via admin CLI.
    NodeInactive,
    /// Not enough active nodes to form write quorum.
    NoQuorum,
    KeyNotFound,
    InternalError,
    /// Write timed out waiting for all active-set ACKs.
    WriteTimeout,
    /// Value exceeds the configured value-size-limit.
    ValueTooLarge,
    /// Cache is at the max-keys limit.
    KeyLimitReached,
    /// Request throttled by node-level rate limiter.
    RateLimited,
    /// Request rejected because circuit breaker is open.
    CircuitOpen,
    /// Request rejected by per-namespace quota.
    NamespaceQuotaExceeded,
    /// Invalid or missing authentication.
    AuthFailed,
}

// ---------------------------------------------------------------------------
// Cluster protocol  (port 7779 TCP)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterMessage {
    // --- Write path (two-phase) ---
    /// Primary → all active nodes: "prepare this entry".
    Prepare {
        log_index: u64,
        key: String,
        value: Option<Bytes>,
        ttl_secs: Option<u64>,
    },
    /// Follower → primary: prepare acknowledged.
    PrepareAck {
        log_index: u64,
        node_id: Uuid,
    },

    /// Primary → all active nodes: "commit, make visible".
    Commit {
        log_index: u64,
    },
    /// Follower → primary: commit acknowledged.
    CommitAck {
        log_index: u64,
        node_id: Uuid,
    },

    // --- Forwarding (non-primary receives client write) ---
    /// Non-primary → primary: forward a client write.
    Forward {
        request: ClientRequest,
        origin_node: Uuid,
    },

    // --- Recovery / anti-entropy ---
    /// Recovering node → any active node: "give me log entries".
    RequestLog {
        from_index: u64,
    },
    /// Active node → recovering node: log entries response.
    LogEntries {
        entries: Vec<LogEntry>,
    },
    /// Recovering node → gossip: "I'm fully synced".
    Synced {
        node_id: Uuid,
        last_applied: u64,
    },

    // --- Forward response (primary → follower after handling forwarded write) ---
    /// Primary → non-primary: result of a forwarded client write.
    ForwardResponse(ClientResponse),

    // --- Primary election override ---
    /// Broadcast by force-elected primary to all peers.
    ForcePrimary {
        node_id: Uuid,
    },

    // --- Admin (also on port 7779) ---
    Admin(AdminRequest),
    /// Node → admin client: response to an Admin request.
    AdminResponse(AdminResponse),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub index: u64,
    pub key: String,
    pub value: Option<Bytes>,
    pub ttl_secs: Option<u64>,
    pub ts_ms: u64,
}

// ---------------------------------------------------------------------------
// Gossip protocol  (port 7780 UDP)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipMessage {
    /// Periodic heartbeat broadcast.
    Heartbeat {
        node_id: Uuid,
        addr: SocketAddr,
        cluster_port: u16,
        status: NodeStatus,
        last_applied: u64,
    },
    /// Triggered when active-set membership changes.
    ActiveSetUpdate { active_nodes: Vec<NodeInfo> },
}

// ---------------------------------------------------------------------------
// Admin protocol  (embedded in ClusterMessage::Admin, same port 7779)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AdminRequest {
    Describe,
    GetProperty {
        name: String,
    },
    SetProperty {
        name: String,
        value: String,
    },
    ListKeys {
        pattern: Option<String>,
    },
    GetStats,
    /// Retrieve full info for a single key (value + TTL + freq + compressed).
    GetKeyInfo {
        key: String,
    },
    /// Set a per-key property (e.g. "compressed" = "true"/"false").
    SetKeyProperty {
        key: String,
        name: String,
        value: String,
    },
    FlushCache,
    ClusterStatus,
    /// Trigger an immediate backup on this node (inactivate → export → activate).
    BackupNow,
    RestoreLatestSnapshot,
    /// Set TTL for all keys matching a glob pattern. `ttl_secs = None` removes TTL.
    SetKeysTtl {
        pattern: String,
        ttl_secs: Option<u64>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AdminResponse {
    Properties(Vec<(String, String)>),
    Keys(Vec<String>),
    Stats(NodeStats),
    /// Response to GetKeyInfo.
    KeyInfo {
        key: String,
        value: Bytes,
        version: u64,
        ttl_remaining_secs: Option<u64>,
        freq_count: u64,
        /// Whether the stored value is LZ4-compressed.
        compressed: bool,
    },
    /// Response to SetKeyProperty.
    KeyPropertyUpdated,
    Flushed,
    ClusterView(Vec<NodeInfo>),
    /// Response to BackupNow: the path of the created backup file.
    BackupResult {
        path: String,
        bytes: u64,
    },
    RestoreResult {
        path: String,
        entries: u64,
        duration_ms: u64,
    },
    /// Response to SetKeysTtl: number of keys whose TTL was updated.
    TtlUpdated {
        updated: usize,
    },
    Ok,
    NotFound,
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceQuotaUsage {
    /// Namespace identifier.
    pub namespace: String,
    /// Current key count in the namespace.
    pub key_count: u64,
    /// Configured quota limit for a namespace.
    pub quota_limit: u64,
    /// Usage ratio in percent (0..=100+ when over quota).
    pub usage_pct: u64,
    /// Remaining key capacity before reaching quota.
    pub remaining_keys: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceLatencySummary {
    /// Namespace identifier.
    pub namespace: String,
    /// Requests observed in this namespace.
    pub request_total: u64,
    /// Estimated namespace latency p95 (ms) from request histogram.
    pub latency_p95_estimate_ms: Option<u64>,
    /// Estimated namespace latency p99 (ms) from request histogram.
    pub latency_p99_estimate_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotKeyUsage {
    /// Namespaced key label in the form `namespace::key`.
    pub key: String,
    /// Observed request count for this key.
    pub request_total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStats {
    pub node_id: Uuid,
    pub status: NodeStatus,
    pub is_primary: bool,
    pub committed_index: u64,
    pub key_count: u64,
    pub memory_used_bytes: u64,
    pub memory_max_bytes: u64,
    pub evictions: u64,
    pub hit_count: u64,
    pub miss_count: u64,
    pub uptime_secs: u64,
    /// Maximum allowed value size in bytes (0 = unlimited).
    pub value_size_limit_bytes: u64,
    /// Maximum number of keys allowed in the cache (0 = unlimited).
    pub max_keys_limit: u64,
    /// Whether automatic LZ4 compression is enabled.
    pub compression_enabled: bool,
    /// Minimum value size (bytes) to trigger automatic compression.
    pub compression_threshold_bytes: u64,
    /// Human-readable node name (DITTO_NODE_ID / cfg.node.id).
    pub node_name: String,
    /// Total bytes occupied by backup files in the configured backup directory.
    /// Computed at query time; 0 if the directory is empty or does not exist.
    pub backup_dir_bytes: u64,
    /// Last restored snapshot path (if startup restore has run successfully).
    pub snapshot_last_load_path: Option<String>,
    /// Duration of the last snapshot load in milliseconds.
    pub snapshot_last_load_duration_ms: u64,
    /// Number of entries restored by the last snapshot load.
    pub snapshot_last_load_entries: u64,
    /// Age of the last snapshot load in seconds.
    /// `None` when no snapshot has been restored in this runtime.
    pub snapshot_last_load_age_secs: Option<u64>,
    /// Total restore command attempts received.
    pub snapshot_restore_attempt_total: u64,
    /// Total successful restore executions.
    pub snapshot_restore_success_total: u64,
    /// Total restore failures (policy blocked + runtime restore errors).
    pub snapshot_restore_failure_total: u64,
    /// Total restore requests where no snapshot file was found.
    pub snapshot_restore_not_found_total: u64,
    /// Total restore requests rejected by persistence policy gate.
    pub snapshot_restore_policy_block_total: u64,
    /// Snapshot restore success ratio in percent.
    pub snapshot_restore_success_ratio_pct: u64,
    /// Platform-level persistence gate (env/config controlled).
    pub persistence_platform_allowed: bool,
    /// Runtime persistence gate (admin controlled).
    pub persistence_runtime_enabled: bool,
    /// Effective global persistence gate (`platform && runtime`).
    pub persistence_enabled: bool,
    /// Effective backup gate (`platform && runtime && backup_allowed`).
    pub persistence_backup_enabled: bool,
    /// Effective export gate (`platform && runtime && export_allowed`).
    pub persistence_export_enabled: bool,
    /// Effective import gate (`platform && runtime && import_allowed`).
    pub persistence_import_enabled: bool,
    /// Namespace isolation feature enabled.
    pub tenancy_enabled: bool,
    /// Default namespace for requests without explicit namespace.
    pub tenancy_default_namespace: String,
    /// Per-namespace key limit (0 = unlimited).
    pub tenancy_max_keys_per_namespace: usize,
    /// Node-level request limiter enabled.
    pub rate_limit_enabled: bool,
    /// Total client requests rejected by rate limiting since startup.
    pub rate_limited_requests_total: u64,
    /// Circuit breaker enabled.
    pub circuit_breaker_enabled: bool,
    /// Hot-key GET coalescing enabled.
    pub hot_key_enabled: bool,
    /// Adaptive per-key waiter limit tuning enabled.
    pub hot_key_adaptive_waiters_enabled: bool,
    /// Read-repair on local miss enabled.
    pub read_repair_enabled: bool,
    /// Total GET requests served via in-flight coalescing wait path.
    pub hot_key_coalesced_hits_total: u64,
    /// Total GET requests that bypassed coalescing (waiter cap / fallback).
    pub hot_key_fallback_exec_total: u64,
    /// Total follower waits that timed out before leader response.
    pub hot_key_wait_timeout_total: u64,
    /// Total responses served from soft-stale cache on fallback paths.
    pub hot_key_stale_served_total: u64,
    /// Number of keys currently tracked as in-flight single-flight entries.
    pub hot_key_inflight_keys: u64,
    /// Number of entries currently retained in soft-stale cache.
    pub hot_key_stale_cache_entries: u64,
    /// Number of keys with adaptive waiter state retained.
    pub hot_key_adaptive_state_keys: u64,
    /// Number of adaptive waiter limit upward adjustments.
    pub hot_key_adaptive_limit_increase_total: u64,
    /// Number of adaptive waiter limit downward adjustments.
    pub hot_key_adaptive_limit_decrease_total: u64,
    /// Total read-repair attempts triggered on local misses.
    pub read_repair_trigger_total: u64,
    /// Total read-repair attempts where primary had the value.
    pub read_repair_success_total: u64,
    /// Total read-repair attempts skipped due to min-interval throttling.
    pub read_repair_throttled_total: u64,
    /// Total read-repair attempts skipped due to per-minute budget exhaustion.
    pub read_repair_budget_exhausted_total: u64,
    /// Total client requests rejected due to per-namespace quota.
    pub namespace_quota_reject_total: u64,
    /// Recent quota reject velocity (events/minute) between two stats snapshots.
    pub namespace_quota_reject_rate_per_min: u64,
    /// Recent quota reject trend label: `steady` | `rising` | `surging`.
    pub namespace_quota_reject_trend: String,
    /// Highest namespace quota pressure entries sorted by usage descending.
    pub namespace_quota_top_usage: Vec<NamespaceQuotaUsage>,
    /// Top namespaces by request volume with estimated latency percentiles.
    pub namespace_latency_top: Vec<NamespaceLatencySummary>,
    /// Top hot keys by request volume.
    pub hot_key_top_usage: Vec<HotKeyUsage>,
    /// Total anti-entropy loop iterations.
    pub anti_entropy_runs_total: u64,
    /// Total anti-entropy triggered repair attempts.
    pub anti_entropy_repair_trigger_total: u64,
    /// Total anti-entropy repair attempts skipped due to min-interval throttling.
    pub anti_entropy_repair_throttled_total: u64,
    /// Last lag value detected by anti-entropy (in log entries).
    pub anti_entropy_last_detected_lag: u64,
    /// Total key-version comparisons performed by anti-entropy sampling.
    pub anti_entropy_key_checks_total: u64,
    /// Total sampled key-version mismatches detected by anti-entropy.
    pub anti_entropy_key_mismatch_total: u64,
    /// Total bounded full keyspace reconcile runs.
    pub anti_entropy_full_reconcile_runs_total: u64,
    /// Total key comparisons performed by full keyspace reconcile.
    pub anti_entropy_full_reconcile_key_checks_total: u64,
    /// Total mismatches detected by full keyspace reconcile.
    pub anti_entropy_full_reconcile_mismatch_total: u64,
    /// Total anti-entropy checks that stopped early due to budget exhaustion.
    pub anti_entropy_budget_exhausted_total: u64,
    /// Total mixed-version probe loop iterations.
    pub mixed_version_probe_runs_total: u64,
    /// Cumulative count of peers detected with a mismatching protocol version.
    pub mixed_version_peers_detected_total: u64,
    /// Total mixed-version probe errors (RPC/property failures).
    pub mixed_version_probe_errors_total: u64,
    /// Last observed number of mismatching peers.
    pub mixed_version_last_detected_peer_count: u64,
    /// Current circuit breaker state: "closed" | "open" | "half-open".
    pub circuit_breaker_state: String,
    /// Number of times the circuit transitioned to open.
    pub circuit_breaker_open_total: u64,
    /// Total client requests rejected because circuit is open/limited in half-open.
    pub circuit_breaker_reject_total: u64,
    /// Total client requests observed by the node request handler.
    pub client_requests_total: u64,
    /// Client request counts split by ingress path.
    pub client_requests_tcp_total: u64,
    pub client_requests_http_total: u64,
    pub client_requests_internal_total: u64,
    /// Request latency bucket counters.
    pub client_request_latency_le_1ms_total: u64,
    pub client_request_latency_le_5ms_total: u64,
    pub client_request_latency_le_20ms_total: u64,
    pub client_request_latency_le_100ms_total: u64,
    pub client_request_latency_le_500ms_total: u64,
    pub client_request_latency_gt_500ms_total: u64,
    /// Approximate latency percentiles derived from histogram buckets.
    pub client_latency_p50_estimate_ms: Option<u64>,
    pub client_latency_p90_estimate_ms: Option<u64>,
    pub client_latency_p95_estimate_ms: Option<u64>,
    pub client_latency_p99_estimate_ms: Option<u64>,
    /// Total client error responses.
    pub client_error_total: u64,
    /// Client error counts split by ingress path.
    pub client_errors_tcp_total: u64,
    pub client_errors_http_total: u64,
    pub client_errors_internal_total: u64,
    /// Client error category counters.
    pub client_error_auth_total: u64,
    pub client_error_throttle_total: u64,
    pub client_error_availability_total: u64,
    pub client_error_validation_total: u64,
    pub client_error_internal_total: u64,
    pub client_error_other_total: u64,
}

// ---------------------------------------------------------------------------
// Wire framing helpers
// ---------------------------------------------------------------------------

/// Encode any serialisable message to length-prefixed bytes (4-byte BE length).
pub fn encode<T: Serialize>(msg: &T) -> anyhow::Result<Vec<u8>> {
    use bincode::Options;
    let payload = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .serialize(msg)?;
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Decode a length-prefixed message from a byte slice with limits.
pub fn decode<T: for<'de> Deserialize<'de>>(buf: &[u8], max_size: u64) -> anyhow::Result<T> {
    use bincode::Options;
    let options = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(max_size)
        .allow_trailing_bytes();
    Ok(options.deserialize(buf)?)
}
