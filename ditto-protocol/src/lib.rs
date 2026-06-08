//! Ditto wire protocol — shared types and framing helpers.
//!
//! All four protocol layers are defined here:
//! - **Client protocol** (port 7777 TCP / 7778 HTTP): `ClientRequest`, `ClientResponse`
//! - **Cluster protocol** (port 7779 TCP): `ClusterMessage`, `LogEntry`
//! - **Gossip protocol** (port 7780 UDP): `GossipMessage`
//! - **Admin protocol** (embedded in `ClusterMessage::Admin`): `AdminRequest`, `AdminResponse`
//!
//! Messages are serialised into a prost envelope and framed with a 4-byte big-endian
//! length prefix using the [`encode`] / [`decode`] helpers.

use bytes::Bytes;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use uuid::Uuid;

#[allow(clippy::large_enum_variant)]
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/ditto.protocol.v1.rs"));
}

const PROTOCOL_VERSION: u32 = 1;

pub trait WireMessage: Sized {
    fn into_payload(self) -> pb::envelope::Payload;
    fn from_payload(payload: pb::envelope::Payload) -> anyhow::Result<Self>;
}

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
    SetNx {
        key: String,
        value: Bytes,
        ttl_secs: Option<u64>,
        namespace: Option<String>,
    },
    Incr {
        key: String,
        delta: i64,
        ttl_secs_on_create: Option<u64>,
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

impl WireMessage for ClientRequest {
    fn into_payload(self) -> pb::envelope::Payload {
        pb::envelope::Payload::ClientRequest(self.into())
    }

    fn from_payload(payload: pb::envelope::Payload) -> anyhow::Result<Self> {
        match payload {
            pb::envelope::Payload::ClientRequest(msg) => msg.try_into(),
            _ => anyhow::bail!("unexpected protobuf payload for ClientRequest"),
        }
    }
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
    SetNx {
        created: bool,
        version: u64,
    },
    Counter {
        value: i64,
        version: u64,
    },
}

impl WireMessage for ClientResponse {
    fn into_payload(self) -> pb::envelope::Payload {
        pb::envelope::Payload::ClientResponse(self.into())
    }

    fn from_payload(payload: pb::envelope::Payload) -> anyhow::Result<Self> {
        match payload {
            pb::envelope::Payload::ClientResponse(msg) => msg.try_into(),
            _ => anyhow::bail!("unexpected protobuf payload for ClientResponse"),
        }
    }
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
    /// Request opcode is not implemented by this server version.
    UnsupportedRequest,
    /// Operation requires a different stored value type.
    TypeMismatch,
    /// Integer operation overflowed or underflowed.
    Overflow,
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
    AdminResponse(Box<AdminResponse>),
}

impl WireMessage for ClusterMessage {
    fn into_payload(self) -> pb::envelope::Payload {
        pb::envelope::Payload::ClusterMessage(self.into())
    }

    fn from_payload(payload: pb::envelope::Payload) -> anyhow::Result<Self> {
        match payload {
            pb::envelope::Payload::ClusterMessage(msg) => msg.try_into(),
            _ => anyhow::bail!("unexpected protobuf payload for ClusterMessage"),
        }
    }
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

impl WireMessage for GossipMessage {
    fn into_payload(self) -> pb::envelope::Payload {
        pb::envelope::Payload::GossipMessage(self.into())
    }

    fn from_payload(payload: pb::envelope::Payload) -> anyhow::Result<Self> {
        match payload {
            pb::envelope::Payload::GossipMessage(msg) => msg.try_into(),
            _ => anyhow::bail!("unexpected protobuf payload for GossipMessage"),
        }
    }
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
    Stats(Box<NodeStats>),
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

fn opt_string(value: Option<String>) -> Option<pb::OptionalString> {
    value.map(|value| pb::OptionalString { value })
}

fn from_opt_string(value: Option<pb::OptionalString>) -> Option<String> {
    value.map(|value| value.value)
}

fn opt_u64(value: Option<u64>) -> Option<pb::OptionalUint64> {
    value.map(|value| pb::OptionalUint64 { value })
}

fn from_opt_u64(value: Option<pb::OptionalUint64>) -> Option<u64> {
    value.map(|value| value.value)
}

fn opt_bytes(value: Option<Bytes>) -> Option<pb::OptionalBytes> {
    value.map(|value| pb::OptionalBytes {
        value: value.to_vec(),
    })
}

fn from_opt_bytes(value: Option<pb::OptionalBytes>) -> Option<Bytes> {
    value.map(|value| Bytes::from(value.value))
}

fn uuid_to_string(value: Uuid) -> String {
    value.to_string()
}

fn uuid_from_string(value: String) -> anyhow::Result<Uuid> {
    Ok(Uuid::parse_str(&value)?)
}

fn socket_to_string(value: SocketAddr) -> String {
    value.to_string()
}

fn socket_from_string(value: String) -> anyhow::Result<SocketAddr> {
    Ok(value.parse()?)
}

impl From<NodeStatus> for pb::NodeStatus {
    fn from(value: NodeStatus) -> Self {
        match value {
            NodeStatus::Active => Self::Active,
            NodeStatus::Syncing => Self::Syncing,
            NodeStatus::Offline => Self::Offline,
            NodeStatus::Inactive => Self::Inactive,
        }
    }
}

impl TryFrom<i32> for NodeStatus {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> anyhow::Result<Self> {
        match pb::NodeStatus::try_from(value)? {
            pb::NodeStatus::Active => Ok(Self::Active),
            pb::NodeStatus::Syncing => Ok(Self::Syncing),
            pb::NodeStatus::Offline => Ok(Self::Offline),
            pb::NodeStatus::Inactive => Ok(Self::Inactive),
        }
    }
}

impl From<ErrorCode> for pb::ErrorCode {
    fn from(value: ErrorCode) -> Self {
        match value {
            ErrorCode::NodeInactive => Self::NodeInactive,
            ErrorCode::NoQuorum => Self::NoQuorum,
            ErrorCode::KeyNotFound => Self::KeyNotFound,
            ErrorCode::InternalError => Self::InternalError,
            ErrorCode::WriteTimeout => Self::WriteTimeout,
            ErrorCode::ValueTooLarge => Self::ValueTooLarge,
            ErrorCode::KeyLimitReached => Self::KeyLimitReached,
            ErrorCode::RateLimited => Self::RateLimited,
            ErrorCode::CircuitOpen => Self::CircuitOpen,
            ErrorCode::NamespaceQuotaExceeded => Self::NamespaceQuotaExceeded,
            ErrorCode::AuthFailed => Self::AuthFailed,
            ErrorCode::UnsupportedRequest => Self::UnsupportedRequest,
            ErrorCode::TypeMismatch => Self::TypeMismatch,
            ErrorCode::Overflow => Self::Overflow,
        }
    }
}

impl TryFrom<i32> for ErrorCode {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> anyhow::Result<Self> {
        match pb::ErrorCode::try_from(value)? {
            pb::ErrorCode::NodeInactive => Ok(Self::NodeInactive),
            pb::ErrorCode::NoQuorum => Ok(Self::NoQuorum),
            pb::ErrorCode::KeyNotFound => Ok(Self::KeyNotFound),
            pb::ErrorCode::InternalError => Ok(Self::InternalError),
            pb::ErrorCode::WriteTimeout => Ok(Self::WriteTimeout),
            pb::ErrorCode::ValueTooLarge => Ok(Self::ValueTooLarge),
            pb::ErrorCode::KeyLimitReached => Ok(Self::KeyLimitReached),
            pb::ErrorCode::RateLimited => Ok(Self::RateLimited),
            pb::ErrorCode::CircuitOpen => Ok(Self::CircuitOpen),
            pb::ErrorCode::NamespaceQuotaExceeded => Ok(Self::NamespaceQuotaExceeded),
            pb::ErrorCode::AuthFailed => Ok(Self::AuthFailed),
            pb::ErrorCode::UnsupportedRequest => Ok(Self::UnsupportedRequest),
            pb::ErrorCode::TypeMismatch => Ok(Self::TypeMismatch),
            pb::ErrorCode::Overflow => Ok(Self::Overflow),
        }
    }
}

impl From<NodeInfo> for pb::NodeInfo {
    fn from(value: NodeInfo) -> Self {
        Self {
            id: uuid_to_string(value.id),
            addr: socket_to_string(value.addr),
            cluster_port: value.cluster_port as u32,
            status: pb::NodeStatus::from(value.status) as i32,
            last_applied: value.last_applied,
        }
    }
}

impl TryFrom<pb::NodeInfo> for NodeInfo {
    type Error = anyhow::Error;

    fn try_from(value: pb::NodeInfo) -> anyhow::Result<Self> {
        Ok(Self {
            id: uuid_from_string(value.id)?,
            addr: socket_from_string(value.addr)?,
            cluster_port: u16::try_from(value.cluster_port)?,
            status: value.status.try_into()?,
            last_applied: value.last_applied,
        })
    }
}

impl From<LogEntry> for pb::LogEntry {
    fn from(value: LogEntry) -> Self {
        Self {
            index: value.index,
            key: value.key,
            value: opt_bytes(value.value),
            ttl_secs: opt_u64(value.ttl_secs),
            ts_ms: value.ts_ms,
        }
    }
}

impl TryFrom<pb::LogEntry> for LogEntry {
    type Error = anyhow::Error;

    fn try_from(value: pb::LogEntry) -> anyhow::Result<Self> {
        Ok(Self {
            index: value.index,
            key: value.key,
            value: from_opt_bytes(value.value),
            ttl_secs: from_opt_u64(value.ttl_secs),
            ts_ms: value.ts_ms,
        })
    }
}

impl From<ClientRequest> for pb::ClientRequest {
    fn from(value: ClientRequest) -> Self {
        use pb::client_request::Request;
        let request = match value {
            ClientRequest::Get { key, namespace } => Request::Get(pb::KeyNamespace {
                key,
                namespace: opt_string(namespace),
            }),
            ClientRequest::Set {
                key,
                value,
                ttl_secs,
                namespace,
            } => Request::Set(pb::SetRequest {
                key,
                value: value.to_vec(),
                ttl_secs: opt_u64(ttl_secs),
                namespace: opt_string(namespace),
            }),
            ClientRequest::Delete { key, namespace } => Request::Delete(pb::KeyNamespace {
                key,
                namespace: opt_string(namespace),
            }),
            ClientRequest::Ping => Request::Ping(pb::Empty {}),
            ClientRequest::Auth { token } => Request::Auth(pb::AuthRequest { token }),
            ClientRequest::Watch { key, namespace } => Request::Watch(pb::KeyNamespace {
                key,
                namespace: opt_string(namespace),
            }),
            ClientRequest::Unwatch { key, namespace } => Request::Unwatch(pb::KeyNamespace {
                key,
                namespace: opt_string(namespace),
            }),
            ClientRequest::DeleteByPattern { pattern, namespace } => {
                Request::DeleteByPattern(pb::PatternNamespace {
                    pattern,
                    namespace: opt_string(namespace),
                })
            }
            ClientRequest::SetNx {
                key,
                value,
                ttl_secs,
                namespace,
            } => Request::SetNx(pb::SetRequest {
                key,
                value: value.to_vec(),
                ttl_secs: opt_u64(ttl_secs),
                namespace: opt_string(namespace),
            }),
            ClientRequest::Incr {
                key,
                delta,
                ttl_secs_on_create,
                namespace,
            } => Request::Incr(pb::IncrRequest {
                key,
                delta: Some(delta),
                ttl_secs_on_create: opt_u64(ttl_secs_on_create),
                namespace: opt_string(namespace),
            }),
            ClientRequest::SetTtlByPattern {
                pattern,
                ttl_secs,
                namespace,
            } => Request::SetTtlByPattern(pb::SetTtlByPatternRequest {
                pattern,
                ttl_secs: opt_u64(ttl_secs),
                namespace: opt_string(namespace),
            }),
        };
        Self {
            request: Some(request),
        }
    }
}

impl TryFrom<pb::ClientRequest> for ClientRequest {
    type Error = anyhow::Error;

    fn try_from(value: pb::ClientRequest) -> anyhow::Result<Self> {
        use pb::client_request::Request;
        let request = value
            .request
            .ok_or_else(|| anyhow::anyhow!("missing client request payload"))?;
        Ok(match request {
            Request::Get(v) => Self::Get {
                key: v.key,
                namespace: from_opt_string(v.namespace),
            },
            Request::Set(v) => Self::Set {
                key: v.key,
                value: Bytes::from(v.value),
                ttl_secs: from_opt_u64(v.ttl_secs),
                namespace: from_opt_string(v.namespace),
            },
            Request::Delete(v) => Self::Delete {
                key: v.key,
                namespace: from_opt_string(v.namespace),
            },
            Request::Ping(_) => Self::Ping,
            Request::Auth(v) => Self::Auth { token: v.token },
            Request::Watch(v) => Self::Watch {
                key: v.key,
                namespace: from_opt_string(v.namespace),
            },
            Request::Unwatch(v) => Self::Unwatch {
                key: v.key,
                namespace: from_opt_string(v.namespace),
            },
            Request::DeleteByPattern(v) => Self::DeleteByPattern {
                pattern: v.pattern,
                namespace: from_opt_string(v.namespace),
            },
            Request::SetNx(v) => Self::SetNx {
                key: v.key,
                value: Bytes::from(v.value),
                ttl_secs: from_opt_u64(v.ttl_secs),
                namespace: from_opt_string(v.namespace),
            },
            Request::Incr(v) => Self::Incr {
                key: v.key,
                delta: v.delta.unwrap_or(1),
                ttl_secs_on_create: from_opt_u64(v.ttl_secs_on_create),
                namespace: from_opt_string(v.namespace),
            },
            Request::SetTtlByPattern(v) => Self::SetTtlByPattern {
                pattern: v.pattern,
                ttl_secs: from_opt_u64(v.ttl_secs),
                namespace: from_opt_string(v.namespace),
            },
        })
    }
}

impl From<ClientResponse> for pb::ClientResponse {
    fn from(value: ClientResponse) -> Self {
        use pb::client_response::Response;
        let response = match value {
            ClientResponse::Value {
                key,
                value,
                version,
            } => Response::Value(pb::ValueResponse {
                key,
                value: value.to_vec(),
                version,
            }),
            ClientResponse::Ok { version } => Response::Ok(pb::VersionResponse { version }),
            ClientResponse::Deleted => Response::Deleted(pb::Empty {}),
            ClientResponse::NotFound => Response::NotFound(pb::Empty {}),
            ClientResponse::Pong => Response::Pong(pb::Empty {}),
            ClientResponse::AuthOk => Response::AuthOk(pb::Empty {}),
            ClientResponse::Error { code, message } => Response::Error(pb::ErrorResponse {
                code: pb::ErrorCode::from(code) as i32,
                message,
            }),
            ClientResponse::Watching => Response::Watching(pb::Empty {}),
            ClientResponse::Unwatched => Response::Unwatched(pb::Empty {}),
            ClientResponse::WatchEvent {
                key,
                value,
                version,
            } => Response::WatchEvent(pb::WatchEvent {
                key,
                value: opt_bytes(value),
                version,
            }),
            ClientResponse::PatternDeleted { deleted } => {
                Response::PatternDeleted(pb::CountResponse {
                    count: deleted as u64,
                })
            }
            ClientResponse::PatternTtlUpdated { updated } => {
                Response::PatternTtlUpdated(pb::CountResponse {
                    count: updated as u64,
                })
            }
            ClientResponse::SetNx { created, version } => {
                Response::SetNx(pb::SetNxResponse { created, version })
            }
            ClientResponse::Counter { value, version } => {
                Response::Counter(pb::CounterResponse { value, version })
            }
        };
        Self {
            response: Some(response),
        }
    }
}

impl TryFrom<pb::ClientResponse> for ClientResponse {
    type Error = anyhow::Error;

    fn try_from(value: pb::ClientResponse) -> anyhow::Result<Self> {
        use pb::client_response::Response;
        let response = value
            .response
            .ok_or_else(|| anyhow::anyhow!("missing client response payload"))?;
        Ok(match response {
            Response::Value(v) => Self::Value {
                key: v.key,
                value: Bytes::from(v.value),
                version: v.version,
            },
            Response::Ok(v) => Self::Ok { version: v.version },
            Response::Deleted(_) => Self::Deleted,
            Response::NotFound(_) => Self::NotFound,
            Response::Pong(_) => Self::Pong,
            Response::AuthOk(_) => Self::AuthOk,
            Response::Error(v) => Self::Error {
                code: v.code.try_into()?,
                message: v.message,
            },
            Response::Watching(_) => Self::Watching,
            Response::Unwatched(_) => Self::Unwatched,
            Response::WatchEvent(v) => Self::WatchEvent {
                key: v.key,
                value: from_opt_bytes(v.value),
                version: v.version,
            },
            Response::PatternDeleted(v) => Self::PatternDeleted {
                deleted: usize::try_from(v.count)?,
            },
            Response::PatternTtlUpdated(v) => Self::PatternTtlUpdated {
                updated: usize::try_from(v.count)?,
            },
            Response::SetNx(v) => Self::SetNx {
                created: v.created,
                version: v.version,
            },
            Response::Counter(v) => Self::Counter {
                value: v.value,
                version: v.version,
            },
        })
    }
}

impl From<AdminRequest> for pb::AdminRequest {
    fn from(value: AdminRequest) -> Self {
        use pb::admin_request::Request;
        let request = match value {
            AdminRequest::Describe => Request::Describe(pb::Empty {}),
            AdminRequest::GetProperty { name } => Request::GetProperty(pb::Named { name }),
            AdminRequest::SetProperty { name, value } => {
                Request::SetProperty(pb::PropertyUpdate { name, value })
            }
            AdminRequest::ListKeys { pattern } => Request::ListKeys(pb::ListKeys {
                pattern: opt_string(pattern),
            }),
            AdminRequest::GetStats => Request::GetStats(pb::Empty {}),
            AdminRequest::GetKeyInfo { key } => Request::GetKeyInfo(pb::KeyOnly { key }),
            AdminRequest::SetKeyProperty { key, name, value } => {
                Request::SetKeyProperty(pb::KeyPropertyUpdate { key, name, value })
            }
            AdminRequest::FlushCache => Request::FlushCache(pb::Empty {}),
            AdminRequest::ClusterStatus => Request::ClusterStatus(pb::Empty {}),
            AdminRequest::BackupNow => Request::BackupNow(pb::Empty {}),
            AdminRequest::RestoreLatestSnapshot => Request::RestoreLatestSnapshot(pb::Empty {}),
            AdminRequest::SetKeysTtl { pattern, ttl_secs } => Request::SetKeysTtl(pb::SetKeysTtl {
                pattern,
                ttl_secs: opt_u64(ttl_secs),
            }),
        };
        Self {
            request: Some(request),
        }
    }
}

impl TryFrom<pb::AdminRequest> for AdminRequest {
    type Error = anyhow::Error;

    fn try_from(value: pb::AdminRequest) -> anyhow::Result<Self> {
        use pb::admin_request::Request;
        let request = value
            .request
            .ok_or_else(|| anyhow::anyhow!("missing admin request payload"))?;
        Ok(match request {
            Request::Describe(_) => Self::Describe,
            Request::GetProperty(v) => Self::GetProperty { name: v.name },
            Request::SetProperty(v) => Self::SetProperty {
                name: v.name,
                value: v.value,
            },
            Request::ListKeys(v) => Self::ListKeys {
                pattern: from_opt_string(v.pattern),
            },
            Request::GetStats(_) => Self::GetStats,
            Request::GetKeyInfo(v) => Self::GetKeyInfo { key: v.key },
            Request::SetKeyProperty(v) => Self::SetKeyProperty {
                key: v.key,
                name: v.name,
                value: v.value,
            },
            Request::FlushCache(_) => Self::FlushCache,
            Request::ClusterStatus(_) => Self::ClusterStatus,
            Request::BackupNow(_) => Self::BackupNow,
            Request::RestoreLatestSnapshot(_) => Self::RestoreLatestSnapshot,
            Request::SetKeysTtl(v) => Self::SetKeysTtl {
                pattern: v.pattern,
                ttl_secs: from_opt_u64(v.ttl_secs),
            },
        })
    }
}

impl From<ClusterMessage> for pb::ClusterMessage {
    fn from(value: ClusterMessage) -> Self {
        use pb::cluster_message::Message;
        let message = match value {
            ClusterMessage::Prepare {
                log_index,
                key,
                value,
                ttl_secs,
            } => Message::Prepare(pb::Prepare {
                log_index,
                key,
                value: opt_bytes(value),
                ttl_secs: opt_u64(ttl_secs),
            }),
            ClusterMessage::PrepareAck { log_index, node_id } => Message::PrepareAck(pb::LogAck {
                log_index,
                node_id: uuid_to_string(node_id),
            }),
            ClusterMessage::Commit { log_index } => Message::Commit(pb::Commit { log_index }),
            ClusterMessage::CommitAck { log_index, node_id } => Message::CommitAck(pb::LogAck {
                log_index,
                node_id: uuid_to_string(node_id),
            }),
            ClusterMessage::Forward {
                request,
                origin_node,
            } => Message::Forward(pb::Forward {
                request: Some(request.into()),
                origin_node: uuid_to_string(origin_node),
            }),
            ClusterMessage::RequestLog { from_index } => {
                Message::RequestLog(pb::RequestLog { from_index })
            }
            ClusterMessage::LogEntries { entries } => Message::LogEntries(pb::LogEntries {
                entries: entries.into_iter().map(Into::into).collect(),
            }),
            ClusterMessage::Synced {
                node_id,
                last_applied,
            } => Message::Synced(pb::Synced {
                node_id: uuid_to_string(node_id),
                last_applied,
            }),
            ClusterMessage::ForwardResponse(resp) => Message::ForwardResponse(resp.into()),
            ClusterMessage::ForcePrimary { node_id } => Message::ForcePrimary(pb::ForcePrimary {
                node_id: uuid_to_string(node_id),
            }),
            ClusterMessage::Admin(req) => Message::Admin(req.into()),
            ClusterMessage::AdminResponse(resp) => Message::AdminResponse((*resp).into()),
        };
        Self {
            message: Some(message),
        }
    }
}

impl TryFrom<pb::ClusterMessage> for ClusterMessage {
    type Error = anyhow::Error;

    fn try_from(value: pb::ClusterMessage) -> anyhow::Result<Self> {
        use pb::cluster_message::Message;
        let message = value
            .message
            .ok_or_else(|| anyhow::anyhow!("missing cluster message payload"))?;
        Ok(match message {
            Message::Prepare(v) => Self::Prepare {
                log_index: v.log_index,
                key: v.key,
                value: from_opt_bytes(v.value),
                ttl_secs: from_opt_u64(v.ttl_secs),
            },
            Message::PrepareAck(v) => Self::PrepareAck {
                log_index: v.log_index,
                node_id: uuid_from_string(v.node_id)?,
            },
            Message::Commit(v) => Self::Commit {
                log_index: v.log_index,
            },
            Message::CommitAck(v) => Self::CommitAck {
                log_index: v.log_index,
                node_id: uuid_from_string(v.node_id)?,
            },
            Message::Forward(v) => Self::Forward {
                request: v
                    .request
                    .ok_or_else(|| anyhow::anyhow!("missing forwarded request"))?
                    .try_into()?,
                origin_node: uuid_from_string(v.origin_node)?,
            },
            Message::RequestLog(v) => Self::RequestLog {
                from_index: v.from_index,
            },
            Message::LogEntries(v) => Self::LogEntries {
                entries: v
                    .entries
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<anyhow::Result<Vec<_>>>()?,
            },
            Message::Synced(v) => Self::Synced {
                node_id: uuid_from_string(v.node_id)?,
                last_applied: v.last_applied,
            },
            Message::ForwardResponse(v) => Self::ForwardResponse(v.try_into()?),
            Message::ForcePrimary(v) => Self::ForcePrimary {
                node_id: uuid_from_string(v.node_id)?,
            },
            Message::Admin(v) => Self::Admin(v.try_into()?),
            Message::AdminResponse(v) => Self::AdminResponse(Box::new(v.try_into()?)),
        })
    }
}

impl From<GossipMessage> for pb::GossipMessage {
    fn from(value: GossipMessage) -> Self {
        use pb::gossip_message::Message;
        let message = match value {
            GossipMessage::Heartbeat {
                node_id,
                addr,
                cluster_port,
                status,
                last_applied,
            } => Message::Heartbeat(pb::Heartbeat {
                node_id: uuid_to_string(node_id),
                addr: socket_to_string(addr),
                cluster_port: cluster_port as u32,
                status: pb::NodeStatus::from(status) as i32,
                last_applied,
            }),
            GossipMessage::ActiveSetUpdate { active_nodes } => {
                Message::ActiveSetUpdate(pb::ActiveSetUpdate {
                    active_nodes: active_nodes.into_iter().map(Into::into).collect(),
                })
            }
        };
        Self {
            message: Some(message),
        }
    }
}

impl TryFrom<pb::GossipMessage> for GossipMessage {
    type Error = anyhow::Error;

    fn try_from(value: pb::GossipMessage) -> anyhow::Result<Self> {
        use pb::gossip_message::Message;
        let message = value
            .message
            .ok_or_else(|| anyhow::anyhow!("missing gossip message payload"))?;
        Ok(match message {
            Message::Heartbeat(v) => Self::Heartbeat {
                node_id: uuid_from_string(v.node_id)?,
                addr: socket_from_string(v.addr)?,
                cluster_port: u16::try_from(v.cluster_port)?,
                status: v.status.try_into()?,
                last_applied: v.last_applied,
            },
            Message::ActiveSetUpdate(v) => Self::ActiveSetUpdate {
                active_nodes: v
                    .active_nodes
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<anyhow::Result<Vec<_>>>()?,
            },
        })
    }
}

impl From<NamespaceQuotaUsage> for pb::NamespaceQuotaUsage {
    fn from(value: NamespaceQuotaUsage) -> Self {
        Self {
            namespace: value.namespace,
            key_count: value.key_count,
            quota_limit: value.quota_limit,
            usage_pct: value.usage_pct,
            remaining_keys: value.remaining_keys,
        }
    }
}

impl From<pb::NamespaceQuotaUsage> for NamespaceQuotaUsage {
    fn from(value: pb::NamespaceQuotaUsage) -> Self {
        Self {
            namespace: value.namespace,
            key_count: value.key_count,
            quota_limit: value.quota_limit,
            usage_pct: value.usage_pct,
            remaining_keys: value.remaining_keys,
        }
    }
}

impl From<NamespaceLatencySummary> for pb::NamespaceLatencySummary {
    fn from(value: NamespaceLatencySummary) -> Self {
        Self {
            namespace: value.namespace,
            request_total: value.request_total,
            latency_p95_estimate_ms: opt_u64(value.latency_p95_estimate_ms),
            latency_p99_estimate_ms: opt_u64(value.latency_p99_estimate_ms),
        }
    }
}

impl From<pb::NamespaceLatencySummary> for NamespaceLatencySummary {
    fn from(value: pb::NamespaceLatencySummary) -> Self {
        Self {
            namespace: value.namespace,
            request_total: value.request_total,
            latency_p95_estimate_ms: from_opt_u64(value.latency_p95_estimate_ms),
            latency_p99_estimate_ms: from_opt_u64(value.latency_p99_estimate_ms),
        }
    }
}

impl From<HotKeyUsage> for pb::HotKeyUsage {
    fn from(value: HotKeyUsage) -> Self {
        Self {
            key: value.key,
            request_total: value.request_total,
        }
    }
}

impl From<pb::HotKeyUsage> for HotKeyUsage {
    fn from(value: pb::HotKeyUsage) -> Self {
        Self {
            key: value.key,
            request_total: value.request_total,
        }
    }
}

impl From<NodeStats> for pb::NodeStats {
    fn from(value: NodeStats) -> Self {
        Self {
            node_id: uuid_to_string(value.node_id),
            status: pb::NodeStatus::from(value.status) as i32,
            is_primary: value.is_primary,
            committed_index: value.committed_index,
            key_count: value.key_count,
            memory_used_bytes: value.memory_used_bytes,
            memory_max_bytes: value.memory_max_bytes,
            evictions: value.evictions,
            hit_count: value.hit_count,
            miss_count: value.miss_count,
            uptime_secs: value.uptime_secs,
            value_size_limit_bytes: value.value_size_limit_bytes,
            max_keys_limit: value.max_keys_limit,
            compression_enabled: value.compression_enabled,
            compression_threshold_bytes: value.compression_threshold_bytes,
            node_name: value.node_name,
            backup_dir_bytes: value.backup_dir_bytes,
            snapshot_last_load_path: opt_string(value.snapshot_last_load_path),
            snapshot_last_load_duration_ms: value.snapshot_last_load_duration_ms,
            snapshot_last_load_entries: value.snapshot_last_load_entries,
            snapshot_last_load_age_secs: opt_u64(value.snapshot_last_load_age_secs),
            snapshot_restore_attempt_total: value.snapshot_restore_attempt_total,
            snapshot_restore_success_total: value.snapshot_restore_success_total,
            snapshot_restore_failure_total: value.snapshot_restore_failure_total,
            snapshot_restore_not_found_total: value.snapshot_restore_not_found_total,
            snapshot_restore_policy_block_total: value.snapshot_restore_policy_block_total,
            snapshot_restore_success_ratio_pct: value.snapshot_restore_success_ratio_pct,
            persistence_platform_allowed: value.persistence_platform_allowed,
            persistence_runtime_enabled: value.persistence_runtime_enabled,
            persistence_enabled: value.persistence_enabled,
            persistence_backup_enabled: value.persistence_backup_enabled,
            persistence_export_enabled: value.persistence_export_enabled,
            persistence_import_enabled: value.persistence_import_enabled,
            tenancy_enabled: value.tenancy_enabled,
            tenancy_default_namespace: value.tenancy_default_namespace,
            tenancy_max_keys_per_namespace: value.tenancy_max_keys_per_namespace as u64,
            rate_limit_enabled: value.rate_limit_enabled,
            rate_limited_requests_total: value.rate_limited_requests_total,
            circuit_breaker_enabled: value.circuit_breaker_enabled,
            hot_key_enabled: value.hot_key_enabled,
            hot_key_adaptive_waiters_enabled: value.hot_key_adaptive_waiters_enabled,
            read_repair_enabled: value.read_repair_enabled,
            hot_key_coalesced_hits_total: value.hot_key_coalesced_hits_total,
            hot_key_fallback_exec_total: value.hot_key_fallback_exec_total,
            hot_key_wait_timeout_total: value.hot_key_wait_timeout_total,
            hot_key_stale_served_total: value.hot_key_stale_served_total,
            hot_key_inflight_keys: value.hot_key_inflight_keys,
            hot_key_stale_cache_entries: value.hot_key_stale_cache_entries,
            hot_key_adaptive_state_keys: value.hot_key_adaptive_state_keys,
            hot_key_adaptive_limit_increase_total: value.hot_key_adaptive_limit_increase_total,
            hot_key_adaptive_limit_decrease_total: value.hot_key_adaptive_limit_decrease_total,
            read_repair_trigger_total: value.read_repair_trigger_total,
            read_repair_success_total: value.read_repair_success_total,
            read_repair_throttled_total: value.read_repair_throttled_total,
            read_repair_budget_exhausted_total: value.read_repair_budget_exhausted_total,
            namespace_quota_reject_total: value.namespace_quota_reject_total,
            namespace_quota_reject_rate_per_min: value.namespace_quota_reject_rate_per_min,
            namespace_quota_reject_trend: value.namespace_quota_reject_trend,
            namespace_quota_top_usage: value
                .namespace_quota_top_usage
                .into_iter()
                .map(Into::into)
                .collect(),
            namespace_latency_top: value
                .namespace_latency_top
                .into_iter()
                .map(Into::into)
                .collect(),
            hot_key_top_usage: value
                .hot_key_top_usage
                .into_iter()
                .map(Into::into)
                .collect(),
            anti_entropy_runs_total: value.anti_entropy_runs_total,
            anti_entropy_repair_trigger_total: value.anti_entropy_repair_trigger_total,
            anti_entropy_repair_throttled_total: value.anti_entropy_repair_throttled_total,
            anti_entropy_last_detected_lag: value.anti_entropy_last_detected_lag,
            anti_entropy_key_checks_total: value.anti_entropy_key_checks_total,
            anti_entropy_key_mismatch_total: value.anti_entropy_key_mismatch_total,
            anti_entropy_full_reconcile_runs_total: value.anti_entropy_full_reconcile_runs_total,
            anti_entropy_full_reconcile_key_checks_total: value
                .anti_entropy_full_reconcile_key_checks_total,
            anti_entropy_full_reconcile_mismatch_total: value
                .anti_entropy_full_reconcile_mismatch_total,
            anti_entropy_budget_exhausted_total: value.anti_entropy_budget_exhausted_total,
            mixed_version_probe_runs_total: value.mixed_version_probe_runs_total,
            mixed_version_peers_detected_total: value.mixed_version_peers_detected_total,
            mixed_version_probe_errors_total: value.mixed_version_probe_errors_total,
            mixed_version_last_detected_peer_count: value.mixed_version_last_detected_peer_count,
            circuit_breaker_state: value.circuit_breaker_state,
            circuit_breaker_open_total: value.circuit_breaker_open_total,
            circuit_breaker_reject_total: value.circuit_breaker_reject_total,
            client_requests_total: value.client_requests_total,
            client_requests_tcp_total: value.client_requests_tcp_total,
            client_requests_http_total: value.client_requests_http_total,
            client_requests_internal_total: value.client_requests_internal_total,
            client_request_latency_le_1ms_total: value.client_request_latency_le_1ms_total,
            client_request_latency_le_5ms_total: value.client_request_latency_le_5ms_total,
            client_request_latency_le_20ms_total: value.client_request_latency_le_20ms_total,
            client_request_latency_le_100ms_total: value.client_request_latency_le_100ms_total,
            client_request_latency_le_500ms_total: value.client_request_latency_le_500ms_total,
            client_request_latency_gt_500ms_total: value.client_request_latency_gt_500ms_total,
            client_latency_p50_estimate_ms: opt_u64(value.client_latency_p50_estimate_ms),
            client_latency_p90_estimate_ms: opt_u64(value.client_latency_p90_estimate_ms),
            client_latency_p95_estimate_ms: opt_u64(value.client_latency_p95_estimate_ms),
            client_latency_p99_estimate_ms: opt_u64(value.client_latency_p99_estimate_ms),
            client_error_total: value.client_error_total,
            client_errors_tcp_total: value.client_errors_tcp_total,
            client_errors_http_total: value.client_errors_http_total,
            client_errors_internal_total: value.client_errors_internal_total,
            client_error_auth_total: value.client_error_auth_total,
            client_error_throttle_total: value.client_error_throttle_total,
            client_error_availability_total: value.client_error_availability_total,
            client_error_validation_total: value.client_error_validation_total,
            client_error_internal_total: value.client_error_internal_total,
            client_error_other_total: value.client_error_other_total,
        }
    }
}

impl TryFrom<pb::NodeStats> for NodeStats {
    type Error = anyhow::Error;

    fn try_from(value: pb::NodeStats) -> anyhow::Result<Self> {
        Ok(Self {
            node_id: uuid_from_string(value.node_id)?,
            status: value.status.try_into()?,
            is_primary: value.is_primary,
            committed_index: value.committed_index,
            key_count: value.key_count,
            memory_used_bytes: value.memory_used_bytes,
            memory_max_bytes: value.memory_max_bytes,
            evictions: value.evictions,
            hit_count: value.hit_count,
            miss_count: value.miss_count,
            uptime_secs: value.uptime_secs,
            value_size_limit_bytes: value.value_size_limit_bytes,
            max_keys_limit: value.max_keys_limit,
            compression_enabled: value.compression_enabled,
            compression_threshold_bytes: value.compression_threshold_bytes,
            node_name: value.node_name,
            backup_dir_bytes: value.backup_dir_bytes,
            snapshot_last_load_path: from_opt_string(value.snapshot_last_load_path),
            snapshot_last_load_duration_ms: value.snapshot_last_load_duration_ms,
            snapshot_last_load_entries: value.snapshot_last_load_entries,
            snapshot_last_load_age_secs: from_opt_u64(value.snapshot_last_load_age_secs),
            snapshot_restore_attempt_total: value.snapshot_restore_attempt_total,
            snapshot_restore_success_total: value.snapshot_restore_success_total,
            snapshot_restore_failure_total: value.snapshot_restore_failure_total,
            snapshot_restore_not_found_total: value.snapshot_restore_not_found_total,
            snapshot_restore_policy_block_total: value.snapshot_restore_policy_block_total,
            snapshot_restore_success_ratio_pct: value.snapshot_restore_success_ratio_pct,
            persistence_platform_allowed: value.persistence_platform_allowed,
            persistence_runtime_enabled: value.persistence_runtime_enabled,
            persistence_enabled: value.persistence_enabled,
            persistence_backup_enabled: value.persistence_backup_enabled,
            persistence_export_enabled: value.persistence_export_enabled,
            persistence_import_enabled: value.persistence_import_enabled,
            tenancy_enabled: value.tenancy_enabled,
            tenancy_default_namespace: value.tenancy_default_namespace,
            tenancy_max_keys_per_namespace: usize::try_from(value.tenancy_max_keys_per_namespace)?,
            rate_limit_enabled: value.rate_limit_enabled,
            rate_limited_requests_total: value.rate_limited_requests_total,
            circuit_breaker_enabled: value.circuit_breaker_enabled,
            hot_key_enabled: value.hot_key_enabled,
            hot_key_adaptive_waiters_enabled: value.hot_key_adaptive_waiters_enabled,
            read_repair_enabled: value.read_repair_enabled,
            hot_key_coalesced_hits_total: value.hot_key_coalesced_hits_total,
            hot_key_fallback_exec_total: value.hot_key_fallback_exec_total,
            hot_key_wait_timeout_total: value.hot_key_wait_timeout_total,
            hot_key_stale_served_total: value.hot_key_stale_served_total,
            hot_key_inflight_keys: value.hot_key_inflight_keys,
            hot_key_stale_cache_entries: value.hot_key_stale_cache_entries,
            hot_key_adaptive_state_keys: value.hot_key_adaptive_state_keys,
            hot_key_adaptive_limit_increase_total: value.hot_key_adaptive_limit_increase_total,
            hot_key_adaptive_limit_decrease_total: value.hot_key_adaptive_limit_decrease_total,
            read_repair_trigger_total: value.read_repair_trigger_total,
            read_repair_success_total: value.read_repair_success_total,
            read_repair_throttled_total: value.read_repair_throttled_total,
            read_repair_budget_exhausted_total: value.read_repair_budget_exhausted_total,
            namespace_quota_reject_total: value.namespace_quota_reject_total,
            namespace_quota_reject_rate_per_min: value.namespace_quota_reject_rate_per_min,
            namespace_quota_reject_trend: value.namespace_quota_reject_trend,
            namespace_quota_top_usage: value
                .namespace_quota_top_usage
                .into_iter()
                .map(Into::into)
                .collect(),
            namespace_latency_top: value
                .namespace_latency_top
                .into_iter()
                .map(Into::into)
                .collect(),
            hot_key_top_usage: value
                .hot_key_top_usage
                .into_iter()
                .map(Into::into)
                .collect(),
            anti_entropy_runs_total: value.anti_entropy_runs_total,
            anti_entropy_repair_trigger_total: value.anti_entropy_repair_trigger_total,
            anti_entropy_repair_throttled_total: value.anti_entropy_repair_throttled_total,
            anti_entropy_last_detected_lag: value.anti_entropy_last_detected_lag,
            anti_entropy_key_checks_total: value.anti_entropy_key_checks_total,
            anti_entropy_key_mismatch_total: value.anti_entropy_key_mismatch_total,
            anti_entropy_full_reconcile_runs_total: value.anti_entropy_full_reconcile_runs_total,
            anti_entropy_full_reconcile_key_checks_total: value
                .anti_entropy_full_reconcile_key_checks_total,
            anti_entropy_full_reconcile_mismatch_total: value
                .anti_entropy_full_reconcile_mismatch_total,
            anti_entropy_budget_exhausted_total: value.anti_entropy_budget_exhausted_total,
            mixed_version_probe_runs_total: value.mixed_version_probe_runs_total,
            mixed_version_peers_detected_total: value.mixed_version_peers_detected_total,
            mixed_version_probe_errors_total: value.mixed_version_probe_errors_total,
            mixed_version_last_detected_peer_count: value.mixed_version_last_detected_peer_count,
            circuit_breaker_state: value.circuit_breaker_state,
            circuit_breaker_open_total: value.circuit_breaker_open_total,
            circuit_breaker_reject_total: value.circuit_breaker_reject_total,
            client_requests_total: value.client_requests_total,
            client_requests_tcp_total: value.client_requests_tcp_total,
            client_requests_http_total: value.client_requests_http_total,
            client_requests_internal_total: value.client_requests_internal_total,
            client_request_latency_le_1ms_total: value.client_request_latency_le_1ms_total,
            client_request_latency_le_5ms_total: value.client_request_latency_le_5ms_total,
            client_request_latency_le_20ms_total: value.client_request_latency_le_20ms_total,
            client_request_latency_le_100ms_total: value.client_request_latency_le_100ms_total,
            client_request_latency_le_500ms_total: value.client_request_latency_le_500ms_total,
            client_request_latency_gt_500ms_total: value.client_request_latency_gt_500ms_total,
            client_latency_p50_estimate_ms: from_opt_u64(value.client_latency_p50_estimate_ms),
            client_latency_p90_estimate_ms: from_opt_u64(value.client_latency_p90_estimate_ms),
            client_latency_p95_estimate_ms: from_opt_u64(value.client_latency_p95_estimate_ms),
            client_latency_p99_estimate_ms: from_opt_u64(value.client_latency_p99_estimate_ms),
            client_error_total: value.client_error_total,
            client_errors_tcp_total: value.client_errors_tcp_total,
            client_errors_http_total: value.client_errors_http_total,
            client_errors_internal_total: value.client_errors_internal_total,
            client_error_auth_total: value.client_error_auth_total,
            client_error_throttle_total: value.client_error_throttle_total,
            client_error_availability_total: value.client_error_availability_total,
            client_error_validation_total: value.client_error_validation_total,
            client_error_internal_total: value.client_error_internal_total,
            client_error_other_total: value.client_error_other_total,
        })
    }
}

impl From<AdminResponse> for pb::AdminResponse {
    fn from(value: AdminResponse) -> Self {
        use pb::admin_response::Response;
        let response = match value {
            AdminResponse::Properties(items) => Response::Properties(pb::Properties {
                items: items
                    .into_iter()
                    .map(|(key, value)| pb::PropertyPair { key, value })
                    .collect(),
            }),
            AdminResponse::Keys(keys) => Response::Keys(pb::Keys { keys }),
            AdminResponse::Stats(stats) => Response::Stats((*stats).into()),
            AdminResponse::KeyInfo {
                key,
                value,
                version,
                ttl_remaining_secs,
                freq_count,
                compressed,
            } => Response::KeyInfo(pb::KeyInfo {
                key,
                value: value.to_vec(),
                version,
                ttl_remaining_secs: opt_u64(ttl_remaining_secs),
                freq_count,
                compressed,
            }),
            AdminResponse::KeyPropertyUpdated => Response::KeyPropertyUpdated(pb::Empty {}),
            AdminResponse::Flushed => Response::Flushed(pb::Empty {}),
            AdminResponse::ClusterView(nodes) => Response::ClusterView(pb::ClusterView {
                nodes: nodes.into_iter().map(Into::into).collect(),
            }),
            AdminResponse::BackupResult { path, bytes } => {
                Response::BackupResult(pb::BackupResult { path, bytes })
            }
            AdminResponse::RestoreResult {
                path,
                entries,
                duration_ms,
            } => Response::RestoreResult(pb::RestoreResult {
                path,
                entries,
                duration_ms,
            }),
            AdminResponse::TtlUpdated { updated } => Response::TtlUpdated(pb::CountResponse {
                count: updated as u64,
            }),
            AdminResponse::Ok => Response::Ok(pb::Empty {}),
            AdminResponse::NotFound => Response::NotFound(pb::Empty {}),
            AdminResponse::Error { message } => Response::Error(pb::ErrorMessage { message }),
        };
        Self {
            response: Some(response),
        }
    }
}

impl TryFrom<pb::AdminResponse> for AdminResponse {
    type Error = anyhow::Error;

    fn try_from(value: pb::AdminResponse) -> anyhow::Result<Self> {
        use pb::admin_response::Response;
        let response = value
            .response
            .ok_or_else(|| anyhow::anyhow!("missing admin response payload"))?;
        Ok(match response {
            Response::Properties(v) => Self::Properties(
                v.items
                    .into_iter()
                    .map(|item| (item.key, item.value))
                    .collect(),
            ),
            Response::Keys(v) => Self::Keys(v.keys),
            Response::Stats(v) => Self::Stats(Box::new(v.try_into()?)),
            Response::KeyInfo(v) => Self::KeyInfo {
                key: v.key,
                value: Bytes::from(v.value),
                version: v.version,
                ttl_remaining_secs: from_opt_u64(v.ttl_remaining_secs),
                freq_count: v.freq_count,
                compressed: v.compressed,
            },
            Response::KeyPropertyUpdated(_) => Self::KeyPropertyUpdated,
            Response::Flushed(_) => Self::Flushed,
            Response::ClusterView(v) => Self::ClusterView(
                v.nodes
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<anyhow::Result<Vec<_>>>()?,
            ),
            Response::BackupResult(v) => Self::BackupResult {
                path: v.path,
                bytes: v.bytes,
            },
            Response::RestoreResult(v) => Self::RestoreResult {
                path: v.path,
                entries: v.entries,
                duration_ms: v.duration_ms,
            },
            Response::TtlUpdated(v) => Self::TtlUpdated {
                updated: usize::try_from(v.count)?,
            },
            Response::Ok(_) => Self::Ok,
            Response::NotFound(_) => Self::NotFound,
            Response::Error(v) => Self::Error { message: v.message },
        })
    }
}

// ---------------------------------------------------------------------------
// Wire framing helpers
// ---------------------------------------------------------------------------

fn encode_payload<T: WireMessage>(msg: T) -> Vec<u8> {
    let envelope = pb::Envelope {
        version: PROTOCOL_VERSION,
        payload: Some(msg.into_payload()),
    };
    envelope.encode_to_vec()
}

fn decode_payload<T: WireMessage>(buf: &[u8], max_size: u64) -> anyhow::Result<T> {
    if buf.len() as u64 > max_size {
        anyhow::bail!("message length {} exceeds max {}", buf.len(), max_size);
    }

    let envelope = pb::Envelope::decode(buf)?;
    if envelope.version != PROTOCOL_VERSION {
        anyhow::bail!(
            "unsupported protocol version {} (expected {})",
            envelope.version,
            PROTOCOL_VERSION
        );
    }
    let payload = envelope
        .payload
        .ok_or_else(|| anyhow::anyhow!("missing protobuf envelope payload"))?;
    T::from_payload(payload)
}

/// Encode any protocol message to length-prefixed bytes (4-byte BE length).
pub fn encode<T: WireMessage + Clone>(msg: &T) -> anyhow::Result<Vec<u8>> {
    let payload = encode_payload(msg.clone());
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Decode a length-prefixed message from a byte slice with limits.
pub fn decode<T: WireMessage>(buf: &[u8], max_size: u64) -> anyhow::Result<T> {
    decode_payload(buf, max_size)
}

/// Encode a gossip datagram payload without TCP length framing.
pub fn encode_gossip(msg: &GossipMessage) -> anyhow::Result<Vec<u8>> {
    Ok(encode_payload(msg.clone()))
}

/// Decode a gossip datagram payload without TCP length framing.
pub fn decode_gossip(buf: &[u8], max_size: u64) -> anyhow::Result<GossipMessage> {
    decode_payload(buf, max_size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::net::SocketAddr;

    fn payload(frame: &[u8]) -> &[u8] {
        let len = u32::from_be_bytes(frame[0..4].try_into().unwrap()) as usize;
        assert_eq!(len, frame.len() - 4);
        &frame[4..]
    }

    #[test]
    fn client_requests_round_trip_through_envelope() {
        let cases = vec![
            ClientRequest::Get {
                key: "k".into(),
                namespace: Some("tenant-a".into()),
            },
            ClientRequest::Set {
                key: "k".into(),
                value: Bytes::from_static(b"value"),
                ttl_secs: Some(60),
                namespace: Some("tenant-a".into()),
            },
            ClientRequest::Delete {
                key: "k".into(),
                namespace: None,
            },
            ClientRequest::Ping,
            ClientRequest::Auth {
                token: "secret".into(),
            },
            ClientRequest::Watch {
                key: "k".into(),
                namespace: Some("tenant-a".into()),
            },
            ClientRequest::Unwatch {
                key: "k".into(),
                namespace: None,
            },
            ClientRequest::DeleteByPattern {
                pattern: "tenant:*".into(),
                namespace: Some("tenant-a".into()),
            },
            ClientRequest::SetNx {
                key: "lock".into(),
                value: Bytes::from_static(b"holder-a"),
                ttl_secs: Some(30),
                namespace: Some("tenant-a".into()),
            },
            ClientRequest::Incr {
                key: "counter".into(),
                delta: -2,
                ttl_secs_on_create: Some(15),
                namespace: None,
            },
            ClientRequest::SetTtlByPattern {
                pattern: "tenant:*".into(),
                ttl_secs: None,
                namespace: Some("tenant-a".into()),
            },
        ];

        for request in cases {
            let encoded = encode(&request).unwrap();
            let decoded: ClientRequest = decode(payload(&encoded), 1024).unwrap();
            match (request, decoded) {
                (
                    ClientRequest::Get {
                        key: a,
                        namespace: an,
                    },
                    ClientRequest::Get {
                        key: b,
                        namespace: bn,
                    },
                ) => {
                    assert_eq!((a, an), (b, bn));
                }
                (
                    ClientRequest::Set {
                        key: a,
                        value: av,
                        ttl_secs: at,
                        namespace: an,
                    },
                    ClientRequest::Set {
                        key: b,
                        value: bv,
                        ttl_secs: bt,
                        namespace: bn,
                    },
                ) => assert_eq!((a, av, at, an), (b, bv, bt, bn)),
                (
                    ClientRequest::Delete {
                        key: a,
                        namespace: an,
                    },
                    ClientRequest::Delete {
                        key: b,
                        namespace: bn,
                    },
                ) => {
                    assert_eq!((a, an), (b, bn));
                }
                (ClientRequest::Ping, ClientRequest::Ping) => {}
                (ClientRequest::Auth { token: a }, ClientRequest::Auth { token: b }) => {
                    assert_eq!(a, b)
                }
                (
                    ClientRequest::Watch {
                        key: a,
                        namespace: an,
                    },
                    ClientRequest::Watch {
                        key: b,
                        namespace: bn,
                    },
                ) => {
                    assert_eq!((a, an), (b, bn));
                }
                (
                    ClientRequest::Unwatch {
                        key: a,
                        namespace: an,
                    },
                    ClientRequest::Unwatch {
                        key: b,
                        namespace: bn,
                    },
                ) => {
                    assert_eq!((a, an), (b, bn));
                }
                (
                    ClientRequest::DeleteByPattern {
                        pattern: a,
                        namespace: an,
                    },
                    ClientRequest::DeleteByPattern {
                        pattern: b,
                        namespace: bn,
                    },
                ) => assert_eq!((a, an), (b, bn)),
                (
                    ClientRequest::SetNx {
                        key: a,
                        value: av,
                        ttl_secs: at,
                        namespace: an,
                    },
                    ClientRequest::SetNx {
                        key: b,
                        value: bv,
                        ttl_secs: bt,
                        namespace: bn,
                    },
                ) => assert_eq!((a, av, at, an), (b, bv, bt, bn)),
                (
                    ClientRequest::Incr {
                        key: a,
                        delta: ad,
                        ttl_secs_on_create: at,
                        namespace: an,
                    },
                    ClientRequest::Incr {
                        key: b,
                        delta: bd,
                        ttl_secs_on_create: bt,
                        namespace: bn,
                    },
                ) => assert_eq!((a, ad, at, an), (b, bd, bt, bn)),
                (
                    ClientRequest::SetTtlByPattern {
                        pattern: a,
                        ttl_secs: at,
                        namespace: an,
                    },
                    ClientRequest::SetTtlByPattern {
                        pattern: b,
                        ttl_secs: bt,
                        namespace: bn,
                    },
                ) => assert_eq!((a, at, an), (b, bt, bn)),
                (a, b) => panic!("roundtrip changed request: {a:?} -> {b:?}"),
            }
        }
    }

    #[test]
    fn client_responses_round_trip_through_envelope() {
        let cases = vec![
            ClientResponse::Value {
                key: "k".into(),
                value: Bytes::from_static(b"value"),
                version: 2,
            },
            ClientResponse::Ok { version: 3 },
            ClientResponse::Deleted,
            ClientResponse::NotFound,
            ClientResponse::Pong,
            ClientResponse::AuthOk,
            ClientResponse::Error {
                code: ErrorCode::NamespaceQuotaExceeded,
                message: "quota".into(),
            },
            ClientResponse::Watching,
            ClientResponse::Unwatched,
            ClientResponse::WatchEvent {
                key: "k".into(),
                value: Some(Bytes::from_static(b"value")),
                version: 4,
            },
            ClientResponse::WatchEvent {
                key: "k".into(),
                value: None,
                version: 5,
            },
            ClientResponse::PatternDeleted { deleted: 6 },
            ClientResponse::PatternTtlUpdated { updated: 7 },
            ClientResponse::SetNx {
                created: true,
                version: 8,
            },
            ClientResponse::Counter {
                value: -9,
                version: 10,
            },
        ];

        for response in cases {
            let encoded = encode(&response).unwrap();
            let decoded: ClientResponse = decode(payload(&encoded), 1024).unwrap();
            match (response, decoded) {
                (
                    ClientResponse::Value {
                        key: a,
                        value: av,
                        version: aver,
                    },
                    ClientResponse::Value {
                        key: b,
                        value: bv,
                        version: bver,
                    },
                ) => assert_eq!((a, av, aver), (b, bv, bver)),
                (ClientResponse::Ok { version: a }, ClientResponse::Ok { version: b }) => {
                    assert_eq!(a, b)
                }
                (ClientResponse::Deleted, ClientResponse::Deleted)
                | (ClientResponse::NotFound, ClientResponse::NotFound)
                | (ClientResponse::Pong, ClientResponse::Pong)
                | (ClientResponse::AuthOk, ClientResponse::AuthOk)
                | (ClientResponse::Watching, ClientResponse::Watching)
                | (ClientResponse::Unwatched, ClientResponse::Unwatched) => {}
                (
                    ClientResponse::Error {
                        code: ErrorCode::NamespaceQuotaExceeded,
                        message: a,
                    },
                    ClientResponse::Error {
                        code: ErrorCode::NamespaceQuotaExceeded,
                        message: b,
                    },
                ) => assert_eq!(a, b),
                (
                    ClientResponse::WatchEvent {
                        key: a,
                        value: av,
                        version: aver,
                    },
                    ClientResponse::WatchEvent {
                        key: b,
                        value: bv,
                        version: bver,
                    },
                ) => assert_eq!((a, av, aver), (b, bv, bver)),
                (
                    ClientResponse::PatternDeleted { deleted: a },
                    ClientResponse::PatternDeleted { deleted: b },
                ) => assert_eq!(a, b),
                (
                    ClientResponse::PatternTtlUpdated { updated: a },
                    ClientResponse::PatternTtlUpdated { updated: b },
                ) => assert_eq!(a, b),
                (
                    ClientResponse::SetNx {
                        created: ac,
                        version: av,
                    },
                    ClientResponse::SetNx {
                        created: bc,
                        version: bv,
                    },
                ) => assert_eq!((ac, av), (bc, bv)),
                (
                    ClientResponse::Counter {
                        value: av,
                        version: aver,
                    },
                    ClientResponse::Counter {
                        value: bv,
                        version: bver,
                    },
                ) => assert_eq!((av, aver), (bv, bver)),
                (a, b) => panic!("roundtrip changed response: {a:?} -> {b:?}"),
            }
        }
    }

    #[test]
    fn cluster_and_gossip_messages_round_trip() {
        let node_id = Uuid::from_u128(42);
        let peer_id = Uuid::from_u128(43);
        let addr: SocketAddr = "127.0.0.1:7779".parse().unwrap();
        let node = NodeInfo {
            id: node_id,
            addr,
            cluster_port: 7779,
            status: NodeStatus::Active,
            last_applied: 12,
        };

        let cluster = ClusterMessage::Forward {
            request: ClientRequest::Ping,
            origin_node: peer_id,
        };
        let decoded: ClusterMessage = decode(payload(&encode(&cluster).unwrap()), 2048).unwrap();
        match decoded {
            ClusterMessage::Forward {
                request: ClientRequest::Ping,
                origin_node,
            } => assert_eq!(origin_node, peer_id),
            other => panic!("unexpected cluster roundtrip: {other:?}"),
        }

        let log_entries = ClusterMessage::LogEntries {
            entries: vec![LogEntry {
                index: 9,
                key: "k".into(),
                value: Some(Bytes::from_static(b"value")),
                ttl_secs: Some(30),
                ts_ms: 123,
            }],
        };
        let decoded: ClusterMessage =
            decode(payload(&encode(&log_entries).unwrap()), 2048).unwrap();
        match decoded {
            ClusterMessage::LogEntries { entries } => {
                assert_eq!(entries[0].key, "k");
                assert_eq!(entries[0].value.as_deref(), Some(&b"value"[..]));
                assert_eq!(entries[0].ttl_secs, Some(30));
            }
            other => panic!("unexpected log entries roundtrip: {other:?}"),
        }

        let gossip = GossipMessage::ActiveSetUpdate {
            active_nodes: vec![node.clone()],
        };
        let decoded = decode_gossip(&encode_gossip(&gossip).unwrap(), 2048).unwrap();
        match decoded {
            GossipMessage::ActiveSetUpdate { active_nodes } => {
                assert_eq!(active_nodes[0].id, node_id);
                assert_eq!(active_nodes[0].addr, addr);
            }
            other => panic!("unexpected gossip roundtrip: {other:?}"),
        }

        let heartbeat = GossipMessage::Heartbeat {
            node_id,
            addr,
            cluster_port: 7779,
            status: NodeStatus::Syncing,
            last_applied: 99,
        };
        match decode_gossip(&encode_gossip(&heartbeat).unwrap(), 2048).unwrap() {
            GossipMessage::Heartbeat {
                status: NodeStatus::Syncing,
                last_applied: 99,
                ..
            } => {}
            other => panic!("unexpected heartbeat roundtrip: {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_bad_envelopes_and_limits() {
        let encoded = encode(&ClientRequest::Ping).unwrap();
        assert!(decode::<ClientRequest>(payload(&encoded), 1)
            .unwrap_err()
            .to_string()
            .contains("exceeds max"));

        let wrong_payload = pb::Envelope {
            version: 99,
            payload: Some(ClientRequest::Ping.into_payload()),
        }
        .encode_to_vec();
        assert!(decode::<ClientRequest>(&wrong_payload, 1024)
            .unwrap_err()
            .to_string()
            .contains("unsupported protocol version"));

        let missing_payload = pb::Envelope {
            version: PROTOCOL_VERSION,
            payload: None,
        }
        .encode_to_vec();
        assert!(decode::<ClientRequest>(&missing_payload, 1024)
            .unwrap_err()
            .to_string()
            .contains("missing protobuf envelope payload"));
    }
}
