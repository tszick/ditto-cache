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
    pub id:           Uuid,
    pub addr:         SocketAddr,
    pub cluster_port: u16,
    pub status:       NodeStatus,
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
    Get    { key: String },
    Set    { key: String, value: Bytes, ttl_secs: Option<u64> },
    Delete { key: String },
    Ping,
    Auth   { token: String },
    // DITTO-02: pub/sub watch API (variants 5, 6)
    Watch   { key: String },
    Unwatch { key: String },
    /// Delete all keys matching a glob-style pattern (`*` wildcard).
    DeleteByPattern { pattern: String },
    /// Update TTL for all keys matching a glob-style pattern.
    /// `ttl_secs = None` removes TTL for matched keys.
    SetTtlByPattern { pattern: String, ttl_secs: Option<u64> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientResponse {
    Value    { key: String, value: Bytes, version: u64 },
    Ok       { version: u64 },
    Deleted,
    NotFound,
    Pong,
    AuthOk,
    Error    { code: ErrorCode, message: String },
    // DITTO-02: pub/sub watch API (variants 7, 8, 9)
    /// Acknowledge to a Watch request.
    Watching,
    /// Acknowledge to an Unwatch request.
    Unwatched,
    /// Server-push: a watched key was committed (value = None means the key was deleted).
    WatchEvent { key: String, value: Option<Bytes>, version: u64 },
    /// Number of keys deleted by a pattern-based delete operation.
    PatternDeleted { deleted: usize },
    /// Number of keys updated by a pattern-based TTL operation.
    PatternTtlUpdated { updated: usize },
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
        key:       String,
        value:     Option<Bytes>,
        ttl_secs:  Option<u64>,
    },
    /// Follower → primary: prepare acknowledged.
    PrepareAck { log_index: u64, node_id: Uuid },

    /// Primary → all active nodes: "commit, make visible".
    Commit { log_index: u64 },
    /// Follower → primary: commit acknowledged.
    CommitAck { log_index: u64, node_id: Uuid },

    // --- Forwarding (non-primary receives client write) ---

    /// Non-primary → primary: forward a client write.
    Forward { request: ClientRequest, origin_node: Uuid },

    // --- Recovery / anti-entropy ---

    /// Recovering node → any active node: "give me log entries".
    RequestLog { from_index: u64 },
    /// Active node → recovering node: log entries response.
    LogEntries { entries: Vec<LogEntry> },
    /// Recovering node → gossip: "I'm fully synced".
    Synced { node_id: Uuid, last_applied: u64 },

    // --- Forward response (primary → follower after handling forwarded write) ---
    /// Primary → non-primary: result of a forwarded client write.
    ForwardResponse(ClientResponse),

    // --- Primary election override ---
    /// Broadcast by force-elected primary to all peers.
    ForcePrimary { node_id: Uuid },

    // --- Admin (also on port 7779) ---
    Admin(AdminRequest),
    /// Node → admin client: response to an Admin request.
    AdminResponse(AdminResponse),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub index:    u64,
    pub key:      String,
    pub value:    Option<Bytes>,
    pub ttl_secs: Option<u64>,
    pub ts_ms:    u64,
}

// ---------------------------------------------------------------------------
// Gossip protocol  (port 7780 UDP)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipMessage {
    /// Periodic heartbeat broadcast.
    Heartbeat {
        node_id:      Uuid,
        addr:         SocketAddr,
        cluster_port: u16,
        status:       NodeStatus,
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
    GetProperty     { name: String },
    SetProperty     { name: String, value: String },
    ListKeys        { pattern: Option<String> },
    GetStats,
    /// Retrieve full info for a single key (value + TTL + freq + compressed).
    GetKeyInfo      { key: String },
    /// Set a per-key property (e.g. "compressed" = "true"/"false").
    SetKeyProperty  { key: String, name: String, value: String },
    FlushCache,
    ClusterStatus,
    /// Trigger an immediate backup on this node (inactivate → export → activate).
    BackupNow,
    /// Set TTL for all keys matching a glob pattern. `ttl_secs = None` removes TTL.
    SetKeysTtl { pattern: String, ttl_secs: Option<u64> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AdminResponse {
    Properties    (Vec<(String, String)>),
    Keys          (Vec<String>),
    Stats         (NodeStats),
    /// Response to GetKeyInfo.
    KeyInfo {
        key:                String,
        value:              Bytes,
        version:            u64,
        ttl_remaining_secs: Option<u64>,
        freq_count:         u64,
        /// Whether the stored value is LZ4-compressed.
        compressed:         bool,
    },
    /// Response to SetKeyProperty.
    KeyPropertyUpdated,
    Flushed,
    ClusterView   (Vec<NodeInfo>),
    /// Response to BackupNow: the path of the created backup file.
    BackupResult  { path: String, bytes: u64 },
    /// Response to SetKeysTtl: number of keys whose TTL was updated.
    TtlUpdated    { updated: usize },
    Ok,
    NotFound,
    Error         { message: String },
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStats {
    pub node_id:          Uuid,
    pub status:           NodeStatus,
    pub is_primary:       bool,
    pub committed_index:  u64,
    pub key_count:        u64,
    pub memory_used_bytes:   u64,
    pub memory_max_bytes:    u64,
    pub evictions:           u64,
    pub hit_count:           u64,
    pub miss_count:          u64,
    pub uptime_secs:         u64,
    /// Maximum allowed value size in bytes (0 = unlimited).
    pub value_size_limit_bytes: u64,
    /// Maximum number of keys allowed in the cache (0 = unlimited).
    pub max_keys_limit:      u64,
    /// Whether automatic LZ4 compression is enabled.
    pub compression_enabled: bool,
    /// Minimum value size (bytes) to trigger automatic compression.
    pub compression_threshold_bytes: u64,
    /// Human-readable node name (DITTO_NODE_ID / cfg.node.id).
    pub node_name:        String,
    /// Total bytes occupied by backup files in the configured backup directory.
    /// Computed at query time; 0 if the directory is empty or does not exist.
    pub backup_dir_bytes: u64,
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
    /// Node-level request limiter enabled.
    pub rate_limit_enabled: bool,
    /// Total client requests rejected by rate limiting since startup.
    pub rate_limited_requests_total: u64,
    /// Circuit breaker enabled.
    pub circuit_breaker_enabled: bool,
    /// Current circuit breaker state: "closed" | "open" | "half-open".
    pub circuit_breaker_state: String,
    /// Number of times the circuit transitioned to open.
    pub circuit_breaker_open_total: u64,
    /// Total client requests rejected because circuit is open/limited in half-open.
    pub circuit_breaker_reject_total: u64,
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
