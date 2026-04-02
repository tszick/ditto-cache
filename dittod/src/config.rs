use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub node: NodeConfig,
    pub cluster: ClusterConfig,
    pub cache: CacheConfig,
    pub replication: ReplicationConfig,
    #[serde(default)]
    pub compression: CompressionConfig,
    #[serde(default)]
    pub tls: TlsConfig,
    #[serde(default)]
    pub http_auth: HttpAuthConfig,
    #[serde(default)]
    pub backup: BackupConfig,
    #[serde(default)]
    pub persistence: PersistenceConfig,
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
    #[serde(default)]
    pub circuit_breaker: CircuitBreakerConfig,
    #[serde(default)]
    pub hot_key: HotKeyConfig,
    #[serde(default)]
    pub log: LogConfig,
}

/// Optional HTTP Basic Auth for the REST port (7778).
///
/// When `password_hash` is set every request to the HTTP API must supply
/// an `Authorization: Basic <base64>` header matching these credentials.
/// The `/ping` endpoint is always exempt (health checks must work unauthenticated).
///
/// Generate a hash with: `dittoctl hash-password`
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HttpAuthConfig {
    /// Expected username (default: `"ditto"` when only hash is set).
    #[serde(default)]
    pub username: Option<String>,
    /// bcrypt hash of the password.  When absent, auth is disabled.
    #[serde(default)]
    pub password_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Unique node identifier (e.g. "node-1").
    pub id: String,
    /// Bind address for client-facing ports (TCP 7777, HTTP 7778).
    /// Supports: "0.0.0.0" (all), "127.0.0.1" / "localhost", or an explicit IP.
    pub bind_addr: String,
    /// Bind address for internal cluster ports (cluster TCP 7779, gossip UDP 7780).
    /// Supports the same values as `bind_addr`, plus `"site-local"` which
    /// automatically selects the first private IPv4 interface on the host
    /// (10.x, 172.16–31.x, 192.168.x).
    /// Default: `"site-local"`.
    #[serde(default = "default_cluster_bind_addr")]
    pub cluster_bind_addr: String,
    /// TCP port for client binary protocol.
    pub client_port: u16,
    /// HTTP port for REST API.
    pub http_port: u16,
    /// TCP port for inter-node cluster + admin traffic.
    pub cluster_port: u16,
    /// UDP port for gossip.
    pub gossip_port: u16,
    /// When false: node rejects client requests but stays synced.
    pub active: bool,
    /// Optional shared secret for TCP client authentication (port 7777).
    #[serde(default)]
    pub client_auth_token: Option<String>,
    /// Maximum allowed incoming TCP message size in bytes. Default: 128 MiB.
    #[serde(default = "default_max_message_size")]
    pub max_message_size_bytes: u32,
    /// Timeout for reading a single client frame header/payload on TCP (ms).
    #[serde(default = "default_frame_read_timeout_ms")]
    pub frame_read_timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Bootstrap seed addresses "host:cluster_port".
    pub seeds: Vec<String>,
    /// Maximum allowed cluster size.
    #[serde(default = "default_max_nodes")]
    pub max_nodes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Maximum memory for the KV store in megabytes.
    pub max_memory_mb: u64,
    /// Global default TTL in seconds; 0 means no TTL.
    pub default_ttl_secs: u64,
    /// Eviction policy (only "lfu" supported for now).
    #[serde(default = "default_eviction_policy")]
    pub eviction_policy: String,
    /// Maximum value size in bytes; 0 means unlimited. Default: 100 MiB.
    #[serde(default = "default_value_size_limit")]
    pub value_size_limit_bytes: u64,
    /// Maximum number of keys in the cache; 0 means unlimited. Default: 100 000.
    #[serde(default = "default_max_keys")]
    pub max_keys: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    /// Enable automatic LZ4 compression for large values.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Values larger than this (bytes) are compressed. Minimum 4096.
    #[serde(default = "default_compression_threshold")]
    pub threshold_bytes: u64,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            threshold_bytes: 4096,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Enable mTLS on the cluster/admin port (7779).
    #[serde(default)]
    pub enabled: bool,
    /// Path to the CA certificate (PEM).
    #[serde(default)]
    pub ca_cert: String,
    /// Path to this node's certificate (PEM).
    #[serde(default)]
    pub cert: String,
    /// Path to this node's private key (PEM).
    #[serde(default)]
    pub key: String,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            ca_cert: String::new(),
            cert: String::new(),
            key: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupConfig {
    /// Whether automatic scheduled backups are enabled.
    #[serde(default)]
    pub enabled: bool,
    /// Cron schedule (standard 5-field: min hour dom month dow).
    /// Example: "0 2 * * *" = daily at 02:00 UTC.
    #[serde(default = "default_backup_schedule")]
    pub schedule: String,
    /// Directory where backup files are written.
    #[serde(default = "default_backup_path")]
    pub path: String,
    /// Serialization format: "binary" or "json".
    #[serde(default = "default_backup_format")]
    pub format: String,
    /// Delete backup files older than this many days (0 = never delete).
    #[serde(default = "default_retain_days")]
    pub retain_days: u64,
    /// Attempt to restore the most recent snapshot file at startup.
    #[serde(default)]
    pub restore_on_start: bool,
    /// Hex-encoded 32-byte AES-256-GCM key for backup file encryption.
    /// When set, backup files are encrypted and get an `.enc` suffix.
    /// Generate with: `openssl rand -hex 32`
    /// When absent, backups are written as plain binary/json (no encryption).
    #[serde(default)]
    pub encryption_key: Option<String>,
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            schedule: default_backup_schedule(),
            path: default_backup_path(),
            format: default_backup_format(),
            retain_days: default_retain_days(),
            restore_on_start: false,
            encryption_key: None,
        }
    }
}

/// Persistence policy gate:
/// - platform_allowed: immutable platform switch (env/config level)
/// - runtime_enabled: mutable runtime switch (admin SetProperty)
/// - *feature*_allowed: platform-level feature gates
///
/// Effective feature status:
/// `platform_allowed && runtime_enabled && <feature_allowed>`
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PersistenceConfig {
    /// Master platform gate. Default false.
    #[serde(default)]
    pub platform_allowed: bool,
    /// Runtime gate toggled by admin API/CLI. Default false.
    #[serde(default)]
    pub runtime_enabled: bool,
    /// Platform gate for backup operations.
    #[serde(default)]
    pub backup_allowed: bool,
    /// Platform gate for export operations.
    #[serde(default)]
    pub export_allowed: bool,
    /// Platform gate for import operations.
    #[serde(default)]
    pub import_allowed: bool,
}

/// Node-level request rate limiter (token bucket).
///
/// Applies to client requests handled by `dittod` (TCP + HTTP).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Enable request rate limiting.
    #[serde(default)]
    pub enabled: bool,
    /// Refill rate in requests per second.
    #[serde(default = "default_rate_limit_rps")]
    pub requests_per_sec: u64,
    /// Maximum burst size (bucket capacity).
    #[serde(default = "default_rate_limit_burst")]
    pub burst: u64,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            requests_per_sec: default_rate_limit_rps(),
            burst: default_rate_limit_burst(),
        }
    }
}

/// Node-level circuit breaker for client request handling.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Enable circuit breaker protection.
    #[serde(default)]
    pub enabled: bool,
    /// Number of consecutive failures before opening the circuit.
    #[serde(default = "default_cb_failure_threshold")]
    pub failure_threshold: u64,
    /// Time to stay open before probing in half-open state.
    #[serde(default = "default_cb_open_ms")]
    pub open_ms: u64,
    /// Number of successful probe requests required to close from half-open.
    #[serde(default = "default_cb_half_open_max_requests")]
    pub half_open_max_requests: u64,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            failure_threshold: default_cb_failure_threshold(),
            open_ms: default_cb_open_ms(),
            half_open_max_requests: default_cb_half_open_max_requests(),
        }
    }
}

/// Hot-key protection via single-flight GET coalescing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotKeyConfig {
    /// Enable GET coalescing for same-key concurrent requests.
    #[serde(default)]
    pub enabled: bool,
    /// Maximum number of waiting followers per in-flight key.
    #[serde(default = "default_hot_key_max_waiters")]
    pub max_waiters: usize,
}

impl Default for HotKeyConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_waiters: default_hot_key_max_waiters(),
        }
    }
}

/// File logging configuration.
///
/// When `enabled = true`, log records are written to rolling files in `path`
/// in addition to stderr.  Mirrors the `[backup]` section structure so that
/// operators have a consistent mental model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogConfig {
    /// Write logs to files inside `path` in addition to stderr.
    #[serde(default)]
    pub enabled: bool,
    /// Directory where log files are written (created if absent).
    #[serde(default = "default_log_path")]
    pub path: String,
    /// File rotation interval: `"hourly"` | `"daily"` | `"never"`.
    #[serde(default = "default_log_rotation")]
    pub rotation: String,
    /// Delete log files older than this many days (0 = keep forever).
    #[serde(default = "default_log_retain_days")]
    pub retain_days: u64,
    /// Minimum log level written to file: `"trace"` | `"debug"` | `"info"` | `"warn"` | `"error"`.
    /// Overridden by the `RUST_LOG` environment variable when set.
    #[serde(default = "default_log_level")]
    pub level: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: default_log_path(),
            rotation: default_log_rotation(),
            retain_days: default_log_retain_days(),
            level: default_log_level(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// Milliseconds to wait for all active-set ACKs on a write.
    pub write_timeout_ms: u64,
    /// Gossip heartbeat interval in milliseconds.
    pub gossip_interval_ms: u64,
    /// How many ms without a heartbeat before a node is marked offline.
    #[serde(default = "default_gossip_dead_ms")]
    pub gossip_dead_ms: u64,
    /// How often (ms) the version-check background task compares our log index
    /// against the primary's. 0 = disabled. Default: 30 000 ms.
    #[serde(default = "default_version_check_interval_ms")]
    pub version_check_interval_ms: u64,
}

fn default_cluster_bind_addr() -> String {
    "site-local".into()
}
fn default_max_nodes() -> usize {
    100
}
fn default_version_check_interval_ms() -> u64 {
    30_000
}
fn default_eviction_policy() -> String {
    "lfu".to_string()
}
fn default_gossip_dead_ms() -> u64 {
    15000
}
fn default_value_size_limit() -> u64 {
    100 * 1024 * 1024
} // 100 MiB
fn default_max_keys() -> usize {
    100_000
}
fn default_true() -> bool {
    true
}
fn default_compression_threshold() -> u64 {
    4096
}
fn default_backup_schedule() -> String {
    "0 2 * * *".into()
}
fn default_max_message_size() -> u32 {
    128 * 1024 * 1024
}
fn default_frame_read_timeout_ms() -> u64 {
    60_000
}
fn default_backup_path() -> String {
    "./backups".into()
}
fn default_backup_format() -> String {
    "binary".into()
}
fn default_retain_days() -> u64 {
    30
}
fn default_log_path() -> String {
    "./logs".into()
}
fn default_log_rotation() -> String {
    "daily".into()
}
fn default_log_retain_days() -> u64 {
    30
}
fn default_log_level() -> String {
    "info".into()
}
fn default_rate_limit_rps() -> u64 {
    1000
}
fn default_rate_limit_burst() -> u64 {
    2000
}
fn default_cb_failure_threshold() -> u64 {
    20
}
fn default_cb_open_ms() -> u64 {
    5000
}
fn default_cb_half_open_max_requests() -> u64 {
    5
}
fn default_hot_key_max_waiters() -> usize {
    128
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let raw = fs::read_to_string(path.as_ref())
            .with_context(|| format!("reading config {:?}", path.as_ref()))?;
        toml::from_str(&raw).context("parsing config TOML")
    }

    /// Persist the current config back to a TOML file.
    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent)?;
        }
        let raw = toml::to_string_pretty(self).context("serialising config")?;
        fs::write(path.as_ref(), raw).with_context(|| format!("writing config {:?}", path.as_ref()))
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node: NodeConfig {
                id: "node-1".into(),
                bind_addr: "0.0.0.0".into(),
                cluster_bind_addr: "site-local".into(),
                client_port: 7777,
                http_port: 7778,
                cluster_port: 7779,
                gossip_port: 7780,
                active: true,
                client_auth_token: None,
                max_message_size_bytes: default_max_message_size(),
                frame_read_timeout_ms: default_frame_read_timeout_ms(),
            },
            cluster: ClusterConfig {
                seeds: vec![],
                max_nodes: 100,
            },
            cache: CacheConfig {
                max_memory_mb: 512,
                default_ttl_secs: 0,
                eviction_policy: "lfu".into(),
                value_size_limit_bytes: default_value_size_limit(),
                max_keys: default_max_keys(),
            },
            replication: ReplicationConfig {
                write_timeout_ms: 500,
                gossip_interval_ms: 200,
                gossip_dead_ms: default_gossip_dead_ms(),
                version_check_interval_ms: 30_000,
            },
            compression: CompressionConfig::default(),
            tls: TlsConfig::default(),
            http_auth: HttpAuthConfig::default(),
            backup: BackupConfig::default(),
            persistence: PersistenceConfig::default(),
            rate_limit: RateLimitConfig::default(),
            circuit_breaker: CircuitBreakerConfig::default(),
            hot_key: HotKeyConfig::default(),
            log: LogConfig::default(),
        }
    }
}
