use crate::{NodeInfo, NodeStatus};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
    /// Trigger an immediate backup on this node (inactivate -> export -> activate).
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

impl From<AdminRequest> for crate::pb::AdminRequest {
    fn from(value: AdminRequest) -> Self {
        use crate::pb::admin_request::Request;

        let request = match value {
            AdminRequest::Describe => Request::Describe(crate::pb::Empty {}),
            AdminRequest::GetProperty { name } => {
                Request::GetProperty(crate::pb::Named { name })
            }
            AdminRequest::SetProperty { name, value } => {
                Request::SetProperty(crate::pb::PropertyUpdate { name, value })
            }
            AdminRequest::ListKeys { pattern } => Request::ListKeys(crate::pb::ListKeys {
                pattern: crate::opt_string(pattern),
            }),
            AdminRequest::GetStats => Request::GetStats(crate::pb::Empty {}),
            AdminRequest::GetKeyInfo { key } => Request::GetKeyInfo(crate::pb::KeyOnly { key }),
            AdminRequest::SetKeyProperty { key, name, value } => {
                Request::SetKeyProperty(crate::pb::KeyPropertyUpdate { key, name, value })
            }
            AdminRequest::FlushCache => Request::FlushCache(crate::pb::Empty {}),
            AdminRequest::ClusterStatus => Request::ClusterStatus(crate::pb::Empty {}),
            AdminRequest::BackupNow => Request::BackupNow(crate::pb::Empty {}),
            AdminRequest::RestoreLatestSnapshot => {
                Request::RestoreLatestSnapshot(crate::pb::Empty {})
            }
            AdminRequest::SetKeysTtl { pattern, ttl_secs } => {
                Request::SetKeysTtl(crate::pb::SetKeysTtl {
                    pattern,
                    ttl_secs: crate::opt_u64(ttl_secs),
                })
            }
        };

        Self {
            request: Some(request),
        }
    }
}

impl TryFrom<crate::pb::AdminRequest> for AdminRequest {
    type Error = anyhow::Error;

    fn try_from(value: crate::pb::AdminRequest) -> anyhow::Result<Self> {
        use crate::pb::admin_request::Request;

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
                pattern: crate::from_opt_string(v.pattern),
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
                ttl_secs: crate::from_opt_u64(v.ttl_secs),
            },
        })
    }
}

impl From<NamespaceQuotaUsage> for crate::pb::NamespaceQuotaUsage {
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

impl From<crate::pb::NamespaceQuotaUsage> for NamespaceQuotaUsage {
    fn from(value: crate::pb::NamespaceQuotaUsage) -> Self {
        Self {
            namespace: value.namespace,
            key_count: value.key_count,
            quota_limit: value.quota_limit,
            usage_pct: value.usage_pct,
            remaining_keys: value.remaining_keys,
        }
    }
}

impl From<NamespaceLatencySummary> for crate::pb::NamespaceLatencySummary {
    fn from(value: NamespaceLatencySummary) -> Self {
        Self {
            namespace: value.namespace,
            request_total: value.request_total,
            latency_p95_estimate_ms: crate::opt_u64(value.latency_p95_estimate_ms),
            latency_p99_estimate_ms: crate::opt_u64(value.latency_p99_estimate_ms),
        }
    }
}

impl From<crate::pb::NamespaceLatencySummary> for NamespaceLatencySummary {
    fn from(value: crate::pb::NamespaceLatencySummary) -> Self {
        Self {
            namespace: value.namespace,
            request_total: value.request_total,
            latency_p95_estimate_ms: crate::from_opt_u64(value.latency_p95_estimate_ms),
            latency_p99_estimate_ms: crate::from_opt_u64(value.latency_p99_estimate_ms),
        }
    }
}

impl From<HotKeyUsage> for crate::pb::HotKeyUsage {
    fn from(value: HotKeyUsage) -> Self {
        Self {
            key: value.key,
            request_total: value.request_total,
        }
    }
}

impl From<crate::pb::HotKeyUsage> for HotKeyUsage {
    fn from(value: crate::pb::HotKeyUsage) -> Self {
        Self {
            key: value.key,
            request_total: value.request_total,
        }
    }
}

impl From<NodeStats> for crate::pb::NodeStats {
    fn from(value: NodeStats) -> Self {
        Self {
            node_id: crate::uuid_to_string(value.node_id),
            status: crate::pb::NodeStatus::from(value.status) as i32,
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
            snapshot_last_load_path: crate::opt_string(value.snapshot_last_load_path),
            snapshot_last_load_duration_ms: value.snapshot_last_load_duration_ms,
            snapshot_last_load_entries: value.snapshot_last_load_entries,
            snapshot_last_load_age_secs: crate::opt_u64(value.snapshot_last_load_age_secs),
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
            hot_key_top_usage: value.hot_key_top_usage.into_iter().map(Into::into).collect(),
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
            client_latency_p50_estimate_ms: crate::opt_u64(value.client_latency_p50_estimate_ms),
            client_latency_p90_estimate_ms: crate::opt_u64(value.client_latency_p90_estimate_ms),
            client_latency_p95_estimate_ms: crate::opt_u64(value.client_latency_p95_estimate_ms),
            client_latency_p99_estimate_ms: crate::opt_u64(value.client_latency_p99_estimate_ms),
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

impl TryFrom<crate::pb::NodeStats> for NodeStats {
    type Error = anyhow::Error;

    fn try_from(value: crate::pb::NodeStats) -> anyhow::Result<Self> {
        Ok(Self {
            node_id: crate::uuid_from_string(value.node_id)?,
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
            snapshot_last_load_path: crate::from_opt_string(value.snapshot_last_load_path),
            snapshot_last_load_duration_ms: value.snapshot_last_load_duration_ms,
            snapshot_last_load_entries: value.snapshot_last_load_entries,
            snapshot_last_load_age_secs: crate::from_opt_u64(value.snapshot_last_load_age_secs),
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
            hot_key_top_usage: value.hot_key_top_usage.into_iter().map(Into::into).collect(),
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
            client_latency_p50_estimate_ms: crate::from_opt_u64(value.client_latency_p50_estimate_ms),
            client_latency_p90_estimate_ms: crate::from_opt_u64(value.client_latency_p90_estimate_ms),
            client_latency_p95_estimate_ms: crate::from_opt_u64(value.client_latency_p95_estimate_ms),
            client_latency_p99_estimate_ms: crate::from_opt_u64(value.client_latency_p99_estimate_ms),
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

impl From<AdminResponse> for crate::pb::AdminResponse {
    fn from(value: AdminResponse) -> Self {
        use crate::pb::admin_response::Response;

        let response = match value {
            AdminResponse::Properties(items) => Response::Properties(crate::pb::Properties {
                items: items
                    .into_iter()
                    .map(|(key, value)| crate::pb::PropertyPair { key, value })
                    .collect(),
            }),
            AdminResponse::Keys(keys) => Response::Keys(crate::pb::Keys { keys }),
            AdminResponse::Stats(stats) => Response::Stats((*stats).into()),
            AdminResponse::KeyInfo {
                key,
                value,
                version,
                ttl_remaining_secs,
                freq_count,
                compressed,
            } => Response::KeyInfo(crate::pb::KeyInfo {
                key,
                value: value.to_vec(),
                version,
                ttl_remaining_secs: crate::opt_u64(ttl_remaining_secs),
                freq_count,
                compressed,
            }),
            AdminResponse::KeyPropertyUpdated => {
                Response::KeyPropertyUpdated(crate::pb::Empty {})
            }
            AdminResponse::Flushed => Response::Flushed(crate::pb::Empty {}),
            AdminResponse::ClusterView(nodes) => Response::ClusterView(crate::pb::ClusterView {
                nodes: nodes.into_iter().map(Into::into).collect(),
            }),
            AdminResponse::BackupResult { path, bytes } => {
                Response::BackupResult(crate::pb::BackupResult { path, bytes })
            }
            AdminResponse::RestoreResult {
                path,
                entries,
                duration_ms,
            } => Response::RestoreResult(crate::pb::RestoreResult {
                path,
                entries,
                duration_ms,
            }),
            AdminResponse::TtlUpdated { updated } => {
                Response::TtlUpdated(crate::pb::CountResponse {
                    count: updated as u64,
                })
            }
            AdminResponse::Ok => Response::Ok(crate::pb::Empty {}),
            AdminResponse::NotFound => Response::NotFound(crate::pb::Empty {}),
            AdminResponse::Error { message } => {
                Response::Error(crate::pb::ErrorMessage { message })
            }
        };

        Self {
            response: Some(response),
        }
    }
}

impl TryFrom<crate::pb::AdminResponse> for AdminResponse {
    type Error = anyhow::Error;

    fn try_from(value: crate::pb::AdminResponse) -> anyhow::Result<Self> {
        use crate::pb::admin_response::Response;

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
                ttl_remaining_secs: crate::from_opt_u64(v.ttl_remaining_secs),
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
