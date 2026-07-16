use crate::config::{Config, WriteQuorumMode};
use tracing::warn;

fn first_override<F>(get: &F, primary: &str, fallback: Option<&str>) -> Option<String>
where
    F: Fn(&str) -> Option<String>,
{
    get(primary).or_else(|| fallback.and_then(get))
}

fn bool_override<F>(get: &F, primary: &str, fallback: Option<&str>) -> Option<bool>
where
    F: Fn(&str) -> Option<String>,
{
    first_override(get, primary, fallback).map(|v| v.trim().eq_ignore_ascii_case("true"))
}

pub fn apply_env_overrides(config: &mut Config) {
    apply_env_overrides_with(config, |var| std::env::var(var).ok());
}

pub fn apply_env_overrides_with<F>(config: &mut Config, get: F)
where
    F: Fn(&str) -> Option<String>,
{
    if let Some(v) = get("DITTO_NODE_ID") {
        config.node.id = v;
    }
    if let Some(v) = get("DITTO_ACTIVE") {
        config.node.active = v.trim().eq_ignore_ascii_case("true");
    }
    if let Some(v) = get("DITTO_CLIENT_AUTH_TOKEN") {
        if !v.is_empty() {
            config.node.client_auth_token = Some(v);
        }
    }
    if let Some(v) = get("DITTO_SEEDS") {
        config.cluster.seeds = v
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
    }
    if let Some(v) = get("DITTO_MAX_MEMORY_MB") {
        if let Ok(n) = v.parse::<u64>() {
            config.cache.max_memory_mb = n;
        }
    }
    if let Some(v) = get("DITTO_TLS_ENABLED") {
        config.tls.enabled = v.trim().eq_ignore_ascii_case("true");
    }
    if let Some(v) = get("DITTO_TLS_CA_CERT") {
        config.tls.ca_cert = v;
    }
    if let Some(v) = get("DITTO_TLS_CERT") {
        config.tls.cert = v;
    }
    if let Some(v) = get("DITTO_TLS_KEY") {
        config.tls.key = v;
    }
    if let Some(v) = get("DITTO_GOSSIP_DEAD_MS") {
        if let Ok(ms) = v.parse::<u64>() {
            config.replication.gossip_dead_ms = ms;
        }
    }
    if let Some(v) = get("DITTO_WRITE_QUORUM_MODE") {
    if let Some(v) = get("DITTO_GOSSIP_AUTH_SECRET") {
        if !v.is_empty() {
            config.replication.gossip_auth_secret = Some(v);
        }
    }
        let normalized = v.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "all-active" => config.replication.write_quorum_mode = WriteQuorumMode::AllActive,
            "majority" => config.replication.write_quorum_mode = WriteQuorumMode::Majority,
            _ => warn!(
                "Ignoring invalid DITTO_WRITE_QUORUM_MODE='{}' (allowed: all-active|majority)",
                v
            ),
        }
    }
    if let Some(v) = bool_override(
        &get,
        "DITTO_READ_REPAIR_ON_MISS_ENABLED",
        Some("READ_REPAIR_ON_MISS_ENABLED"),
    ) {
        config.replication.read_repair_on_miss_enabled = v;
    }
    if let Some(v) = first_override(
        &get,
        "DITTO_READ_REPAIR_MIN_INTERVAL_MS",
        Some("READ_REPAIR_MIN_INTERVAL_MS"),
    ) {
        if let Ok(ms) = v.parse::<u64>() {
            config.replication.read_repair_min_interval_ms = ms.max(1);
        }
    }
    if let Some(v) = first_override(
        &get,
        "DITTO_READ_REPAIR_MAX_PER_MINUTE",
        Some("READ_REPAIR_MAX_PER_MINUTE"),
    ) {
        if let Ok(n) = v.parse::<u64>() {
            config.replication.read_repair_max_per_minute = n;
        }
    }
    if let Some(v) = bool_override(
        &get,
        "DITTO_ANTI_ENTROPY_ENABLED",
        Some("ANTI_ENTROPY_ENABLED"),
    ) {
        config.replication.anti_entropy_enabled = v;
    }
    if let Some(v) = first_override(
        &get,
        "DITTO_ANTI_ENTROPY_INTERVAL_MS",
        Some("ANTI_ENTROPY_INTERVAL_MS"),
    ) {
        if let Ok(ms) = v.parse::<u64>() {
            config.replication.anti_entropy_interval_ms = ms.max(1);
        }
    }
    if let Some(v) = first_override(
        &get,
        "DITTO_ANTI_ENTROPY_MIN_REPAIR_INTERVAL_MS",
        Some("ANTI_ENTROPY_MIN_REPAIR_INTERVAL_MS"),
    ) {
        if let Ok(ms) = v.parse::<u64>() {
            config.replication.anti_entropy_min_repair_interval_ms = ms.max(1);
        }
    }
    if let Some(v) = first_override(
        &get,
        "DITTO_ANTI_ENTROPY_LAG_THRESHOLD",
        Some("ANTI_ENTROPY_LAG_THRESHOLD"),
    ) {
        if let Ok(n) = v.parse::<u64>() {
            config.replication.anti_entropy_lag_threshold = n.max(1);
        }
    }
    if let Some(v) = first_override(
        &get,
        "DITTO_ANTI_ENTROPY_KEY_SAMPLE_SIZE",
        Some("ANTI_ENTROPY_KEY_SAMPLE_SIZE"),
    ) {
        if let Ok(n) = v.parse::<usize>() {
            config.replication.anti_entropy_key_sample_size = n;
        }
    }
    if let Some(v) = first_override(
        &get,
        "DITTO_ANTI_ENTROPY_FULL_RECONCILE_EVERY",
        Some("ANTI_ENTROPY_FULL_RECONCILE_EVERY"),
    ) {
        if let Ok(n) = v.parse::<u64>() {
            config.replication.anti_entropy_full_reconcile_every = n;
        }
    }
    if let Some(v) = first_override(
        &get,
        "DITTO_ANTI_ENTROPY_FULL_RECONCILE_MAX_KEYS",
        Some("ANTI_ENTROPY_FULL_RECONCILE_MAX_KEYS"),
    ) {
        if let Ok(n) = v.parse::<usize>() {
            config.replication.anti_entropy_full_reconcile_max_keys = n;
        }
    }
    if let Some(v) = first_override(
        &get,
        "DITTO_ANTI_ENTROPY_BUDGET_MAX_CHECKS_PER_RUN",
        Some("ANTI_ENTROPY_BUDGET_MAX_CHECKS_PER_RUN"),
    ) {
        if let Ok(n) = v.parse::<usize>() {
            config.replication.anti_entropy_budget_max_checks_per_run = n;
        }
    }
    if let Some(v) = first_override(
        &get,
        "DITTO_ANTI_ENTROPY_BUDGET_MAX_DURATION_MS",
        Some("ANTI_ENTROPY_BUDGET_MAX_DURATION_MS"),
    ) {
        if let Ok(ms) = v.parse::<u64>() {
            config.replication.anti_entropy_budget_max_duration_ms = ms;
        }
    }
    if let Some(v) = bool_override(
        &get,
        "DITTO_MIXED_VERSION_PROBE_ENABLED",
        Some("MIXED_VERSION_PROBE_ENABLED"),
    ) {
        config.replication.mixed_version_probe_enabled = v;
    }
    if let Some(v) = first_override(
        &get,
        "DITTO_MIXED_VERSION_PROBE_INTERVAL_MS",
        Some("MIXED_VERSION_PROBE_INTERVAL_MS"),
    ) {
        if let Ok(ms) = v.parse::<u64>() {
            config.replication.mixed_version_probe_interval_ms = ms.max(1);
        }
    }
    if let Some(v) = get("DITTO_HTTP_AUTH_USER") {
        config.http_auth.username = Some(v);
    }
    if let Some(v) = get("DITTO_HTTP_AUTH_PASSWORD_HASH") {
        config.http_auth.password_hash = Some(v);
    }
    if let Some(v) = get("DITTO_BACKUP_ENCRYPTION_KEY") {
        config.backup.encryption_key = Some(v);
    }
    if let Some(v) = get("DITTO_BACKUP_ENCRYPTION_KEY_ENV") {
        config.backup.encryption_key_env = Some(v);
    }
    if let Some(v) = get("DITTO_BACKUP_ENCRYPTION_KEY_FILE") {
        config.backup.encryption_key_file = Some(v);
    }
    if let Some(v) = get("DITTO_BACKUP_MAX_SNAPSHOT_BYTES") {
        if let Ok(n) = v.parse::<u64>() {
            config.backup.max_snapshot_bytes = n;
        }
    }
    if let Some(v) = get("DITTO_BACKUP_MAX_RESTORE_ENTRIES") {
        if let Ok(n) = v.parse::<usize>() {
            config.backup.max_restore_entries = n;
        }
    }
    if let Some(v) = bool_override(
        &get,
        "DITTO_SNAPSHOT_RESTORE_ON_START",
        Some("SNAPSHOT_RESTORE_ON_START"),
    ) {
        config.backup.restore_on_start = v;
    }
    if let Some(v) = bool_override(
        &get,
        "DITTO_PERSISTENCE_PLATFORM_ALLOWED",
        Some("PERSISTENCE_PLATFORM_ALLOWED"),
    ) {
        config.persistence.platform_allowed = v;
    }
    if let Some(v) = bool_override(
        &get,
        "DITTO_PERSISTENCE_BACKUP_ALLOWED",
        Some("PERSISTENCE_BACKUP_ALLOWED"),
    ) {
        config.persistence.backup_allowed = v;
    }
    if let Some(v) = bool_override(
        &get,
        "DITTO_PERSISTENCE_EXPORT_ALLOWED",
        Some("PERSISTENCE_EXPORT_ALLOWED"),
    ) {
        config.persistence.export_allowed = v;
    }
    if let Some(v) = bool_override(
        &get,
        "DITTO_PERSISTENCE_IMPORT_ALLOWED",
        Some("PERSISTENCE_IMPORT_ALLOWED"),
    ) {
        config.persistence.import_allowed = v;
    }
    if let Some(v) = bool_override(&get, "DITTO_TENANCY_ENABLED", Some("TENANCY_ENABLED")) {
        config.tenancy.enabled = v;
    }
    if let Some(v) = first_override(
        &get,
        "DITTO_TENANCY_DEFAULT_NAMESPACE",
        Some("TENANCY_DEFAULT_NAMESPACE"),
    ) {
        if !v.trim().is_empty() {
            config.tenancy.default_namespace = v.trim().to_string();
        }
    }
    if let Some(v) = first_override(
        &get,
        "DITTO_TENANCY_MAX_KEYS_PER_NAMESPACE",
        Some("TENANCY_MAX_KEYS_PER_NAMESPACE"),
    ) {
        if let Ok(n) = v.parse::<usize>() {
            config.tenancy.max_keys_per_namespace = n;
        }
    }
    if let Some(v) = bool_override(&get, "DITTO_RATE_LIMIT_ENABLED", Some("RATE_LIMIT_ENABLED")) {
        config.rate_limit.enabled = v;
    }
    if let Some(v) = first_override(
        &get,
        "DITTO_RATE_LIMIT_REQUESTS_PER_SEC",
        Some("RATE_LIMIT_REQUESTS_PER_SEC"),
    ) {
        if let Ok(n) = v.parse::<u64>() {
            config.rate_limit.requests_per_sec = n.max(1);
        }
    }
    if let Some(v) = first_override(&get, "DITTO_RATE_LIMIT_BURST", Some("RATE_LIMIT_BURST")) {
        if let Ok(n) = v.parse::<u64>() {
            config.rate_limit.burst = n.max(1);
        }
    }
    if let Some(v) = bool_override(
        &get,
        "DITTO_CIRCUIT_BREAKER_ENABLED",
        Some("CIRCUIT_BREAKER_ENABLED"),
    ) {
        config.circuit_breaker.enabled = v;
    }
    if let Some(v) = first_override(
        &get,
        "DITTO_CIRCUIT_BREAKER_FAILURE_THRESHOLD",
        Some("CIRCUIT_BREAKER_FAILURE_THRESHOLD"),
    ) {
        if let Ok(n) = v.parse::<u64>() {
            config.circuit_breaker.failure_threshold = n.max(1);
        }
    }
    if let Some(v) = first_override(
        &get,
        "DITTO_CIRCUIT_BREAKER_OPEN_MS",
        Some("CIRCUIT_BREAKER_OPEN_MS"),
    ) {
        if let Ok(n) = v.parse::<u64>() {
            config.circuit_breaker.open_ms = n.max(1);
        }
    }
    if let Some(v) = first_override(
        &get,
        "DITTO_CIRCUIT_BREAKER_HALF_OPEN_MAX_REQUESTS",
        Some("CIRCUIT_BREAKER_HALF_OPEN_MAX_REQUESTS"),
    ) {
        if let Ok(n) = v.parse::<u64>() {
            config.circuit_breaker.half_open_max_requests = n.max(1);
        }
    }
    if let Some(v) = bool_override(&get, "DITTO_HOT_KEY_ENABLED", Some("HOT_KEY_ENABLED")) {
        config.hot_key.enabled = v;
    }
    if let Some(v) = first_override(
        &get,
        "DITTO_HOT_KEY_MAX_WAITERS",
        Some("HOT_KEY_MAX_WAITERS"),
    ) {
        if let Ok(n) = v.parse::<usize>() {
            config.hot_key.max_waiters = n.max(1);
        }
    }
    if let Some(v) = first_override(
        &get,
        "DITTO_HOT_KEY_FOLLOWER_WAIT_TIMEOUT_MS",
        Some("HOT_KEY_FOLLOWER_WAIT_TIMEOUT_MS"),
    ) {
        if let Ok(n) = v.parse::<u64>() {
            config.hot_key.follower_wait_timeout_ms = n.max(1);
        }
    }
    if let Some(v) = first_override(
        &get,
        "DITTO_HOT_KEY_STALE_TTL_MS",
        Some("HOT_KEY_STALE_TTL_MS"),
    ) {
        if let Ok(n) = v.parse::<u64>() {
            config.hot_key.stale_ttl_ms = n;
        }
    }
    if let Some(v) = first_override(
        &get,
        "DITTO_HOT_KEY_STALE_MAX_ENTRIES",
        Some("HOT_KEY_STALE_MAX_ENTRIES"),
    ) {
        if let Ok(n) = v.parse::<usize>() {
            config.hot_key.stale_max_entries = n.max(1);
        }
    }
    if let Some(v) = bool_override(
        &get,
        "DITTO_HOT_KEY_ADAPTIVE_WAITERS_ENABLED",
        Some("HOT_KEY_ADAPTIVE_WAITERS_ENABLED"),
    ) {
        config.hot_key.adaptive_waiters_enabled = v;
    }
    if let Some(v) = first_override(
        &get,
        "DITTO_HOT_KEY_ADAPTIVE_MIN_WAITERS",
        Some("HOT_KEY_ADAPTIVE_MIN_WAITERS"),
    ) {
        if let Ok(n) = v.parse::<usize>() {
            config.hot_key.adaptive_min_waiters = n.max(1);
        }
    }
    if let Some(v) = first_override(
        &get,
        "DITTO_HOT_KEY_ADAPTIVE_SUCCESS_THRESHOLD",
        Some("HOT_KEY_ADAPTIVE_SUCCESS_THRESHOLD"),
    ) {
        if let Ok(n) = v.parse::<u32>() {
            config.hot_key.adaptive_success_threshold = n.max(1);
        }
    }
    if let Some(v) = first_override(
        &get,
        "DITTO_HOT_KEY_ADAPTIVE_STATE_MAX_KEYS",
        Some("HOT_KEY_ADAPTIVE_STATE_MAX_KEYS"),
    ) {
        if let Ok(n) = v.parse::<usize>() {
            config.hot_key.adaptive_state_max_keys = n.max(1);
        }
    }
    if let Some(v) = get("DITTO_BIND_ADDR") {
        config.node.bind_addr = v;
    }
    if let Some(v) = get("DITTO_CLUSTER_BIND_ADDR") {
        config.node.cluster_bind_addr = v;
    }
    if let Some(v) = first_override(
        &get,
        "DITTO_FRAME_READ_TIMEOUT_MS",
        Some("FRAME_READ_TIMEOUT_MS"),
    ) {
        if let Ok(ms) = v.parse::<u64>() {
            config.node.frame_read_timeout_ms = ms.max(1);
        }
    }
}
