#![recursion_limit = "256"]

mod admin_audit;
mod backup;
mod config;
mod gossip;
mod network;
mod node;
mod replication;
mod runtime_property;
mod store;

use anyhow::{Context, Result};
use config::{Config, LogConfig, WriteQuorumMode};
use gossip::GossipEngine;
use node::NodeHandle;
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

fn apply_replication_guardrails(config: &mut Config) {
    let interval_ms = config.replication.gossip_interval_ms.max(1);
    let min_dead_ms = interval_ms.saturating_mul(3);
    if config.replication.gossip_dead_ms < min_dead_ms {
        warn!(
            "gossip_dead_ms={}ms is lower than 3x gossip_interval_ms={}ms; clamping to {}ms",
            config.replication.gossip_dead_ms, interval_ms, min_dead_ms
        );
        config.replication.gossip_dead_ms = min_dead_ms;
    }
    if config.replication.gossip_dead_ms < 3000 {
        warn!(
            "gossip_dead_ms={}ms is aggressive and may cause false OFFLINE flapping under transient pauses",
            config.replication.gossip_dead_ms
        );
    }
    if config.replication.gossip_dead_ms > 30_000 {
        warn!(
            "gossip_dead_ms={}ms is high and may delay true failure detection",
            config.replication.gossip_dead_ms
        );
    }
}

fn tcp_client_auth_required(config: &Config, resolved_bind: &str, insecure: bool) -> bool {
    !insecure
        && !ditto_config::is_loopback_bind_addr(resolved_bind)
        && config.node.client_port != 0
        && config.node.client_auth_token.is_none()
}

fn validate_backup_encryption_policy(config: &Config, insecure: bool) -> Result<()> {
    if insecure {
        return Ok(());
    }

    let persistence_can_write_or_read_snapshots = config.backup.enabled
        || config.backup.restore_on_start
        || config.persistence.backup_allowed
        || config.persistence.import_allowed
        || config.persistence.export_allowed;
    if !persistence_can_write_or_read_snapshots {
        return Ok(());
    }

    let Some(key) = config.backup.encryption_key.as_deref() else {
        anyhow::bail!(
            "Strict security: backup.encryption_key must be configured when backup, export, or import persistence gates are enabled. Production backups/restores must be encrypted."
        );
    };

    let key_bytes = hex::decode(key.trim()).map_err(|e| {
        anyhow::anyhow!(
            "Strict security: backup.encryption_key is not valid hex: {}",
            e
        )
    })?;
    if key_bytes.len() != 32 {
        anyhow::bail!(
            "Strict security: backup.encryption_key must be 32 bytes (64 hex chars), got {} bytes.",
            key_bytes.len()
        );
    }

    Ok(())
}

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

fn apply_env_overrides(config: &mut Config) {
    apply_env_overrides_with(config, |var| std::env::var(var).ok());
}

fn apply_env_overrides_with<F>(config: &mut Config, get: F)
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

#[tokio::main]
async fn main() -> Result<()> {
    // Install the ring crypto provider for rustls (must happen before any TLS use).
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Config — load *before* logging so LogConfig can drive the file appender.
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "node.toml".to_string());

    let config_missing = !PathBuf::from(&config_path).exists();
    let mut config = if config_missing {
        Config::default()
    } else {
        Config::load(&config_path)?
    };

    // Apply environment variable overrides (used by Docker / CI).
    apply_env_overrides(&mut config);
    config.backup.resolve_encryption_key()?;
    apply_replication_guardrails(&mut config);

    // DITTO_INSECURE=true bypasses strict security checks (dev/test only).
    let insecure = std::env::var("DITTO_INSECURE")
        .unwrap_or_default()
        .eq_ignore_ascii_case("true");
    if insecure && !cfg!(debug_assertions) {
        anyhow::bail!(
            "DITTO_INSECURE is blocked in release builds. Use a debug/dev build for insecure local testing."
        );
    }

    // Logging — initialised after config so the file appender path / rotation
    // can be read from LogConfig.  _appender_guard must stay alive until main()
    // returns or the async writer thread is shut down prematurely.
    let _appender_guard = init_logging(&config.log)?;

    if config_missing {
        info!("Config file '{}' not found, using defaults.", config_path);
    }
    if insecure {
        warn!(
            "DITTO_INSECURE=true enabled: strict security startup checks are bypassed for local/dev testing only"
        );
    }

    // Resolve bind addresses early so startup fails fast with a clear error
    // when "site-local" is requested but no private interface is available.
    let resolved_bind = ditto_config::resolve_bind_addr(&config.node.bind_addr)
        .context("resolving node.bind_addr")?;
    let resolved_cluster_bind = ditto_config::resolve_bind_addr(&config.node.cluster_bind_addr)
        .context("resolving node.cluster_bind_addr")?;

    // Strict security mode: cluster/admin traffic must use mTLS.
    if !config.tls.enabled && !insecure {
        anyhow::bail!(
            "Strict security: [tls].enabled must be true. Refusing to start cluster/admin port without mTLS."
        );
    }

    // Strict security mode: REST API must be protected.
    if config.http_auth.password_hash.is_none() && !insecure {
        anyhow::bail!(
            "Strict security: [http_auth].password_hash must be configured. Refusing to start unauthenticated HTTP API."
        );
    }

    // Strict security mode: non-loopback TCP client exposure must require token auth.
    if tcp_client_auth_required(&config, &resolved_bind, insecure) {
        anyhow::bail!(
            "Strict security: non-loopback TCP client exposure requires [node].client_auth_token (or DITTO_CLIENT_AUTH_TOKEN). Bind to localhost for dev-only local access, or configure TCP auth before exposing port 7777."
        );
    }

    validate_backup_encryption_policy(&config, insecure)?;

    info!(
        "Starting dittod  node_id={}  active={}",
        config.node.id, config.node.active
    );
    info!(
        "Bind addresses — client: {} (bind_addr={})  cluster: {} (cluster_bind_addr={})",
        resolved_bind, config.node.bind_addr, resolved_cluster_bind, config.node.cluster_bind_addr,
    );

    // Log file cleanup task: hourly sweep that removes old rolling files.
    if config.log.enabled && config.log.retain_days > 0 {
        let log_cfg = config.log.clone();
        tokio::spawn(async move {
            let interval = tokio::time::Duration::from_secs(3600);
            loop {
                tokio::time::sleep(interval).await;
                rotate_old_logs(&log_cfg.path, log_cfg.retain_days);
            }
        });
    }

    // KV Store
    let store = Arc::new(store::KvStore::new(
        config.cache.max_memory_mb,
        config.cache.default_ttl_secs,
        config.cache.value_size_limit_bytes,
        config.cache.max_keys,
        config.compression.enabled,
        config.compression.threshold_bytes,
    ));

    // TTL sweep (every 5 seconds)
    store::ttl::start_ttl_sweep(store.clone(), 5);

    // TLS acceptor / connector (optional; only built when [tls] enabled = true)
    let (tls_acceptor, tls_connector) = if config.tls.enabled {
        let acceptor = network::tls::build_acceptor(&config.tls)?;
        let connector = network::tls::build_connector(&config.tls)?;
        info!("mTLS enabled on cluster/admin port");
        (Some(acceptor), Some(connector))
    } else {
        (None, None)
    };

    // Node handle (shared state)
    let node = NodeHandle::new(
        config.clone(),
        config_path.clone(),
        store.clone(),
        tls_connector,
        resolved_cluster_bind.clone(),
    );

    if config.backup.restore_on_start {
        match backup::restore_latest_snapshot(&node, &config.backup) {
            Ok(Some(loaded)) => {
                node.record_snapshot_restore(
                    loaded.path.clone(),
                    loaded.entries as u64,
                    loaded.duration_ms,
                );
                info!(
                    "Startup snapshot restore completed: path={} entries={} duration_ms={}",
                    loaded.path, loaded.entries, loaded.duration_ms
                );
            }
            Ok(None) => {
                info!("Startup snapshot restore enabled, but no snapshot file found.");
            }
            Err(e) => {
                warn!(
                    "Startup snapshot restore failed: {} (continuing without restore)",
                    e
                );
            }
        }
    }

    // Gossip engine
    let gossip_addr: SocketAddr =
        format!("{}:{}", resolved_cluster_bind, config.node.gossip_port).parse()?;

    // Derive gossip addresses for seed nodes: replace the cluster port in each
    // seed string with the local gossip port.  Keep as strings so tokio can
    // resolve hostnames (e.g. "node-2") at send time – SocketAddr::parse()
    // rejects DNS names.
    let seed_gossip_addrs: Vec<String> = config
        .cluster
        .seeds
        .iter()
        .filter_map(|seed| {
            let host = seed.split(':').next()?;
            Some(format!("{}:{}", host, config.node.gossip_port))
        })
        .collect();
    info!("Gossip bootstrap seeds: {:?}", seed_gossip_addrs);

    let gossip = Arc::new(
        GossipEngine::new(
            gossip_addr,
            node.active_set.clone(),
            config.replication.gossip_interval_ms,
            seed_gossip_addrs,
        )
        .await?,
    );
    gossip.start();
    info!("Gossip engine started on UDP {}", gossip_addr);

    // Node recovery: if we're behind, sync from a peer before joining active set.
    {
        let n = node.clone();
        tokio::spawn(async move { n.run_recovery().await });
    }

    // Backup scheduler (runs only if backup.enabled = true in config).
    {
        let n = node.clone();
        let cfg = config.backup.clone();
        tokio::spawn(async move { backup::run_scheduler(n, cfg).await });
    }

    // Background log compaction (every 30 s, safe index = min active applied).
    node.clone().start_log_compaction();

    // Background version check: periodically compare our log index against the
    // primary and trigger a resync if we are lagging behind.
    node.clone().start_version_check();
    // Background anti-entropy reconciliation (optional): lag-threshold based
    // periodic repair trigger.
    node.clone().start_anti_entropy();
    // Rolling-upgrade compatibility probe (non-intrusive): periodically checks
    // peer protocol-version property and reports mixed-version clusters.
    node.clone().start_mixed_version_probe();

    // Start servers concurrently.
    // Client-facing ports use bind_addr; cluster/gossip ports use cluster_bind_addr.
    let tcp_bind = format!("{}:{}", resolved_bind, config.node.client_port);
    let http_bind = format!("{}:{}", resolved_bind, config.node.http_port);
    let cluster_bind = format!("{}:{}", resolved_cluster_bind, config.node.cluster_port);

    // Optional server-only TLS for the HTTP REST port (uses the same cert as
    // the cluster port but without client-cert verification).
    let http_tls = if config.tls.enabled {
        Some((config.tls.cert.clone(), config.tls.key.clone()))
    } else {
        None
    };

    let n1 = node.clone();
    let n2 = node.clone();
    let n3 = node.clone();

    tokio::try_join!(
        network::tcp_server::start(tcp_bind, n1),
        network::http_server::start(http_bind, n2, http_tls),
        network::cluster_server::start(cluster_bind, n3, tls_acceptor),
    )?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Logging helpers
// ---------------------------------------------------------------------------

/// Initialise the tracing subscriber with an optional rolling-file layer.
///
/// Always installs a console (stderr) layer.  When `cfg.enabled = true` a
/// second layer writes to a rolling file in `cfg.path`.
///
/// The returned `WorkerGuard` **must be kept alive** for the entire lifetime
/// of `main()`.  Dropping it early causes the async writer thread to shut
/// down and queued records to be lost.
fn init_logging(cfg: &LogConfig) -> Result<tracing_appender::non_blocking::WorkerGuard> {
    // EnvFilter: RUST_LOG env var takes precedence; fallback to config level.
    let level_str = std::env::var("RUST_LOG").unwrap_or_else(|_| format!("dittod={}", cfg.level));
    let filter = EnvFilter::try_new(&level_str).unwrap_or_else(|_| EnvFilter::new("dittod=info"));

    let console = tracing_subscriber::fmt::layer()
        .with_target(false)
        .with_filter(filter.clone());

    let registry = tracing_subscriber::registry().with(console);

    if cfg.enabled {
        std::fs::create_dir_all(&cfg.path)
            .map_err(|e| anyhow::anyhow!("cannot create log directory {:?}: {}", cfg.path, e))?;

        let rotation = match cfg.rotation.as_str() {
            "hourly" => tracing_appender::rolling::Rotation::HOURLY,
            "never" => tracing_appender::rolling::Rotation::NEVER,
            _ => tracing_appender::rolling::Rotation::DAILY,
        };
        let appender =
            tracing_appender::rolling::RollingFileAppender::new(rotation, &cfg.path, "dittod.log");
        let (non_blocking, guard) = tracing_appender::non_blocking(appender);
        registry
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(non_blocking)
                    .with_ansi(false)
                    .with_filter(filter),
            )
            .init();
        Ok(guard)
    } else {
        // Sink guard: zero overhead, keeps the type uniform.
        let (_, guard) = tracing_appender::non_blocking(std::io::sink());
        registry.init();
        Ok(guard)
    }
}

/// Delete rolling log files older than `retain_days` days from `dir`.
///
/// Only files whose names start with `"dittod.log"` are examined so that
/// no other files in the directory are accidentally removed.
fn rotate_old_logs(dir: &str, retain_days: u64) {
    let cutoff = std::time::SystemTime::now()
        .checked_sub(std::time::Duration::from_secs(retain_days * 86_400))
        .unwrap_or(std::time::SystemTime::UNIX_EPOCH);

    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.starts_with("dittod.log"))
            .unwrap_or(false)
        {
            continue;
        }
        if let Ok(meta) = std::fs::metadata(&path) {
            if meta.modified().map(|t| t < cutoff).unwrap_or(false) {
                if let Err(e) = std::fs::remove_file(&path) {
                    tracing::warn!("log cleanup: could not remove {:?}: {}", path, e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        apply_env_overrides_with, apply_replication_guardrails, rotate_old_logs,
        tcp_client_auth_required, validate_backup_encryption_policy,
    };
    use crate::config::{Config, WriteQuorumMode};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_test_dir(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "ditto-main-test-{}-{}-{}",
            name,
            std::process::id(),
            nanos
        ))
    }

    #[test]
    fn guardrail_clamps_dead_threshold_to_at_least_three_intervals() {
        let mut cfg = Config::default();
        cfg.replication.gossip_interval_ms = 400;
        cfg.replication.gossip_dead_ms = 500;
        apply_replication_guardrails(&mut cfg);
        assert_eq!(cfg.replication.gossip_dead_ms, 1200);
    }

    #[test]
    fn guardrail_keeps_valid_dead_threshold_unchanged() {
        let mut cfg = Config::default();
        cfg.replication.gossip_interval_ms = 200;
        cfg.replication.gossip_dead_ms = 15000;
        apply_replication_guardrails(&mut cfg);
        assert_eq!(cfg.replication.gossip_dead_ms, 15000);
    }

    #[test]
    fn guardrail_handles_zero_interval_and_warn_only_extremes() {
        let mut low = Config::default();
        low.replication.gossip_interval_ms = 0;
        low.replication.gossip_dead_ms = 1;
        apply_replication_guardrails(&mut low);
        assert_eq!(low.replication.gossip_dead_ms, 3);

        let mut high = Config::default();
        high.replication.gossip_interval_ms = 10_000;
        high.replication.gossip_dead_ms = 90_000;
        apply_replication_guardrails(&mut high);
        assert_eq!(high.replication.gossip_dead_ms, 90_000);
    }

    #[test]
    fn env_overrides_cover_primary_and_legacy_startup_knobs() {
        let mut cfg = Config::default();
        let overrides = [
            ("DITTO_NODE_ID", "env-node"),
            ("DITTO_ACTIVE", "false"),
            ("DITTO_CLIENT_AUTH_TOKEN", "secret"),
            ("DITTO_SEEDS", " node-a:7779, ,node-b:7779 "),
            ("DITTO_MAX_MEMORY_MB", "64"),
            ("DITTO_TLS_ENABLED", "true"),
            ("DITTO_TLS_CA_CERT", "ca.pem"),
            ("DITTO_TLS_CERT", "cert.pem"),
            ("DITTO_TLS_KEY", "key.pem"),
            ("DITTO_GOSSIP_DEAD_MS", "12345"),
            ("DITTO_WRITE_QUORUM_MODE", "majority"),
            ("READ_REPAIR_ON_MISS_ENABLED", "true"),
            ("READ_REPAIR_MIN_INTERVAL_MS", "0"),
            ("READ_REPAIR_MAX_PER_MINUTE", "9"),
            ("ANTI_ENTROPY_ENABLED", "true"),
            ("ANTI_ENTROPY_INTERVAL_MS", "0"),
            ("ANTI_ENTROPY_MIN_REPAIR_INTERVAL_MS", "0"),
            ("ANTI_ENTROPY_LAG_THRESHOLD", "0"),
            ("ANTI_ENTROPY_KEY_SAMPLE_SIZE", "17"),
            ("ANTI_ENTROPY_FULL_RECONCILE_EVERY", "3"),
            ("ANTI_ENTROPY_FULL_RECONCILE_MAX_KEYS", "19"),
            ("ANTI_ENTROPY_BUDGET_MAX_CHECKS_PER_RUN", "23"),
            ("ANTI_ENTROPY_BUDGET_MAX_DURATION_MS", "29"),
            ("MIXED_VERSION_PROBE_ENABLED", "false"),
            ("MIXED_VERSION_PROBE_INTERVAL_MS", "0"),
            ("DITTO_HTTP_AUTH_USER", "admin"),
            ("DITTO_HTTP_AUTH_PASSWORD_HASH", "hash"),
            ("DITTO_BACKUP_ENCRYPTION_KEY", "enc-key"),
            ("DITTO_BACKUP_ENCRYPTION_KEY_ENV", "BACKUP_KEY"),
            (
                "DITTO_BACKUP_ENCRYPTION_KEY_FILE",
                "/run/secrets/backup-key",
            ),
            ("DITTO_BACKUP_MAX_SNAPSHOT_BYTES", "4096"),
            ("DITTO_BACKUP_MAX_RESTORE_ENTRIES", "31"),
            ("SNAPSHOT_RESTORE_ON_START", "true"),
            ("PERSISTENCE_PLATFORM_ALLOWED", "true"),
            ("PERSISTENCE_BACKUP_ALLOWED", "true"),
            ("PERSISTENCE_EXPORT_ALLOWED", "true"),
            ("PERSISTENCE_IMPORT_ALLOWED", "true"),
            ("TENANCY_ENABLED", "true"),
            ("TENANCY_DEFAULT_NAMESPACE", "tenant-a"),
            ("TENANCY_MAX_KEYS_PER_NAMESPACE", "37"),
            ("RATE_LIMIT_ENABLED", "true"),
            ("RATE_LIMIT_REQUESTS_PER_SEC", "0"),
            ("RATE_LIMIT_BURST", "0"),
            ("CIRCUIT_BREAKER_ENABLED", "true"),
            ("CIRCUIT_BREAKER_FAILURE_THRESHOLD", "0"),
            ("CIRCUIT_BREAKER_OPEN_MS", "0"),
            ("CIRCUIT_BREAKER_HALF_OPEN_MAX_REQUESTS", "0"),
            ("HOT_KEY_ENABLED", "true"),
            ("HOT_KEY_MAX_WAITERS", "0"),
            ("HOT_KEY_FOLLOWER_WAIT_TIMEOUT_MS", "0"),
            ("HOT_KEY_STALE_TTL_MS", "0"),
            ("HOT_KEY_STALE_MAX_ENTRIES", "0"),
            ("HOT_KEY_ADAPTIVE_WAITERS_ENABLED", "true"),
            ("HOT_KEY_ADAPTIVE_MIN_WAITERS", "0"),
            ("HOT_KEY_ADAPTIVE_SUCCESS_THRESHOLD", "0"),
            ("HOT_KEY_ADAPTIVE_STATE_MAX_KEYS", "0"),
            ("DITTO_BIND_ADDR", "127.0.0.2"),
            ("DITTO_CLUSTER_BIND_ADDR", "127.0.0.3"),
            ("FRAME_READ_TIMEOUT_MS", "0"),
        ];

        apply_env_overrides_with(&mut cfg, |name| {
            overrides
                .iter()
                .find(|(key, _)| key == &name)
                .map(|(_, value)| value.to_string())
        });

        assert_eq!(cfg.node.id, "env-node");
        assert!(!cfg.node.active);
        assert_eq!(cfg.node.client_auth_token.as_deref(), Some("secret"));
        assert_eq!(cfg.cluster.seeds, vec!["node-a:7779", "node-b:7779"]);
        assert_eq!(cfg.cache.max_memory_mb, 64);
        assert!(cfg.tls.enabled);
        assert_eq!(cfg.tls.ca_cert, "ca.pem");
        assert_eq!(cfg.tls.cert, "cert.pem");
        assert_eq!(cfg.tls.key, "key.pem");
        assert_eq!(cfg.replication.gossip_dead_ms, 12345);
        assert_eq!(cfg.replication.write_quorum_mode, WriteQuorumMode::Majority);
        assert!(cfg.replication.read_repair_on_miss_enabled);
        assert_eq!(cfg.replication.read_repair_min_interval_ms, 1);
        assert_eq!(cfg.replication.read_repair_max_per_minute, 9);
        assert!(cfg.replication.anti_entropy_enabled);
        assert_eq!(cfg.replication.anti_entropy_interval_ms, 1);
        assert_eq!(cfg.replication.anti_entropy_min_repair_interval_ms, 1);
        assert_eq!(cfg.replication.anti_entropy_lag_threshold, 1);
        assert_eq!(cfg.replication.anti_entropy_key_sample_size, 17);
        assert_eq!(cfg.replication.anti_entropy_full_reconcile_every, 3);
        assert_eq!(cfg.replication.anti_entropy_full_reconcile_max_keys, 19);
        assert_eq!(cfg.replication.anti_entropy_budget_max_checks_per_run, 23);
        assert_eq!(cfg.replication.anti_entropy_budget_max_duration_ms, 29);
        assert!(!cfg.replication.mixed_version_probe_enabled);
        assert_eq!(cfg.replication.mixed_version_probe_interval_ms, 1);
        assert_eq!(cfg.http_auth.username.as_deref(), Some("admin"));
        assert_eq!(cfg.http_auth.password_hash.as_deref(), Some("hash"));
        assert_eq!(cfg.backup.encryption_key.as_deref(), Some("enc-key"));
        assert_eq!(cfg.backup.encryption_key_env.as_deref(), Some("BACKUP_KEY"));
        assert_eq!(
            cfg.backup.encryption_key_file.as_deref(),
            Some("/run/secrets/backup-key")
        );
        assert_eq!(cfg.backup.max_snapshot_bytes, 4096);
        assert_eq!(cfg.backup.max_restore_entries, 31);
        assert!(cfg.backup.restore_on_start);
        assert!(cfg.persistence.platform_allowed);
        assert!(cfg.persistence.backup_allowed);
        assert!(cfg.persistence.export_allowed);
        assert!(cfg.persistence.import_allowed);
        assert!(cfg.tenancy.enabled);
        assert_eq!(cfg.tenancy.default_namespace, "tenant-a");
        assert_eq!(cfg.tenancy.max_keys_per_namespace, 37);
        assert!(cfg.rate_limit.enabled);
        assert_eq!(cfg.rate_limit.requests_per_sec, 1);
        assert_eq!(cfg.rate_limit.burst, 1);
        assert!(cfg.circuit_breaker.enabled);
        assert_eq!(cfg.circuit_breaker.failure_threshold, 1);
        assert_eq!(cfg.circuit_breaker.open_ms, 1);
        assert_eq!(cfg.circuit_breaker.half_open_max_requests, 1);
        assert!(cfg.hot_key.enabled);
        assert_eq!(cfg.hot_key.max_waiters, 1);
        assert_eq!(cfg.hot_key.follower_wait_timeout_ms, 1);
        assert_eq!(cfg.hot_key.stale_ttl_ms, 0);
        assert_eq!(cfg.hot_key.stale_max_entries, 1);
        assert!(cfg.hot_key.adaptive_waiters_enabled);
        assert_eq!(cfg.hot_key.adaptive_min_waiters, 1);
        assert_eq!(cfg.hot_key.adaptive_success_threshold, 1);
        assert_eq!(cfg.hot_key.adaptive_state_max_keys, 1);
        assert_eq!(cfg.node.bind_addr, "127.0.0.2");
        assert_eq!(cfg.node.cluster_bind_addr, "127.0.0.3");
        assert_eq!(cfg.node.frame_read_timeout_ms, 1);
    }

    #[test]
    fn env_overrides_ignore_invalid_numbers_and_empty_token() {
        let mut cfg = Config::default();
        let original_memory = cfg.cache.max_memory_mb;
        let original_snapshot_limit = cfg.backup.max_snapshot_bytes;
        let overrides = [
            ("DITTO_CLIENT_AUTH_TOKEN", ""),
            ("DITTO_MAX_MEMORY_MB", "not-a-number"),
            ("DITTO_BACKUP_MAX_SNAPSHOT_BYTES", "nope"),
            ("DITTO_WRITE_QUORUM_MODE", "invalid"),
            ("DITTO_TENANCY_DEFAULT_NAMESPACE", "   "),
        ];

        apply_env_overrides_with(&mut cfg, |name| {
            overrides
                .iter()
                .find(|(key, _)| key == &name)
                .map(|(_, value)| value.to_string())
        });

        assert!(cfg.node.client_auth_token.is_none());
        assert_eq!(cfg.cache.max_memory_mb, original_memory);
        assert_eq!(cfg.backup.max_snapshot_bytes, original_snapshot_limit);
        assert_eq!(
            cfg.replication.write_quorum_mode,
            WriteQuorumMode::AllActive
        );
        assert_eq!(cfg.tenancy.default_namespace, "default");
    }

    #[test]
    fn tcp_auth_required_for_non_loopback_bind_in_strict_mode() {
        let cfg = Config::default();
        assert!(tcp_client_auth_required(&cfg, "0.0.0.0", false));
        assert!(tcp_client_auth_required(&cfg, "192.168.1.10", false));
    }

    #[test]
    fn tcp_auth_not_required_for_loopback_bind() {
        let cfg = Config::default();
        assert!(!tcp_client_auth_required(&cfg, "127.0.0.1", false));
        assert!(!tcp_client_auth_required(&cfg, "localhost", false));
    }

    #[test]
    fn tcp_auth_not_required_when_token_is_configured() {
        let mut cfg = Config::default();
        cfg.node.client_auth_token = Some("secret".into());
        assert!(!tcp_client_auth_required(&cfg, "0.0.0.0", false));
    }

    #[test]
    fn tcp_auth_not_required_in_insecure_mode() {
        let cfg = Config::default();
        assert!(!tcp_client_auth_required(&cfg, "0.0.0.0", true));
    }

    #[test]
    fn tcp_auth_not_required_when_client_port_disabled() {
        let mut cfg = Config::default();
        cfg.node.client_port = 0;
        assert!(!tcp_client_auth_required(&cfg, "0.0.0.0", false));
    }

    #[test]
    fn backup_encryption_policy_requires_key_for_persistence_gates() {
        let mut cfg = Config::default();
        validate_backup_encryption_policy(&cfg, false).expect("disabled persistence should pass");

        cfg.persistence.platform_allowed = true;
        cfg.persistence.backup_allowed = true;
        let err = validate_backup_encryption_policy(&cfg, false)
            .expect_err("strict production backup gate requires encryption key");
        assert!(err.to_string().contains("backup.encryption_key"));

        cfg.backup.encryption_key = Some("abc123".into());
        let err =
            validate_backup_encryption_policy(&cfg, false).expect_err("short key should fail");
        assert!(err.to_string().contains("32 bytes"));

        cfg.backup.encryption_key =
            Some("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into());
        validate_backup_encryption_policy(&cfg, false).expect("valid key should pass");
    }

    #[test]
    fn backup_encryption_policy_allows_insecure_dev_bypass() {
        let mut cfg = Config::default();
        cfg.persistence.platform_allowed = true;
        cfg.persistence.import_allowed = true;
        validate_backup_encryption_policy(&cfg, true).expect("insecure dev mode bypasses policy");
    }

    #[test]
    fn rotate_old_logs_removes_only_dittod_log_files() {
        let dir = unique_test_dir("rotate-logs");
        std::fs::create_dir_all(&dir).expect("create log dir");
        let old_log = dir.join("dittod.log.2026-05-01");
        let current_log = dir.join("dittod.log");
        let unrelated = dir.join("other.log");
        std::fs::write(&old_log, b"old").expect("write old log");
        std::fs::write(&current_log, b"current").expect("write current log");
        std::fs::write(&unrelated, b"keep").expect("write unrelated log");

        rotate_old_logs(&dir.to_string_lossy(), 0);

        assert!(!old_log.exists());
        assert!(!current_log.exists());
        assert!(unrelated.exists());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn rotate_old_logs_ignores_missing_directories() {
        let dir = unique_test_dir("missing-logs");
        rotate_old_logs(&dir.to_string_lossy(), 1);
        assert!(!dir.exists());
    }
}
