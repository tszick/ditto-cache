#![recursion_limit = "256"]

mod backup;
mod config;
mod gossip;
mod network;
mod node;
mod replication;
mod store;

use anyhow::{Context, Result};
use config::{Config, LogConfig};
use gossip::GossipEngine;
use node::NodeHandle;
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

fn parse_bool_env(var: &str) -> Option<bool> {
    std::env::var(var)
        .ok()
        .map(|v| v.trim().eq_ignore_ascii_case("true"))
}

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
    if let Ok(v) = std::env::var("DITTO_NODE_ID") {
        config.node.id = v;
    }
    if let Ok(v) = std::env::var("DITTO_ACTIVE") {
        config.node.active = v.trim().eq_ignore_ascii_case("true");
    }
    if let Ok(v) = std::env::var("DITTO_CLIENT_AUTH_TOKEN") {
        if !v.is_empty() {
            config.node.client_auth_token = Some(v);
        }
    }
    if let Ok(v) = std::env::var("DITTO_SEEDS") {
        config.cluster.seeds = v
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
    }
    if let Ok(v) = std::env::var("DITTO_MAX_MEMORY_MB") {
        if let Ok(n) = v.parse::<u64>() {
            config.cache.max_memory_mb = n;
        }
    }
    if let Ok(v) = std::env::var("DITTO_TLS_ENABLED") {
        config.tls.enabled = v.trim().eq_ignore_ascii_case("true");
    }
    if let Ok(v) = std::env::var("DITTO_TLS_CA_CERT") {
        config.tls.ca_cert = v;
    }
    if let Ok(v) = std::env::var("DITTO_TLS_CERT") {
        config.tls.cert = v;
    }
    if let Ok(v) = std::env::var("DITTO_TLS_KEY") {
        config.tls.key = v;
    }
    if let Ok(v) = std::env::var("DITTO_GOSSIP_DEAD_MS") {
        if let Ok(ms) = v.parse::<u64>() {
            config.replication.gossip_dead_ms = ms;
        }
    }
    if let Some(v) = parse_bool_env("DITTO_READ_REPAIR_ON_MISS_ENABLED")
        .or_else(|| parse_bool_env("READ_REPAIR_ON_MISS_ENABLED"))
    {
        config.replication.read_repair_on_miss_enabled = v;
    }
    if let Ok(v) = std::env::var("DITTO_READ_REPAIR_MIN_INTERVAL_MS")
        .or_else(|_| std::env::var("READ_REPAIR_MIN_INTERVAL_MS"))
    {
        if let Ok(ms) = v.parse::<u64>() {
            config.replication.read_repair_min_interval_ms = ms.max(1);
        }
    }
    if let Some(v) = parse_bool_env("DITTO_ANTI_ENTROPY_ENABLED")
        .or_else(|| parse_bool_env("ANTI_ENTROPY_ENABLED"))
    {
        config.replication.anti_entropy_enabled = v;
    }
    if let Ok(v) = std::env::var("DITTO_ANTI_ENTROPY_INTERVAL_MS")
        .or_else(|_| std::env::var("ANTI_ENTROPY_INTERVAL_MS"))
    {
        if let Ok(ms) = v.parse::<u64>() {
            config.replication.anti_entropy_interval_ms = ms.max(1);
        }
    }
    if let Ok(v) = std::env::var("DITTO_ANTI_ENTROPY_LAG_THRESHOLD")
        .or_else(|_| std::env::var("ANTI_ENTROPY_LAG_THRESHOLD"))
    {
        if let Ok(n) = v.parse::<u64>() {
            config.replication.anti_entropy_lag_threshold = n.max(1);
        }
    }
    if let Ok(v) = std::env::var("DITTO_ANTI_ENTROPY_KEY_SAMPLE_SIZE")
        .or_else(|_| std::env::var("ANTI_ENTROPY_KEY_SAMPLE_SIZE"))
    {
        if let Ok(n) = v.parse::<usize>() {
            config.replication.anti_entropy_key_sample_size = n;
        }
    }
    if let Ok(v) = std::env::var("DITTO_ANTI_ENTROPY_FULL_RECONCILE_EVERY")
        .or_else(|_| std::env::var("ANTI_ENTROPY_FULL_RECONCILE_EVERY"))
    {
        if let Ok(n) = v.parse::<u64>() {
            config.replication.anti_entropy_full_reconcile_every = n;
        }
    }
    if let Ok(v) = std::env::var("DITTO_ANTI_ENTROPY_FULL_RECONCILE_MAX_KEYS")
        .or_else(|_| std::env::var("ANTI_ENTROPY_FULL_RECONCILE_MAX_KEYS"))
    {
        if let Ok(n) = v.parse::<usize>() {
            config.replication.anti_entropy_full_reconcile_max_keys = n;
        }
    }
    if let Some(v) = parse_bool_env("DITTO_MIXED_VERSION_PROBE_ENABLED")
        .or_else(|| parse_bool_env("MIXED_VERSION_PROBE_ENABLED"))
    {
        config.replication.mixed_version_probe_enabled = v;
    }
    if let Ok(v) = std::env::var("DITTO_MIXED_VERSION_PROBE_INTERVAL_MS")
        .or_else(|_| std::env::var("MIXED_VERSION_PROBE_INTERVAL_MS"))
    {
        if let Ok(ms) = v.parse::<u64>() {
            config.replication.mixed_version_probe_interval_ms = ms.max(1);
        }
    }
    if let Ok(v) = std::env::var("DITTO_HTTP_AUTH_USER") {
        config.http_auth.username = Some(v);
    }
    if let Ok(v) = std::env::var("DITTO_HTTP_AUTH_PASSWORD_HASH") {
        config.http_auth.password_hash = Some(v);
    }
    if let Ok(v) = std::env::var("DITTO_BACKUP_ENCRYPTION_KEY") {
        config.backup.encryption_key = Some(v);
    }
    if let Some(v) = parse_bool_env("DITTO_SNAPSHOT_RESTORE_ON_START")
        .or_else(|| parse_bool_env("SNAPSHOT_RESTORE_ON_START"))
    {
        config.backup.restore_on_start = v;
    }
    if let Some(v) = parse_bool_env("DITTO_PERSISTENCE_PLATFORM_ALLOWED")
        .or_else(|| parse_bool_env("PERSISTENCE_PLATFORM_ALLOWED"))
    {
        config.persistence.platform_allowed = v;
    }
    if let Some(v) = parse_bool_env("DITTO_PERSISTENCE_BACKUP_ALLOWED")
        .or_else(|| parse_bool_env("PERSISTENCE_BACKUP_ALLOWED"))
    {
        config.persistence.backup_allowed = v;
    }
    if let Some(v) = parse_bool_env("DITTO_PERSISTENCE_EXPORT_ALLOWED")
        .or_else(|| parse_bool_env("PERSISTENCE_EXPORT_ALLOWED"))
    {
        config.persistence.export_allowed = v;
    }
    if let Some(v) = parse_bool_env("DITTO_PERSISTENCE_IMPORT_ALLOWED")
        .or_else(|| parse_bool_env("PERSISTENCE_IMPORT_ALLOWED"))
    {
        config.persistence.import_allowed = v;
    }
    if let Some(v) =
        parse_bool_env("DITTO_TENANCY_ENABLED").or_else(|| parse_bool_env("TENANCY_ENABLED"))
    {
        config.tenancy.enabled = v;
    }
    if let Ok(v) = std::env::var("DITTO_TENANCY_DEFAULT_NAMESPACE")
        .or_else(|_| std::env::var("TENANCY_DEFAULT_NAMESPACE"))
    {
        if !v.trim().is_empty() {
            config.tenancy.default_namespace = v.trim().to_string();
        }
    }
    if let Ok(v) = std::env::var("DITTO_TENANCY_MAX_KEYS_PER_NAMESPACE")
        .or_else(|_| std::env::var("TENANCY_MAX_KEYS_PER_NAMESPACE"))
    {
        if let Ok(n) = v.parse::<usize>() {
            config.tenancy.max_keys_per_namespace = n;
        }
    }
    if let Some(v) =
        parse_bool_env("DITTO_RATE_LIMIT_ENABLED").or_else(|| parse_bool_env("RATE_LIMIT_ENABLED"))
    {
        config.rate_limit.enabled = v;
    }
    if let Ok(v) = std::env::var("DITTO_RATE_LIMIT_REQUESTS_PER_SEC")
        .or_else(|_| std::env::var("RATE_LIMIT_REQUESTS_PER_SEC"))
    {
        if let Ok(n) = v.parse::<u64>() {
            config.rate_limit.requests_per_sec = n.max(1);
        }
    }
    if let Ok(v) =
        std::env::var("DITTO_RATE_LIMIT_BURST").or_else(|_| std::env::var("RATE_LIMIT_BURST"))
    {
        if let Ok(n) = v.parse::<u64>() {
            config.rate_limit.burst = n.max(1);
        }
    }
    if let Some(v) = parse_bool_env("DITTO_CIRCUIT_BREAKER_ENABLED")
        .or_else(|| parse_bool_env("CIRCUIT_BREAKER_ENABLED"))
    {
        config.circuit_breaker.enabled = v;
    }
    if let Ok(v) = std::env::var("DITTO_CIRCUIT_BREAKER_FAILURE_THRESHOLD")
        .or_else(|_| std::env::var("CIRCUIT_BREAKER_FAILURE_THRESHOLD"))
    {
        if let Ok(n) = v.parse::<u64>() {
            config.circuit_breaker.failure_threshold = n.max(1);
        }
    }
    if let Ok(v) = std::env::var("DITTO_CIRCUIT_BREAKER_OPEN_MS")
        .or_else(|_| std::env::var("CIRCUIT_BREAKER_OPEN_MS"))
    {
        if let Ok(n) = v.parse::<u64>() {
            config.circuit_breaker.open_ms = n.max(1);
        }
    }
    if let Ok(v) = std::env::var("DITTO_CIRCUIT_BREAKER_HALF_OPEN_MAX_REQUESTS")
        .or_else(|_| std::env::var("CIRCUIT_BREAKER_HALF_OPEN_MAX_REQUESTS"))
    {
        if let Ok(n) = v.parse::<u64>() {
            config.circuit_breaker.half_open_max_requests = n.max(1);
        }
    }
    if let Some(v) =
        parse_bool_env("DITTO_HOT_KEY_ENABLED").or_else(|| parse_bool_env("HOT_KEY_ENABLED"))
    {
        config.hot_key.enabled = v;
    }
    if let Ok(v) =
        std::env::var("DITTO_HOT_KEY_MAX_WAITERS").or_else(|_| std::env::var("HOT_KEY_MAX_WAITERS"))
    {
        if let Ok(n) = v.parse::<usize>() {
            config.hot_key.max_waiters = n.max(1);
        }
    }
    if let Ok(v) = std::env::var("DITTO_HOT_KEY_FOLLOWER_WAIT_TIMEOUT_MS")
        .or_else(|_| std::env::var("HOT_KEY_FOLLOWER_WAIT_TIMEOUT_MS"))
    {
        if let Ok(n) = v.parse::<u64>() {
            config.hot_key.follower_wait_timeout_ms = n.max(1);
        }
    }
    if let Ok(v) = std::env::var("DITTO_HOT_KEY_STALE_TTL_MS")
        .or_else(|_| std::env::var("HOT_KEY_STALE_TTL_MS"))
    {
        if let Ok(n) = v.parse::<u64>() {
            config.hot_key.stale_ttl_ms = n;
        }
    }
    if let Ok(v) = std::env::var("DITTO_HOT_KEY_STALE_MAX_ENTRIES")
        .or_else(|_| std::env::var("HOT_KEY_STALE_MAX_ENTRIES"))
    {
        if let Ok(n) = v.parse::<usize>() {
            config.hot_key.stale_max_entries = n.max(1);
        }
    }
    if let Ok(v) = std::env::var("DITTO_BIND_ADDR") {
        config.node.bind_addr = v;
    }
    if let Ok(v) = std::env::var("DITTO_CLUSTER_BIND_ADDR") {
        config.node.cluster_bind_addr = v;
    }
    if let Ok(v) = std::env::var("DITTO_FRAME_READ_TIMEOUT_MS")
        .or_else(|_| std::env::var("FRAME_READ_TIMEOUT_MS"))
    {
        if let Ok(ms) = v.parse::<u64>() {
            config.node.frame_read_timeout_ms = ms.max(1);
        }
    }
    apply_replication_guardrails(&mut config);

    // DITTO_INSECURE=true bypasses strict security checks (dev/test only).
    let insecure = std::env::var("DITTO_INSECURE")
        .unwrap_or_default()
        .eq_ignore_ascii_case("true");

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

    // Logging — initialised after config so the file appender path / rotation
    // can be read from LogConfig.  _appender_guard must stay alive until main()
    // returns or the async writer thread is shut down prematurely.
    let _appender_guard = init_logging(&config.log)?;

    if config_missing {
        info!("Config file '{}' not found, using defaults.", config_path);
    }

    // Resolve bind addresses early so startup fails fast with a clear error
    // when "site-local" is requested but no private interface is available.
    let resolved_bind = ditto_config::resolve_bind_addr(&config.node.bind_addr)
        .context("resolving node.bind_addr")?;
    let resolved_cluster_bind = ditto_config::resolve_bind_addr(&config.node.cluster_bind_addr)
        .context("resolving node.cluster_bind_addr")?;

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
        match backup::restore_latest_snapshot(&*node, &config.backup) {
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
    use super::apply_replication_guardrails;
    use crate::config::Config;

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
}
