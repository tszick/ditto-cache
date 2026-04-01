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
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

fn parse_bool_env(var: &str) -> Option<bool> {
    std::env::var(var).ok().map(|v| v.trim().eq_ignore_ascii_case("true"))
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
    if let Ok(v) = std::env::var("DITTO_HTTP_AUTH_USER") {
        config.http_auth.username = Some(v);
    }
    if let Ok(v) = std::env::var("DITTO_HTTP_AUTH_PASSWORD_HASH") {
        config.http_auth.password_hash = Some(v);
    }
    if let Ok(v) = std::env::var("DITTO_BACKUP_ENCRYPTION_KEY") {
        config.backup.encryption_key = Some(v);
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
    if let Ok(v) = std::env::var("DITTO_BIND_ADDR") {
        config.node.bind_addr = v;
    }
    if let Ok(v) = std::env::var("DITTO_CLUSTER_BIND_ADDR") {
        config.node.cluster_bind_addr = v;
    }

    // Strict security mode: cluster/admin traffic must use mTLS.
    if !config.tls.enabled {
        anyhow::bail!(
            "Strict security: [tls].enabled must be true. Refusing to start cluster/admin port without mTLS."
        );
    }

    // Strict security mode: REST API must be protected.
    if config.http_auth.password_hash.is_none() {
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
    let resolved_bind         = ditto_config::resolve_bind_addr(&config.node.bind_addr)
        .context("resolving node.bind_addr")?;
    let resolved_cluster_bind = ditto_config::resolve_bind_addr(&config.node.cluster_bind_addr)
        .context("resolving node.cluster_bind_addr")?;

    info!(
        "Starting dittod  node_id={}  active={}",
        config.node.id, config.node.active
    );
    info!(
        "Bind addresses — client: {} (bind_addr={})  cluster: {} (cluster_bind_addr={})",
        resolved_bind, config.node.bind_addr,
        resolved_cluster_bind, config.node.cluster_bind_addr,
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
        let acceptor  = network::tls::build_acceptor(&config.tls)?;
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

    // Gossip engine
    let gossip_addr: SocketAddr = format!(
        "{}:{}",
        resolved_cluster_bind, config.node.gossip_port
    )
    .parse()?;

    // Derive gossip addresses for seed nodes: replace the cluster port in each
    // seed string with the local gossip port.  Keep as strings so tokio can
    // resolve hostnames (e.g. "node-2") at send time – SocketAddr::parse()
    // rejects DNS names.
    let seed_gossip_addrs: Vec<String> = config.cluster.seeds
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
        ).await?,
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
        let n   = node.clone();
        let cfg = config.backup.clone();
        tokio::spawn(async move { backup::run_scheduler(n, cfg).await });
    }

    // Background log compaction (every 30 s, safe index = min active applied).
    node.clone().start_log_compaction();

    // Background version check: periodically compare our log index against the
    // primary and trigger a resync if we are lagging behind.
    node.clone().start_version_check();

    // Start servers concurrently.
    // Client-facing ports use bind_addr; cluster/gossip ports use cluster_bind_addr.
    let tcp_bind     = format!("{}:{}", resolved_bind, config.node.client_port);
    let http_bind    = format!("{}:{}", resolved_bind, config.node.http_port);
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
    let level_str = std::env::var("RUST_LOG")
        .unwrap_or_else(|_| format!("dittod={}", cfg.level));
    let filter = EnvFilter::try_new(&level_str)
        .unwrap_or_else(|_| EnvFilter::new("dittod=info"));

    let console = tracing_subscriber::fmt::layer()
        .with_target(false)
        .with_filter(filter.clone());

    let registry = tracing_subscriber::registry().with(console);

    if cfg.enabled {
        std::fs::create_dir_all(&cfg.path).map_err(|e| {
            anyhow::anyhow!("cannot create log directory {:?}: {}", cfg.path, e)
        })?;

        let rotation = match cfg.rotation.as_str() {
            "hourly" => tracing_appender::rolling::Rotation::HOURLY,
            "never"  => tracing_appender::rolling::Rotation::NEVER,
            _        => tracing_appender::rolling::Rotation::DAILY,
        };
        let appender = tracing_appender::rolling::RollingFileAppender::new(
            rotation,
            &cfg.path,
            "dittod.log",
        );
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

    let Ok(entries) = std::fs::read_dir(dir) else { return };
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
