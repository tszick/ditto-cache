#![recursion_limit = "256"]

mod admin_audit;
mod backup;
mod backup_support;
mod bootstrap;
mod config;
mod gossip;
mod network;
mod node;
mod replication;
mod runtime_property;
mod store;

use anyhow::{Context, Result};
#[cfg(test)]
use bootstrap::{apply_env_overrides_with, apply_replication_guardrails};
use bootstrap::{
    build_runtime_components, build_server_binds, init_logging, load_startup_state,
    rotate_old_logs, run_servers, start_background_tasks, start_gossip_engine,
    tcp_client_auth_required, validate_backup_encryption_policy,
};
use tracing::{info, warn};

const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> Result<()> {
    // Install the ring crypto provider for rustls (must happen before any TLS use).
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Config — load *before* logging so LogConfig can drive the file appender.
    let startup = load_startup_state()?;
    let config_path = startup.config_path;
    let config_missing = startup.config_missing;
    let config = startup.config;
    let insecure = startup.insecure;

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
        "Starting dittod v{}  node_id={}  active={}",
        APP_VERSION, config.node.id, config.node.active
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

    let runtime = build_runtime_components(&config, &config_path, &resolved_cluster_bind)?;
    let _store = runtime.store;
    let client_tls_acceptor = runtime.client_tls_acceptor;
    let tls_acceptor = runtime.tls_acceptor;
    let node = runtime.node;

    // Gossip engine
    let _gossip = start_gossip_engine(&config, &resolved_cluster_bind, &node).await?;

    start_background_tasks(&node, &config);
    let binds = build_server_binds(&config, &resolved_bind, &resolved_cluster_bind);

    // Optional server-only TLS for the HTTP REST port (uses the same cert as
    // the cluster port but without client-cert verification).
    let http_tls = if config.tls.enabled {
        Some((config.tls.cert.clone(), config.tls.key.clone()))
    } else {
        None
    };

    run_servers(
        binds,
        node.clone(),
        client_tls_acceptor,
        tls_acceptor,
        http_tls,
    )
    .await?;

    Ok(())
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
