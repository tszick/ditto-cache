use crate::{
    config::{Config, WriteQuorumMode},
    store::KvStore,
};
use std::sync::{Arc, Mutex};

#[derive(Debug, PartialEq, Eq)]
pub enum ApplyResult {
    Applied,
    Rejected(String),
    Unknown,
}

pub fn is_known(name: &str) -> bool {
    matches!(
        name,
        "max-memory"
            | "default-ttl"
            | "value-size-limit"
            | "max-keys"
            | "compression-enabled"
            | "compression-threshold"
            | "write-timeout-ms"
            | "write-quorum-mode"
            | "version-check-interval"
            | "read-repair-on-miss-enabled"
            | "read-repair-min-interval-ms"
            | "read-repair-max-per-minute"
            | "anti-entropy-enabled"
            | "anti-entropy-interval-ms"
            | "anti-entropy-min-repair-interval-ms"
            | "anti-entropy-lag-threshold"
            | "anti-entropy-key-sample-size"
            | "anti-entropy-full-reconcile-every"
            | "anti-entropy-full-reconcile-max-keys"
            | "anti-entropy-budget-max-checks-per-run"
            | "anti-entropy-budget-max-duration-ms"
            | "mixed-version-probe-enabled"
            | "mixed-version-probe-interval-ms"
            | "persistence-runtime-enabled"
            | "persistence-enabled"
            | "tenancy-enabled"
            | "tenancy-default-namespace"
            | "tenancy-max-keys-per-namespace"
            | "rate-limit-enabled"
            | "rate-limit-requests-per-sec"
            | "rate-limit-burst"
            | "hot-key-enabled"
            | "hot-key-max-waiters"
            | "hot-key-follower-wait-timeout-ms"
            | "hot-key-stale-ttl-ms"
            | "hot-key-stale-max-entries"
            | "hot-key-adaptive-waiters-enabled"
            | "hot-key-adaptive-min-waiters"
            | "hot-key-adaptive-success-threshold"
            | "hot-key-adaptive-state-max-keys"
            | "circuit-breaker-enabled"
            | "circuit-breaker-failure-threshold"
            | "circuit-breaker-open-ms"
            | "circuit-breaker-half-open-max-requests"
    )
}

pub fn apply(name: &str, value: &str, config: &Mutex<Config>, store: &Arc<KvStore>) -> ApplyResult {
    match name {
        "max-memory" => apply_max_memory(value, config, store),
        "default-ttl" => apply_u64(value, "default-ttl", config, |secs, cfg| {
            store.set_default_ttl_secs(secs);
            cfg.cache.default_ttl_secs = secs;
            tracing::info!("default-ttl -> {}s", secs);
        }),
        "value-size-limit" => apply_u64(value, "value-size-limit", config, |bytes, cfg| {
            store.set_value_size_limit(bytes);
            cfg.cache.value_size_limit_bytes = bytes;
            tracing::info!("value-size-limit -> {} bytes", bytes);
        }),
        "max-keys" => apply_usize(value, "max-keys", config, |n, cfg| {
            store.set_max_keys(n);
            cfg.cache.max_keys = n;
            tracing::info!("max-keys -> {}", n);
        }),
        "compression-enabled" => {
            let val = parse_bool(value);
            store.set_compression_enabled(val);
            config.lock().unwrap().compression.enabled = val;
            tracing::info!("compression-enabled -> {}", val);
            ApplyResult::Applied
        }
        "compression-threshold" => match value.trim().parse::<u64>() {
            Ok(bytes) => match store.set_compression_threshold(bytes) {
                Ok(new_val) => {
                    config.lock().unwrap().compression.threshold_bytes = new_val;
                    tracing::info!("compression-threshold -> {} bytes", new_val);
                    ApplyResult::Applied
                }
                Err(msg) => {
                    tracing::warn!("SetProperty compression-threshold: {}", msg);
                    ApplyResult::Rejected(msg.to_string())
                }
            },
            Err(_) => warn_invalid(value, "compression-threshold"),
        },
        "write-timeout-ms" => apply_positive_u64(value, "write-timeout-ms", config, |ms, cfg| {
            cfg.replication.write_timeout_ms = ms;
            tracing::info!("write-timeout-ms -> {}", ms);
        }),
        "write-quorum-mode" => apply_write_quorum_mode(value, config),
        "version-check-interval" => {
            apply_u64(value, "version-check-interval", config, |ms, cfg| {
                cfg.replication.version_check_interval_ms = ms;
                tracing::info!("version-check-interval -> {}ms", ms);
            })
        }
        "read-repair-on-miss-enabled" => {
            apply_bool(value, "read-repair-on-miss-enabled", config, |val, cfg| {
                cfg.replication.read_repair_on_miss_enabled = val;
                tracing::info!("read-repair-on-miss-enabled -> {}", val);
            })
        }
        "read-repair-min-interval-ms" => {
            apply_positive_u64(value, "read-repair-min-interval-ms", config, |ms, cfg| {
                cfg.replication.read_repair_min_interval_ms = ms;
                tracing::info!("read-repair-min-interval-ms -> {}", ms);
            })
        }
        "read-repair-max-per-minute" => {
            apply_u64(value, "read-repair-max-per-minute", config, |v, cfg| {
                cfg.replication.read_repair_max_per_minute = v;
                tracing::info!("read-repair-max-per-minute -> {}", v);
            })
        }
        "anti-entropy-enabled" => apply_bool(value, "anti-entropy-enabled", config, |val, cfg| {
            cfg.replication.anti_entropy_enabled = val;
            tracing::info!("anti-entropy-enabled -> {}", val);
        }),
        "anti-entropy-interval-ms" => {
            apply_positive_u64(value, "anti-entropy-interval-ms", config, |ms, cfg| {
                cfg.replication.anti_entropy_interval_ms = ms;
                tracing::info!("anti-entropy-interval-ms -> {}", ms);
            })
        }
        "anti-entropy-min-repair-interval-ms" => apply_positive_u64(
            value,
            "anti-entropy-min-repair-interval-ms",
            config,
            |ms, cfg| {
                cfg.replication.anti_entropy_min_repair_interval_ms = ms;
                tracing::info!("anti-entropy-min-repair-interval-ms -> {}", ms);
            },
        ),
        "anti-entropy-lag-threshold" => {
            apply_positive_u64(value, "anti-entropy-lag-threshold", config, |v, cfg| {
                cfg.replication.anti_entropy_lag_threshold = v;
                tracing::info!("anti-entropy-lag-threshold -> {}", v);
            })
        }
        "anti-entropy-key-sample-size" => {
            apply_usize(value, "anti-entropy-key-sample-size", config, |v, cfg| {
                cfg.replication.anti_entropy_key_sample_size = v;
                tracing::info!("anti-entropy-key-sample-size -> {}", v);
            })
        }
        "anti-entropy-full-reconcile-every" => apply_u64(
            value,
            "anti-entropy-full-reconcile-every",
            config,
            |v, cfg| {
                cfg.replication.anti_entropy_full_reconcile_every = v;
                tracing::info!("anti-entropy-full-reconcile-every -> {}", v);
            },
        ),
        "anti-entropy-full-reconcile-max-keys" => apply_usize(
            value,
            "anti-entropy-full-reconcile-max-keys",
            config,
            |v, cfg| {
                cfg.replication.anti_entropy_full_reconcile_max_keys = v;
                tracing::info!("anti-entropy-full-reconcile-max-keys -> {}", v);
            },
        ),
        "anti-entropy-budget-max-checks-per-run" => apply_usize(
            value,
            "anti-entropy-budget-max-checks-per-run",
            config,
            |v, cfg| {
                cfg.replication.anti_entropy_budget_max_checks_per_run = v;
                tracing::info!("anti-entropy-budget-max-checks-per-run -> {}", v);
            },
        ),
        "anti-entropy-budget-max-duration-ms" => apply_u64(
            value,
            "anti-entropy-budget-max-duration-ms",
            config,
            |v, cfg| {
                cfg.replication.anti_entropy_budget_max_duration_ms = v;
                tracing::info!("anti-entropy-budget-max-duration-ms -> {}", v);
            },
        ),
        "mixed-version-probe-enabled" => {
            apply_bool(value, "mixed-version-probe-enabled", config, |val, cfg| {
                cfg.replication.mixed_version_probe_enabled = val;
                tracing::info!("mixed-version-probe-enabled -> {}", val);
            })
        }
        "mixed-version-probe-interval-ms" => apply_positive_u64(
            value,
            "mixed-version-probe-interval-ms",
            config,
            |ms, cfg| {
                cfg.replication.mixed_version_probe_interval_ms = ms;
                tracing::info!("mixed-version-probe-interval-ms -> {}", ms);
            },
        ),
        "persistence-runtime-enabled" | "persistence-enabled" => {
            apply_bool(value, "persistence-runtime-enabled", config, |val, cfg| {
                cfg.persistence.runtime_enabled = val;
                tracing::info!("persistence-runtime-enabled -> {}", val);
            })
        }
        "tenancy-enabled" => apply_bool(value, "tenancy-enabled", config, |val, cfg| {
            cfg.tenancy.enabled = val;
            tracing::info!("tenancy-enabled -> {}", val);
        }),
        "tenancy-default-namespace" => apply_default_namespace(value, config),
        "tenancy-max-keys-per-namespace" => {
            apply_usize(value, "tenancy-max-keys-per-namespace", config, |v, cfg| {
                cfg.tenancy.max_keys_per_namespace = v;
                tracing::info!("tenancy-max-keys-per-namespace -> {}", v);
            })
        }
        "rate-limit-enabled" => apply_bool(value, "rate-limit-enabled", config, |val, cfg| {
            cfg.rate_limit.enabled = val;
            tracing::info!("rate-limit-enabled -> {}", val);
        }),
        "rate-limit-requests-per-sec" => {
            apply_positive_u64(value, "rate-limit-requests-per-sec", config, |v, cfg| {
                cfg.rate_limit.requests_per_sec = v;
                tracing::info!("rate-limit-requests-per-sec -> {}", v);
            })
        }
        "rate-limit-burst" => apply_positive_u64(value, "rate-limit-burst", config, |v, cfg| {
            cfg.rate_limit.burst = v;
            tracing::info!("rate-limit-burst -> {}", v);
        }),
        "hot-key-enabled" => apply_bool(value, "hot-key-enabled", config, |val, cfg| {
            cfg.hot_key.enabled = val;
            tracing::info!("hot-key-enabled -> {}", val);
        }),
        "hot-key-max-waiters" => {
            apply_positive_usize(value, "hot-key-max-waiters", config, |v, cfg| {
                cfg.hot_key.max_waiters = v;
                tracing::info!("hot-key-max-waiters -> {}", v);
            })
        }
        "hot-key-follower-wait-timeout-ms" => apply_positive_u64(
            value,
            "hot-key-follower-wait-timeout-ms",
            config,
            |v, cfg| {
                cfg.hot_key.follower_wait_timeout_ms = v;
                tracing::info!("hot-key-follower-wait-timeout-ms -> {}", v);
            },
        ),
        "hot-key-stale-ttl-ms" => apply_u64(value, "hot-key-stale-ttl-ms", config, |v, cfg| {
            cfg.hot_key.stale_ttl_ms = v;
            tracing::info!("hot-key-stale-ttl-ms -> {}", v);
        }),
        "hot-key-stale-max-entries" => {
            apply_positive_usize(value, "hot-key-stale-max-entries", config, |v, cfg| {
                cfg.hot_key.stale_max_entries = v;
                tracing::info!("hot-key-stale-max-entries -> {}", v);
            })
        }
        "hot-key-adaptive-waiters-enabled" => apply_bool(
            value,
            "hot-key-adaptive-waiters-enabled",
            config,
            |val, cfg| {
                cfg.hot_key.adaptive_waiters_enabled = val;
                tracing::info!("hot-key-adaptive-waiters-enabled -> {}", val);
            },
        ),
        "hot-key-adaptive-min-waiters" => {
            apply_positive_usize(value, "hot-key-adaptive-min-waiters", config, |v, cfg| {
                cfg.hot_key.adaptive_min_waiters = v;
                tracing::info!("hot-key-adaptive-min-waiters -> {}", v);
            })
        }
        "hot-key-adaptive-success-threshold" => apply_positive_u32(
            value,
            "hot-key-adaptive-success-threshold",
            config,
            |v, cfg| {
                cfg.hot_key.adaptive_success_threshold = v;
                tracing::info!("hot-key-adaptive-success-threshold -> {}", v);
            },
        ),
        "hot-key-adaptive-state-max-keys" => apply_positive_usize(
            value,
            "hot-key-adaptive-state-max-keys",
            config,
            |v, cfg| {
                cfg.hot_key.adaptive_state_max_keys = v;
                tracing::info!("hot-key-adaptive-state-max-keys -> {}", v);
            },
        ),
        "circuit-breaker-enabled" => {
            apply_bool(value, "circuit-breaker-enabled", config, |val, cfg| {
                cfg.circuit_breaker.enabled = val;
                tracing::info!("circuit-breaker-enabled -> {}", val);
            })
        }
        "circuit-breaker-failure-threshold" => apply_positive_u64(
            value,
            "circuit-breaker-failure-threshold",
            config,
            |v, cfg| {
                cfg.circuit_breaker.failure_threshold = v;
                tracing::info!("circuit-breaker-failure-threshold -> {}", v);
            },
        ),
        "circuit-breaker-open-ms" => {
            apply_positive_u64(value, "circuit-breaker-open-ms", config, |v, cfg| {
                cfg.circuit_breaker.open_ms = v;
                tracing::info!("circuit-breaker-open-ms -> {}", v);
            })
        }
        "circuit-breaker-half-open-max-requests" => apply_positive_u64(
            value,
            "circuit-breaker-half-open-max-requests",
            config,
            |v, cfg| {
                cfg.circuit_breaker.half_open_max_requests = v;
                tracing::info!("circuit-breaker-half-open-max-requests -> {}", v);
            },
        ),
        _ => ApplyResult::Unknown,
    }
}

fn apply_max_memory(value: &str, config: &Mutex<Config>, store: &Arc<KvStore>) -> ApplyResult {
    let trimmed = value.trim().to_ascii_lowercase();
    let digits = trimmed.trim_end_matches("mb").trim();
    match digits.parse::<u64>() {
        Ok(mb) => {
            store.set_max_memory_mb(mb);
            config.lock().unwrap().cache.max_memory_mb = mb;
            tracing::info!("max-memory -> {}mb", mb);
            ApplyResult::Applied
        }
        Err(_) => warn_invalid(value, "max-memory"),
    }
}

fn apply_write_quorum_mode(value: &str, config: &Mutex<Config>) -> ApplyResult {
    let mode = match value.trim().to_ascii_lowercase().as_str() {
        "all-active" => Some(WriteQuorumMode::AllActive),
        "majority" => Some(WriteQuorumMode::Majority),
        _ => None,
    };
    if let Some(mode) = mode {
        config.lock().unwrap().replication.write_quorum_mode = mode;
        tracing::info!("write-quorum-mode -> {}", mode.as_str());
        ApplyResult::Applied
    } else {
        warn_invalid(value, "write-quorum-mode")
    }
}

fn apply_default_namespace(value: &str, config: &Mutex<Config>) -> ApplyResult {
    let v = value.trim();
    if v.is_empty() || v.contains("::") {
        warn_invalid(value, "tenancy-default-namespace")
    } else {
        config.lock().unwrap().tenancy.default_namespace = v.to_string();
        tracing::info!("tenancy-default-namespace -> {}", v);
        ApplyResult::Applied
    }
}

fn apply_bool<F>(value: &str, _name: &str, config: &Mutex<Config>, apply: F) -> ApplyResult
where
    F: FnOnce(bool, &mut Config),
{
    with_config(config, |cfg| apply(parse_bool(value), cfg))
}

fn apply_u64<F>(value: &str, name: &str, config: &Mutex<Config>, apply: F) -> ApplyResult
where
    F: FnOnce(u64, &mut Config),
{
    match value.trim().parse::<u64>() {
        Ok(v) => with_config(config, |cfg| apply(v, cfg)),
        Err(_) => warn_invalid(value, name),
    }
}

fn apply_positive_u64<F>(value: &str, name: &str, config: &Mutex<Config>, apply: F) -> ApplyResult
where
    F: FnOnce(u64, &mut Config),
{
    match value.trim().parse::<u64>() {
        Ok(v) if v > 0 => with_config(config, |cfg| apply(v, cfg)),
        _ => warn_invalid(value, name),
    }
}

fn apply_usize<F>(value: &str, name: &str, config: &Mutex<Config>, apply: F) -> ApplyResult
where
    F: FnOnce(usize, &mut Config),
{
    match value.trim().parse::<usize>() {
        Ok(v) => with_config(config, |cfg| apply(v, cfg)),
        Err(_) => warn_invalid(value, name),
    }
}

fn apply_positive_usize<F>(value: &str, name: &str, config: &Mutex<Config>, apply: F) -> ApplyResult
where
    F: FnOnce(usize, &mut Config),
{
    match value.trim().parse::<usize>() {
        Ok(v) if v > 0 => with_config(config, |cfg| apply(v, cfg)),
        _ => warn_invalid(value, name),
    }
}

fn apply_positive_u32<F>(value: &str, name: &str, config: &Mutex<Config>, apply: F) -> ApplyResult
where
    F: FnOnce(u32, &mut Config),
{
    match value.trim().parse::<u32>() {
        Ok(v) if v > 0 => with_config(config, |cfg| apply(v, cfg)),
        _ => warn_invalid(value, name),
    }
}

fn with_config<F>(config: &Mutex<Config>, apply: F) -> ApplyResult
where
    F: FnOnce(&mut Config),
{
    let mut cfg = config.lock().unwrap();
    apply(&mut cfg);
    ApplyResult::Applied
}

fn parse_bool(value: &str) -> bool {
    value.trim().eq_ignore_ascii_case("true")
}

fn warn_invalid(value: &str, name: &str) -> ApplyResult {
    tracing::warn!("SetProperty {}: invalid value '{}'", name, value);
    ApplyResult::Applied
}

#[cfg(test)]
mod tests {
    use super::{apply, is_known, ApplyResult};
    use crate::{
        config::{Config, WriteQuorumMode},
        store::KvStore,
    };
    use std::sync::{Arc, Mutex};

    fn test_state() -> (Mutex<Config>, Arc<KvStore>) {
        let cfg = Config::default();
        let store = Arc::new(KvStore::new(
            cfg.cache.max_memory_mb,
            cfg.cache.default_ttl_secs,
            cfg.cache.value_size_limit_bytes,
            cfg.cache.max_keys,
            cfg.compression.enabled,
            cfg.compression.threshold_bytes,
        ));
        (Mutex::new(cfg), store)
    }

    #[test]
    fn known_property_registry_covers_runtime_properties() {
        for name in [
            "max-memory",
            "default-ttl",
            "compression-threshold",
            "write-quorum-mode",
            "persistence-enabled",
            "tenancy-default-namespace",
            "rate-limit-requests-per-sec",
            "hot-key-adaptive-success-threshold",
            "circuit-breaker-half-open-max-requests",
        ] {
            assert!(is_known(name), "{name} should be registered");
        }

        assert!(!is_known("active"));
        assert!(!is_known("client-port"));
        let (config, store) = test_state();
        assert_eq!(
            apply("not-a-runtime-property", "true", &config, &store),
            ApplyResult::Unknown
        );
    }

    #[test]
    fn applies_cache_store_and_compression_properties() {
        let (config, store) = test_state();

        assert_eq!(
            apply("max-memory", "7mb", &config, &store),
            ApplyResult::Applied
        );
        assert_eq!(
            apply("default-ttl", "15", &config, &store),
            ApplyResult::Applied
        );
        assert_eq!(
            apply("value-size-limit", "99", &config, &store),
            ApplyResult::Applied
        );
        assert_eq!(
            apply("max-keys", "11", &config, &store),
            ApplyResult::Applied
        );
        assert_eq!(
            apply("compression-enabled", "false", &config, &store),
            ApplyResult::Applied
        );
        assert_eq!(
            apply("compression-threshold", "8192", &config, &store),
            ApplyResult::Applied
        );

        let cfg = config.lock().unwrap();
        assert_eq!(cfg.cache.max_memory_mb, 7);
        assert_eq!(cfg.cache.default_ttl_secs, 15);
        assert_eq!(cfg.cache.value_size_limit_bytes, 99);
        assert_eq!(cfg.cache.max_keys, 11);
        assert!(!cfg.compression.enabled);
        assert_eq!(cfg.compression.threshold_bytes, 8192);

        let stats = store.stats();
        assert_eq!(stats.memory_max_bytes, 7 * 1024 * 1024);
        assert_eq!(stats.value_size_limit_bytes, 99);
        assert_eq!(stats.max_keys_limit, 11);
        assert!(!stats.compression_enabled);
        assert_eq!(stats.compression_threshold_bytes, 8192);
    }

    #[test]
    fn invalid_values_keep_existing_runtime_state() {
        let (config, store) = test_state();
        config.lock().unwrap().replication.write_timeout_ms = 123;
        config.lock().unwrap().tenancy.default_namespace = "tenant-a".into();

        assert_eq!(
            apply("write-timeout-ms", "0", &config, &store),
            ApplyResult::Applied
        );
        assert_eq!(
            apply("write-timeout-ms", "not-a-number", &config, &store),
            ApplyResult::Applied
        );
        assert_eq!(
            apply("tenancy-default-namespace", "", &config, &store),
            ApplyResult::Applied
        );
        assert_eq!(
            apply(
                "tenancy-default-namespace",
                "bad::namespace",
                &config,
                &store
            ),
            ApplyResult::Applied
        );

        let cfg = config.lock().unwrap();
        assert_eq!(cfg.replication.write_timeout_ms, 123);
        assert_eq!(cfg.tenancy.default_namespace, "tenant-a");
    }

    #[test]
    fn rejected_compression_threshold_preserves_config_and_store() {
        let (config, store) = test_state();
        assert_eq!(
            apply("compression-threshold", "8192", &config, &store),
            ApplyResult::Applied
        );

        let result = apply("compression-threshold", "4096", &config, &store);
        assert!(
            matches!(result, ApplyResult::Rejected(message) if message.contains("threshold can only be increased"))
        );
        assert_eq!(config.lock().unwrap().compression.threshold_bytes, 8192);
        assert_eq!(store.stats().compression_threshold_bytes, 8192);
    }

    #[test]
    fn applies_replication_persistence_tenancy_and_resilience_properties() {
        let (config, store) = test_state();

        for (name, value) in [
            ("write-timeout-ms", "42"),
            ("write-quorum-mode", "majority"),
            ("version-check-interval", "1000"),
            ("read-repair-on-miss-enabled", "true"),
            ("read-repair-min-interval-ms", "25"),
            ("read-repair-max-per-minute", "9"),
            ("anti-entropy-enabled", "true"),
            ("anti-entropy-interval-ms", "200"),
            ("anti-entropy-min-repair-interval-ms", "50"),
            ("anti-entropy-lag-threshold", "3"),
            ("anti-entropy-key-sample-size", "17"),
            ("anti-entropy-full-reconcile-every", "4"),
            ("anti-entropy-full-reconcile-max-keys", "500"),
            ("anti-entropy-budget-max-checks-per-run", "19"),
            ("anti-entropy-budget-max-duration-ms", "250"),
            ("mixed-version-probe-enabled", "false"),
            ("mixed-version-probe-interval-ms", "3000"),
            ("persistence-enabled", "true"),
            ("tenancy-enabled", "true"),
            ("tenancy-default-namespace", "prod"),
            ("tenancy-max-keys-per-namespace", "100"),
            ("rate-limit-enabled", "true"),
            ("rate-limit-requests-per-sec", "20"),
            ("rate-limit-burst", "40"),
            ("hot-key-enabled", "true"),
            ("hot-key-max-waiters", "8"),
            ("hot-key-follower-wait-timeout-ms", "75"),
            ("hot-key-stale-ttl-ms", "90"),
            ("hot-key-stale-max-entries", "10"),
            ("hot-key-adaptive-waiters-enabled", "true"),
            ("hot-key-adaptive-min-waiters", "2"),
            ("hot-key-adaptive-success-threshold", "6"),
            ("hot-key-adaptive-state-max-keys", "30"),
            ("circuit-breaker-enabled", "true"),
            ("circuit-breaker-failure-threshold", "5"),
            ("circuit-breaker-open-ms", "600"),
            ("circuit-breaker-half-open-max-requests", "3"),
        ] {
            assert_eq!(apply(name, value, &config, &store), ApplyResult::Applied);
        }

        let cfg = config.lock().unwrap();
        assert_eq!(cfg.replication.write_timeout_ms, 42);
        assert_eq!(cfg.replication.write_quorum_mode, WriteQuorumMode::Majority);
        assert_eq!(cfg.replication.version_check_interval_ms, 1000);
        assert!(cfg.replication.read_repair_on_miss_enabled);
        assert_eq!(cfg.replication.read_repair_min_interval_ms, 25);
        assert_eq!(cfg.replication.read_repair_max_per_minute, 9);
        assert!(cfg.replication.anti_entropy_enabled);
        assert_eq!(cfg.replication.anti_entropy_interval_ms, 200);
        assert_eq!(cfg.replication.anti_entropy_min_repair_interval_ms, 50);
        assert_eq!(cfg.replication.anti_entropy_lag_threshold, 3);
        assert_eq!(cfg.replication.anti_entropy_key_sample_size, 17);
        assert_eq!(cfg.replication.anti_entropy_full_reconcile_every, 4);
        assert_eq!(cfg.replication.anti_entropy_full_reconcile_max_keys, 500);
        assert_eq!(cfg.replication.anti_entropy_budget_max_checks_per_run, 19);
        assert_eq!(cfg.replication.anti_entropy_budget_max_duration_ms, 250);
        assert!(!cfg.replication.mixed_version_probe_enabled);
        assert_eq!(cfg.replication.mixed_version_probe_interval_ms, 3000);
        assert!(cfg.persistence.runtime_enabled);
        assert!(cfg.tenancy.enabled);
        assert_eq!(cfg.tenancy.default_namespace, "prod");
        assert_eq!(cfg.tenancy.max_keys_per_namespace, 100);
        assert!(cfg.rate_limit.enabled);
        assert_eq!(cfg.rate_limit.requests_per_sec, 20);
        assert_eq!(cfg.rate_limit.burst, 40);
        assert!(cfg.hot_key.enabled);
        assert_eq!(cfg.hot_key.max_waiters, 8);
        assert_eq!(cfg.hot_key.follower_wait_timeout_ms, 75);
        assert_eq!(cfg.hot_key.stale_ttl_ms, 90);
        assert_eq!(cfg.hot_key.stale_max_entries, 10);
        assert!(cfg.hot_key.adaptive_waiters_enabled);
        assert_eq!(cfg.hot_key.adaptive_min_waiters, 2);
        assert_eq!(cfg.hot_key.adaptive_success_threshold, 6);
        assert_eq!(cfg.hot_key.adaptive_state_max_keys, 30);
        assert!(cfg.circuit_breaker.enabled);
        assert_eq!(cfg.circuit_breaker.failure_threshold, 5);
        assert_eq!(cfg.circuit_breaker.open_ms, 600);
        assert_eq!(cfg.circuit_breaker.half_open_max_requests, 3);
    }
}
