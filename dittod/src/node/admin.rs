use super::{observability, NodeHandle, PROTOCOL_VERSION};
use crate::{network::cluster_server::send_cluster, store::kv_store::sanitize_for_log};
use ditto_protocol::{AdminRequest, AdminResponse, ClusterMessage, NodeStatus};
use std::{
    net::SocketAddr,
    sync::{atomic::Ordering, Arc},
};

impl NodeHandle {
    pub(super) fn persistence_states(
        cfg: &crate::config::Config,
    ) -> (bool, bool, bool, bool, bool, bool) {
        let platform = cfg.persistence.platform_allowed;
        let runtime = cfg.persistence.runtime_enabled;
        let enabled = platform && runtime;
        let backup_enabled = enabled && cfg.persistence.backup_allowed;
        let export_enabled = enabled && cfg.persistence.export_allowed;
        let import_enabled = enabled && cfg.persistence.import_allowed;
        (
            platform,
            runtime,
            enabled,
            backup_enabled,
            export_enabled,
            import_enabled,
        )
    }

    pub(super) fn audit_admin_request(&self, req: &AdminRequest) {
        let Some((action, resource)) = crate::admin_audit::labels(req) else {
            return;
        };
        let target = crate::admin_audit::target(req).unwrap_or_else(|| "none".into());
        let node_name = self.config.lock().unwrap().node.id.clone();
        tracing::info!(
            target: "ditto.audit",
            audit = true,
            event = "node_admin_request",
            node_id = %self.id,
            node_name = %node_name,
            action = action,
            resource = resource,
            request_target = %target,
            actor = "admin-protocol",
            auth_scheme = "cluster-admin-tls-or-network-policy"
        );
    }

    pub(super) async fn handle_admin(self: Arc<Self>, req: AdminRequest) -> AdminResponse {
        self.audit_admin_request(&req);
        match req {
            AdminRequest::Describe => AdminResponse::Properties(self.all_properties().await),
            AdminRequest::GetStats => AdminResponse::Stats(Box::new(self.stats().await)),
            AdminRequest::ListKeys { pattern } => {
                AdminResponse::Keys(self.store.keys(pattern.as_deref()))
            }
            AdminRequest::GetKeyInfo { key } => {
                let compressed = self.store.is_compressed(&key).unwrap_or(false);
                match self.store.get(&key) {
                    Some(entry) => {
                        let ttl = entry.ttl_remaining_secs();
                        let freq = entry.freq_count;
                        let ver = entry.version;
                        AdminResponse::KeyInfo {
                            key,
                            value: entry.value,
                            version: ver,
                            ttl_remaining_secs: ttl,
                            freq_count: freq,
                            compressed,
                        }
                    }
                    None => AdminResponse::NotFound,
                }
            }
            AdminRequest::SetKeyProperty { key, name, value } => match name.as_str() {
                "compressed" => {
                    let compress = value.trim().eq_ignore_ascii_case("true");
                    match self.store.set_key_compressed(&key, compress) {
                        Ok(()) => {
                            tracing::info!(
                                key = sanitize_for_log(&key).as_str(),
                                compressed = compress,
                                "SetKeyProperty updated"
                            );
                            AdminResponse::KeyPropertyUpdated
                        }
                        Err("key not found") => AdminResponse::NotFound,
                        Err(msg) => AdminResponse::Error {
                            message: msg.to_string(),
                        },
                    }
                }
                other => AdminResponse::Error {
                    message: format!("Unknown key property '{}'", other),
                },
            },
            AdminRequest::FlushCache => {
                let status = self.active_set.lock().await.local_status();
                if matches!(status, NodeStatus::Active | NodeStatus::Syncing) {
                    AdminResponse::Error {
                        message: "node must be Inactive before flush; use set-active false first"
                            .into(),
                    }
                } else {
                    self.store.flush();
                    self.write_log.lock().await.reset();
                    AdminResponse::Flushed
                }
            }
            AdminRequest::ClusterStatus => {
                let nodes = self.active_set.lock().await.snapshot();
                AdminResponse::ClusterView(nodes)
            }
            AdminRequest::SetKeysTtl { pattern, ttl_secs } => {
                let updated = self.store.set_ttl_by_pattern(&pattern, ttl_secs);
                tracing::info!(
                    pattern = sanitize_for_log(&pattern).as_str(),
                    ttl_secs = ?ttl_secs,
                    updated,
                    "SetKeysTtl updated"
                );
                AdminResponse::TtlUpdated { updated }
            }
            AdminRequest::BackupNow => {
                let (backup_enabled, cfg) = {
                    let cfg_guard = self.config.lock().unwrap();
                    let (_, _, _, backup_enabled, _, _) = Self::persistence_states(&cfg_guard);
                    (backup_enabled, cfg_guard.backup.clone())
                };
                if !backup_enabled {
                    return AdminResponse::Error {
                        message: "Backup is disabled by persistence policy. Require DITTO_PERSISTENCE_PLATFORM_ALLOWED=true, DITTO_PERSISTENCE_BACKUP_ALLOWED=true and runtime property persistence-runtime-enabled=true.".into(),
                    };
                }
                match crate::backup::run_backup(Arc::clone(&self), &cfg).await {
                    Ok(path) => AdminResponse::BackupResult { path, bytes: 0 },
                    Err(e) => AdminResponse::Error {
                        message: e.to_string(),
                    },
                }
            }
            AdminRequest::RestoreLatestSnapshot => {
                self.snapshot_restore_attempt_total
                    .fetch_add(1, Ordering::Relaxed);
                let (import_enabled, cfg) = {
                    let cfg_guard = self.config.lock().unwrap();
                    let (_, _, _, _, _, import_enabled) = Self::persistence_states(&cfg_guard);
                    (import_enabled, cfg_guard.backup.clone())
                };
                if !import_enabled {
                    self.snapshot_restore_failure_total
                        .fetch_add(1, Ordering::Relaxed);
                    self.snapshot_restore_policy_block_total
                        .fetch_add(1, Ordering::Relaxed);
                    return AdminResponse::Error {
                        message: "Restore is disabled by persistence policy. Require DITTO_PERSISTENCE_PLATFORM_ALLOWED=true, DITTO_PERSISTENCE_IMPORT_ALLOWED=true and runtime property persistence-runtime-enabled=true.".into(),
                    };
                }
                match crate::backup::restore_latest_snapshot(&self, &cfg) {
                    Ok(Some(loaded)) => {
                        self.record_snapshot_restore(
                            loaded.path.clone(),
                            loaded.entries as u64,
                            loaded.duration_ms,
                        );
                        self.snapshot_restore_success_total
                            .fetch_add(1, Ordering::Relaxed);
                        AdminResponse::RestoreResult {
                            path: loaded.path,
                            entries: loaded.entries as u64,
                            duration_ms: loaded.duration_ms,
                        }
                    }
                    Ok(None) => {
                        self.snapshot_restore_not_found_total
                            .fetch_add(1, Ordering::Relaxed);
                        AdminResponse::NotFound
                    }
                    Err(e) => AdminResponse::Error {
                        message: {
                            self.snapshot_restore_failure_total
                                .fetch_add(1, Ordering::Relaxed);
                            e.to_string()
                        },
                    },
                }
            }
            AdminRequest::GetProperty { name } => {
                let props = self.all_properties().await;
                let filtered: Vec<_> = props.into_iter().filter(|(k, _)| k == &name).collect();
                AdminResponse::Properties(filtered)
            }
            AdminRequest::SetProperty { name, value } => set_property(self, name, value).await,
        }
    }

    pub(super) async fn all_properties(&self) -> Vec<(String, String)> {
        let stats = self.stats().await;
        let set = self.active_set.lock().await;
        let cfg = self.config.lock().unwrap();
        vec![
            ("id".into(), cfg.node.id.clone()),
            ("protocol-version".into(), PROTOCOL_VERSION.to_string()),
            ("committed-index".into(), stats.committed_index.to_string()),
            ("uptime".into(), format!("{}s", stats.uptime_secs)),
            ("status".into(), format!("{:?}", set.local_status())),
            ("primary".into(), set.is_primary().to_string()),
            ("bind-addr".into(), cfg.node.bind_addr.clone()),
            (
                "cluster-bind-addr".into(),
                cfg.node.cluster_bind_addr.clone(),
            ),
            ("client-port".into(), cfg.node.client_port.to_string()),
            ("http-port".into(), cfg.node.http_port.to_string()),
            ("cluster-port".into(), cfg.node.cluster_port.to_string()),
            ("gossip-port".into(), cfg.node.gossip_port.to_string()),
            (
                "frame-read-timeout-ms".into(),
                cfg.node.frame_read_timeout_ms.to_string(),
            ),
            (
                "client-auth-enabled".into(),
                cfg.node.client_auth_token.is_some().to_string(),
            ),
            (
                "tcp-client-bind-loopback-only".into(),
                ditto_config::is_loopback_bind_addr(&cfg.node.bind_addr).to_string(),
            ),
            (
                "tcp-production-safe".into(),
                (cfg.node.client_auth_token.is_some()
                    || ditto_config::is_loopback_bind_addr(&cfg.node.bind_addr))
                .to_string(),
            ),
            (
                "insecure-runtime-enabled".into(),
                std::env::var("DITTO_INSECURE")
                    .unwrap_or_default()
                    .eq_ignore_ascii_case("true")
                    .to_string(),
            ),
            (
                "strict-security-enforced".into(),
                (!std::env::var("DITTO_INSECURE")
                    .unwrap_or_default()
                    .eq_ignore_ascii_case("true"))
                .to_string(),
            ),
            (
                "max-memory".into(),
                format!("{}mb", cfg.cache.max_memory_mb),
            ),
            (
                "default-ttl".into(),
                format!("{}s", cfg.cache.default_ttl_secs),
            ),
            (
                "value-size-limit".into(),
                if stats.value_size_limit_bytes == 0 {
                    "unlimited".into()
                } else {
                    format!("{}b", stats.value_size_limit_bytes)
                },
            ),
            (
                "max-keys".into(),
                if stats.max_keys_limit == 0 {
                    "unlimited".into()
                } else {
                    stats.max_keys_limit.to_string()
                },
            ),
            (
                "compression-enabled".into(),
                stats.compression_enabled.to_string(),
            ),
            (
                "compression-threshold".into(),
                format!("{}b", stats.compression_threshold_bytes),
            ),
            (
                "write-timeout-ms".into(),
                cfg.replication.write_timeout_ms.to_string(),
            ),
            (
                "write-quorum-mode".into(),
                cfg.replication.write_quorum_mode.as_str().to_string(),
            ),
            (
                "gossip-interval-ms".into(),
                cfg.replication.gossip_interval_ms.to_string(),
            ),
            (
                "gossip-dead-ms".into(),
                cfg.replication.gossip_dead_ms.to_string(),
            ),
            (
                "version-check-interval".into(),
                format!("{}ms", cfg.replication.version_check_interval_ms),
            ),
            (
                "anti-entropy-enabled".into(),
                cfg.replication.anti_entropy_enabled.to_string(),
            ),
            (
                "anti-entropy-interval-ms".into(),
                cfg.replication.anti_entropy_interval_ms.to_string(),
            ),
            (
                "anti-entropy-min-repair-interval-ms".into(),
                cfg.replication
                    .anti_entropy_min_repair_interval_ms
                    .to_string(),
            ),
            (
                "anti-entropy-lag-threshold".into(),
                cfg.replication.anti_entropy_lag_threshold.to_string(),
            ),
            (
                "anti-entropy-key-sample-size".into(),
                cfg.replication.anti_entropy_key_sample_size.to_string(),
            ),
            (
                "anti-entropy-full-reconcile-every".into(),
                cfg.replication
                    .anti_entropy_full_reconcile_every
                    .to_string(),
            ),
            (
                "anti-entropy-full-reconcile-max-keys".into(),
                cfg.replication
                    .anti_entropy_full_reconcile_max_keys
                    .to_string(),
            ),
            (
                "anti-entropy-budget-max-checks-per-run".into(),
                cfg.replication
                    .anti_entropy_budget_max_checks_per_run
                    .to_string(),
            ),
            (
                "anti-entropy-budget-max-duration-ms".into(),
                cfg.replication
                    .anti_entropy_budget_max_duration_ms
                    .to_string(),
            ),
            (
                "anti-entropy-runs-total".into(),
                stats.anti_entropy_runs_total.to_string(),
            ),
            (
                "anti-entropy-repair-trigger-total".into(),
                stats.anti_entropy_repair_trigger_total.to_string(),
            ),
            (
                "anti-entropy-repair-throttled-total".into(),
                stats.anti_entropy_repair_throttled_total.to_string(),
            ),
            (
                "anti-entropy-last-detected-lag".into(),
                stats.anti_entropy_last_detected_lag.to_string(),
            ),
            (
                "anti-entropy-key-checks-total".into(),
                stats.anti_entropy_key_checks_total.to_string(),
            ),
            (
                "anti-entropy-key-mismatch-total".into(),
                stats.anti_entropy_key_mismatch_total.to_string(),
            ),
            (
                "anti-entropy-full-reconcile-runs-total".into(),
                stats.anti_entropy_full_reconcile_runs_total.to_string(),
            ),
            (
                "anti-entropy-full-reconcile-key-checks-total".into(),
                stats
                    .anti_entropy_full_reconcile_key_checks_total
                    .to_string(),
            ),
            (
                "anti-entropy-full-reconcile-mismatch-total".into(),
                stats.anti_entropy_full_reconcile_mismatch_total.to_string(),
            ),
            (
                "anti-entropy-budget-exhausted-total".into(),
                stats.anti_entropy_budget_exhausted_total.to_string(),
            ),
            (
                "mixed-version-probe-enabled".into(),
                cfg.replication.mixed_version_probe_enabled.to_string(),
            ),
            (
                "mixed-version-probe-interval-ms".into(),
                cfg.replication.mixed_version_probe_interval_ms.to_string(),
            ),
            (
                "mixed-version-probe-runs-total".into(),
                stats.mixed_version_probe_runs_total.to_string(),
            ),
            (
                "mixed-version-peers-detected-total".into(),
                stats.mixed_version_peers_detected_total.to_string(),
            ),
            (
                "mixed-version-probe-errors-total".into(),
                stats.mixed_version_probe_errors_total.to_string(),
            ),
            (
                "mixed-version-last-detected-peer-count".into(),
                stats.mixed_version_last_detected_peer_count.to_string(),
            ),
            (
                "read-repair-on-miss-enabled".into(),
                cfg.replication.read_repair_on_miss_enabled.to_string(),
            ),
            (
                "read-repair-min-interval-ms".into(),
                cfg.replication.read_repair_min_interval_ms.to_string(),
            ),
            (
                "read-repair-max-per-minute".into(),
                cfg.replication.read_repair_max_per_minute.to_string(),
            ),
            (
                "read-repair-trigger-total".into(),
                stats.read_repair_trigger_total.to_string(),
            ),
            (
                "read-repair-success-total".into(),
                stats.read_repair_success_total.to_string(),
            ),
            (
                "read-repair-throttled-total".into(),
                stats.read_repair_throttled_total.to_string(),
            ),
            (
                "read-repair-budget-exhausted-total".into(),
                stats.read_repair_budget_exhausted_total.to_string(),
            ),
            (
                "namespace-quota-reject-total".into(),
                stats.namespace_quota_reject_total.to_string(),
            ),
            (
                "namespace-quota-reject-rate-per-min".into(),
                stats.namespace_quota_reject_rate_per_min.to_string(),
            ),
            (
                "namespace-quota-reject-trend".into(),
                stats.namespace_quota_reject_trend.clone(),
            ),
            (
                "namespace-quota-top-usage".into(),
                observability::format_namespace_quota_top_usage(&stats.namespace_quota_top_usage),
            ),
            (
                "namespace-latency-top".into(),
                observability::format_namespace_latency_top(&stats.namespace_latency_top),
            ),
            (
                "hot-key-top-usage".into(),
                observability::format_hot_key_top_usage(&stats.hot_key_top_usage),
            ),
            (
                "persistence-platform-allowed".into(),
                stats.persistence_platform_allowed.to_string(),
            ),
            (
                "persistence-runtime-enabled".into(),
                stats.persistence_runtime_enabled.to_string(),
            ),
            (
                "persistence-enabled".into(),
                stats.persistence_enabled.to_string(),
            ),
            ("tenancy-enabled".into(), stats.tenancy_enabled.to_string()),
            (
                "tenancy-default-namespace".into(),
                stats.tenancy_default_namespace.clone(),
            ),
            (
                "tenancy-max-keys-per-namespace".into(),
                stats.tenancy_max_keys_per_namespace.to_string(),
            ),
            (
                "persistence-backup-platform-allowed".into(),
                cfg.persistence.backup_allowed.to_string(),
            ),
            (
                "persistence-export-platform-allowed".into(),
                cfg.persistence.export_allowed.to_string(),
            ),
            (
                "persistence-import-platform-allowed".into(),
                cfg.persistence.import_allowed.to_string(),
            ),
            (
                "persistence-backup-enabled".into(),
                stats.persistence_backup_enabled.to_string(),
            ),
            (
                "persistence-export-enabled".into(),
                stats.persistence_export_enabled.to_string(),
            ),
            (
                "persistence-import-enabled".into(),
                stats.persistence_import_enabled.to_string(),
            ),
            (
                "snapshot-restore-on-start".into(),
                cfg.backup.restore_on_start.to_string(),
            ),
            (
                "snapshot-last-load-path".into(),
                stats.snapshot_last_load_path.clone().unwrap_or_default(),
            ),
            (
                "snapshot-last-load-duration-ms".into(),
                stats.snapshot_last_load_duration_ms.to_string(),
            ),
            (
                "snapshot-last-load-entries".into(),
                stats.snapshot_last_load_entries.to_string(),
            ),
            (
                "snapshot-last-load-age-secs".into(),
                stats
                    .snapshot_last_load_age_secs
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "0".to_string()),
            ),
            (
                "snapshot-restore-attempt-total".into(),
                stats.snapshot_restore_attempt_total.to_string(),
            ),
            (
                "snapshot-restore-success-total".into(),
                stats.snapshot_restore_success_total.to_string(),
            ),
            (
                "snapshot-restore-failure-total".into(),
                stats.snapshot_restore_failure_total.to_string(),
            ),
            (
                "snapshot-restore-not-found-total".into(),
                stats.snapshot_restore_not_found_total.to_string(),
            ),
            (
                "snapshot-restore-policy-block-total".into(),
                stats.snapshot_restore_policy_block_total.to_string(),
            ),
            (
                "snapshot-restore-success-ratio-pct".into(),
                stats.snapshot_restore_success_ratio_pct.to_string(),
            ),
            (
                "rate-limit-enabled".into(),
                stats.rate_limit_enabled.to_string(),
            ),
            (
                "rate-limit-requests-per-sec".into(),
                cfg.rate_limit.requests_per_sec.to_string(),
            ),
            ("rate-limit-burst".into(), cfg.rate_limit.burst.to_string()),
            (
                "rate-limited-requests-total".into(),
                stats.rate_limited_requests_total.to_string(),
            ),
            ("hot-key-enabled".into(), stats.hot_key_enabled.to_string()),
            (
                "hot-key-max-waiters".into(),
                cfg.hot_key.max_waiters.to_string(),
            ),
            (
                "hot-key-follower-wait-timeout-ms".into(),
                cfg.hot_key.follower_wait_timeout_ms.to_string(),
            ),
            (
                "hot-key-stale-ttl-ms".into(),
                cfg.hot_key.stale_ttl_ms.to_string(),
            ),
            (
                "hot-key-stale-max-entries".into(),
                cfg.hot_key.stale_max_entries.to_string(),
            ),
            (
                "hot-key-adaptive-waiters-enabled".into(),
                cfg.hot_key.adaptive_waiters_enabled.to_string(),
            ),
            (
                "hot-key-adaptive-min-waiters".into(),
                cfg.hot_key.adaptive_min_waiters.to_string(),
            ),
            (
                "hot-key-adaptive-success-threshold".into(),
                cfg.hot_key.adaptive_success_threshold.to_string(),
            ),
            (
                "hot-key-adaptive-state-max-keys".into(),
                cfg.hot_key.adaptive_state_max_keys.to_string(),
            ),
            (
                "hot-key-coalesced-hits-total".into(),
                stats.hot_key_coalesced_hits_total.to_string(),
            ),
            (
                "hot-key-fallback-exec-total".into(),
                stats.hot_key_fallback_exec_total.to_string(),
            ),
            (
                "hot-key-wait-timeout-total".into(),
                stats.hot_key_wait_timeout_total.to_string(),
            ),
            (
                "hot-key-stale-served-total".into(),
                stats.hot_key_stale_served_total.to_string(),
            ),
            (
                "hot-key-inflight-keys".into(),
                stats.hot_key_inflight_keys.to_string(),
            ),
            (
                "hot-key-stale-cache-entries".into(),
                stats.hot_key_stale_cache_entries.to_string(),
            ),
            (
                "hot-key-adaptive-state-keys".into(),
                stats.hot_key_adaptive_state_keys.to_string(),
            ),
            (
                "hot-key-adaptive-limit-increase-total".into(),
                stats.hot_key_adaptive_limit_increase_total.to_string(),
            ),
            (
                "hot-key-adaptive-limit-decrease-total".into(),
                stats.hot_key_adaptive_limit_decrease_total.to_string(),
            ),
            (
                "circuit-breaker-enabled".into(),
                stats.circuit_breaker_enabled.to_string(),
            ),
            (
                "circuit-breaker-failure-threshold".into(),
                cfg.circuit_breaker.failure_threshold.to_string(),
            ),
            (
                "circuit-breaker-open-ms".into(),
                cfg.circuit_breaker.open_ms.to_string(),
            ),
            (
                "circuit-breaker-half-open-max-requests".into(),
                cfg.circuit_breaker.half_open_max_requests.to_string(),
            ),
            (
                "circuit-breaker-state".into(),
                stats.circuit_breaker_state.clone(),
            ),
            (
                "circuit-breaker-open-total".into(),
                stats.circuit_breaker_open_total.to_string(),
            ),
            (
                "circuit-breaker-reject-total".into(),
                stats.circuit_breaker_reject_total.to_string(),
            ),
            (
                "client-requests-total".into(),
                stats.client_requests_total.to_string(),
            ),
            (
                "client-requests-tcp-total".into(),
                stats.client_requests_tcp_total.to_string(),
            ),
            (
                "client-requests-http-total".into(),
                stats.client_requests_http_total.to_string(),
            ),
            (
                "client-requests-internal-total".into(),
                stats.client_requests_internal_total.to_string(),
            ),
            (
                "client-request-latency-buckets".into(),
                observability::format_request_latency_buckets(&stats),
            ),
            (
                "client-latency-p50-estimate-ms".into(),
                stats
                    .client_latency_p50_estimate_ms
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "n/a".to_string()),
            ),
            (
                "client-latency-p90-estimate-ms".into(),
                stats
                    .client_latency_p90_estimate_ms
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "n/a".to_string()),
            ),
            (
                "client-latency-p95-estimate-ms".into(),
                stats
                    .client_latency_p95_estimate_ms
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "n/a".to_string()),
            ),
            (
                "client-latency-p99-estimate-ms".into(),
                stats
                    .client_latency_p99_estimate_ms
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "n/a".to_string()),
            ),
            (
                "client-error-total".into(),
                stats.client_error_total.to_string(),
            ),
            (
                "client-errors-tcp-total".into(),
                stats.client_errors_tcp_total.to_string(),
            ),
            (
                "client-errors-http-total".into(),
                stats.client_errors_http_total.to_string(),
            ),
            (
                "client-errors-internal-total".into(),
                stats.client_errors_internal_total.to_string(),
            ),
            (
                "client-error-auth-total".into(),
                stats.client_error_auth_total.to_string(),
            ),
            (
                "client-error-throttle-total".into(),
                stats.client_error_throttle_total.to_string(),
            ),
            (
                "client-error-availability-total".into(),
                stats.client_error_availability_total.to_string(),
            ),
            (
                "client-error-validation-total".into(),
                stats.client_error_validation_total.to_string(),
            ),
            (
                "client-error-internal-total".into(),
                stats.client_error_internal_total.to_string(),
            ),
            (
                "client-error-other-total".into(),
                stats.client_error_other_total.to_string(),
            ),
        ]
    }
}

pub(super) async fn set_property(
    node: Arc<NodeHandle>,
    name: String,
    value: String,
) -> AdminResponse {
    match name.as_str() {
        "active" | "status" => set_active_status(node, &value).await,
        "primary" => set_primary(node, &value).await,
        "bind-addr" | "cluster-bind-addr" => {
            if let Err(response) = set_bind_address(node, &name, &value).await {
                return response;
            }
        }
        "client-port" | "http-port" | "cluster-port" | "gossip-port" => {
            if let Err(response) = set_port(node, &name, &value).await {
                return response;
            }
        }
        runtime_name if crate::runtime_property::is_known(runtime_name) => {
            match crate::runtime_property::apply(runtime_name, &value, &node.config, &node.store) {
                crate::runtime_property::ApplyResult::Applied => {}
                crate::runtime_property::ApplyResult::Rejected(message) => {
                    return AdminResponse::Error { message };
                }
                crate::runtime_property::ApplyResult::Unknown => {
                    tracing::warn!("SetProperty: unknown property '{}'", runtime_name);
                }
            }
        }
        other => tracing::warn!("SetProperty: unknown property '{}'", other),
    }
    AdminResponse::Ok
}

async fn set_active_status(node: Arc<NodeHandle>, value: &str) {
    let active = matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "true" | "active"
    );
    if active {
        tracing::info!("Node reactivating - spawning re-sync pass (active until sync completes).");
        tokio::spawn(node.run_resync());
    } else {
        node.active.store(false, Ordering::Relaxed);
        node.active_set
            .lock()
            .await
            .set_local_status(NodeStatus::Inactive);
        tracing::info!("Node status -> Inactive");
    }
}

async fn set_primary(node: Arc<NodeHandle>, value: &str) {
    if value.trim().eq_ignore_ascii_case("true") {
        node.active_set.lock().await.set_pinned_primary(node.id);
        let peers: Vec<SocketAddr> = {
            let set = node.active_set.lock().await;
            set.active_peers()
                .into_iter()
                .map(|peer| SocketAddr::new(peer.addr.ip(), peer.cluster_port))
                .collect()
        };
        let message = ClusterMessage::ForcePrimary { node_id: node.id };
        for addr in peers {
            let _ = send_cluster(addr, &message, node.tls_connector.as_ref()).await;
        }
        tracing::info!("Force-elected self ({}) as primary", node.id);
    } else {
        node.active_set.lock().await.clear_pinned_primary();
        tracing::info!("Primary pin cleared; reverting to automatic election");
    }
}

async fn set_bind_address(
    node: Arc<NodeHandle>,
    name: &str,
    value: &str,
) -> Result<(), AdminResponse> {
    let status = node.active_set.lock().await.local_status();
    if status != NodeStatus::Inactive {
        return Err(AdminResponse::Error {
            message: format!(
                "Node must be Inactive before changing bind addresses. Use set-active false first (current status: {:?}).",
                status
            ),
        });
    }
    let address = value.trim().to_string();
    let mut cfg = node.config.lock().unwrap();
    match name {
        "bind-addr" => cfg.node.bind_addr = address.clone(),
        _ => cfg.node.cluster_bind_addr = address.clone(),
    }
    let _ = cfg.save(&node.config_path);
    tracing::info!("{} -> {} (saved; restart node to apply)", name, address);
    Ok(())
}

async fn set_port(node: Arc<NodeHandle>, name: &str, value: &str) -> Result<(), AdminResponse> {
    let status = node.active_set.lock().await.local_status();
    if status != NodeStatus::Inactive {
        return Err(AdminResponse::Error {
            message: format!(
                "Node must be Inactive before changing ports. Use set-active false first (current status: {:?}).",
                status
            ),
        });
    }
    match value.trim().parse::<u16>() {
        Ok(port) => {
            let mut cfg = node.config.lock().unwrap();
            match name {
                "client-port" => cfg.node.client_port = port,
                "http-port" => cfg.node.http_port = port,
                "cluster-port" => cfg.node.cluster_port = port,
                _ => cfg.node.gossip_port = port,
            }
            let _ = cfg.save(&node.config_path);
            tracing::info!("{} -> {} (saved; restart node to apply)", name, port);
        }
        Err(_) => tracing::warn!("SetProperty {}: invalid port '{}'", name, value),
    }
    Ok(())
}
