use super::{
    ClientRequestSource, NodeHandle, HOT_KEY_STATE_MAX, HOT_KEY_TOP_LIMIT,
    NAMESPACE_LATENCY_STATE_MAX, NAMESPACE_LATENCY_TOP_LIMIT,
};
use ditto_protocol::{
    ClientRequest, ClientResponse, ErrorCode, HotKeyUsage, NamespaceLatencySummary,
    NamespaceQuotaUsage, NodeStats,
};
use std::{cmp::Reverse, collections::HashMap, sync::atomic::Ordering, time::Duration};

pub(super) fn format_namespace_quota_top_usage(usage: &[NamespaceQuotaUsage]) -> String {
    if usage.is_empty() {
        return "-".to_string();
    }
    usage
        .iter()
        .map(|u| {
            format!(
                "{}:{}/{}({}%)",
                u.namespace, u.key_count, u.quota_limit, u.usage_pct
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn format_namespace_latency_top(rows: &[NamespaceLatencySummary]) -> String {
    if rows.is_empty() {
        return "-".to_string();
    }
    rows.iter()
        .map(|r| {
            let p95 = r
                .latency_p95_estimate_ms
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".to_string());
            let p99 = r
                .latency_p99_estimate_ms
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".to_string());
            format!(
                "{}:{}(p95={},p99={})",
                r.namespace, r.request_total, p95, p99
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn format_hot_key_top_usage(rows: &[HotKeyUsage]) -> String {
    if rows.is_empty() {
        return "-".to_string();
    }
    rows.iter()
        .map(|r| format!("{}:{}", r.key, r.request_total))
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn format_request_latency_buckets(stats: &NodeStats) -> String {
    format!(
        "<=1ms:{};<=5ms:{};<=20ms:{};<=100ms:{};<=500ms:{};>500ms:{}",
        stats.client_request_latency_le_1ms_total,
        stats.client_request_latency_le_5ms_total,
        stats.client_request_latency_le_20ms_total,
        stats.client_request_latency_le_100ms_total,
        stats.client_request_latency_le_500ms_total,
        stats.client_request_latency_gt_500ms_total
    )
}

pub(super) fn estimated_latency_percentile_ms(counts: [u64; 6], percentile: u64) -> Option<u64> {
    if percentile == 0 || percentile > 100 {
        return None;
    }
    let total: u64 = counts.iter().sum();
    if total == 0 {
        return None;
    }
    let target = ((total * percentile).saturating_add(99)) / 100;
    let mut cumulative = 0u64;
    for (idx, count) in counts.into_iter().enumerate() {
        cumulative = cumulative.saturating_add(count);
        if cumulative >= target {
            return Some(match idx {
                0 => 1,
                1 => 5,
                2 => 20,
                3 => 100,
                4 => 500,
                _ => 1000,
            });
        }
    }
    Some(1000)
}

impl NodeHandle {
    pub(super) fn observe_client_response_metrics(
        &self,
        source: ClientRequestSource,
        elapsed: Duration,
        response: &ClientResponse,
        namespace_label: Option<&str>,
        hot_key_label: Option<&str>,
    ) {
        self.client_requests_total.fetch_add(1, Ordering::Relaxed);
        match source {
            ClientRequestSource::Tcp => {
                self.client_requests_tcp_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            ClientRequestSource::Http => {
                self.client_requests_http_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            ClientRequestSource::Internal => {
                self.client_requests_internal_total
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        let ms = elapsed.as_millis() as u64;
        let bucket_idx = Self::latency_bucket_index(ms);
        if bucket_idx == 0 {
            self.client_request_latency_le_1ms_total
                .fetch_add(1, Ordering::Relaxed);
        } else if bucket_idx == 1 {
            self.client_request_latency_le_5ms_total
                .fetch_add(1, Ordering::Relaxed);
        } else if bucket_idx == 2 {
            self.client_request_latency_le_20ms_total
                .fetch_add(1, Ordering::Relaxed);
        } else if bucket_idx == 3 {
            self.client_request_latency_le_100ms_total
                .fetch_add(1, Ordering::Relaxed);
        } else if bucket_idx == 4 {
            self.client_request_latency_le_500ms_total
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.client_request_latency_gt_500ms_total
                .fetch_add(1, Ordering::Relaxed);
        }
        self.observe_namespace_latency(namespace_label, bucket_idx);
        self.observe_hot_key_usage(hot_key_label);

        if let ClientResponse::Error { code, .. } = response {
            self.client_error_total.fetch_add(1, Ordering::Relaxed);
            match source {
                ClientRequestSource::Tcp => {
                    self.client_errors_tcp_total.fetch_add(1, Ordering::Relaxed);
                }
                ClientRequestSource::Http => {
                    self.client_errors_http_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                ClientRequestSource::Internal => {
                    self.client_errors_internal_total
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            match code {
                ErrorCode::AuthFailed => {
                    self.client_error_auth_total.fetch_add(1, Ordering::Relaxed);
                }
                ErrorCode::RateLimited
                | ErrorCode::CircuitOpen
                | ErrorCode::NamespaceQuotaExceeded => {
                    self.client_error_throttle_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                ErrorCode::NodeInactive | ErrorCode::NoQuorum | ErrorCode::WriteTimeout => {
                    self.client_error_availability_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                ErrorCode::KeyNotFound
                | ErrorCode::ValueTooLarge
                | ErrorCode::KeyLimitReached
                | ErrorCode::TypeMismatch
                | ErrorCode::Overflow => {
                    self.client_error_validation_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                ErrorCode::InternalError => {
                    self.client_error_internal_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                ErrorCode::UnsupportedRequest => {
                    self.client_error_other_total
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    pub(super) fn latency_bucket_index(ms: u64) -> usize {
        if ms <= 1 {
            0
        } else if ms <= 5 {
            1
        } else if ms <= 20 {
            2
        } else if ms <= 100 {
            3
        } else if ms <= 500 {
            4
        } else {
            5
        }
    }

    pub(super) fn request_namespace_label(&self, req: &ClientRequest) -> Option<String> {
        match req {
            ClientRequest::Get { namespace, .. }
            | ClientRequest::Set { namespace, .. }
            | ClientRequest::SetNx { namespace, .. }
            | ClientRequest::Incr { namespace, .. }
            | ClientRequest::Delete { namespace, .. }
            | ClientRequest::Watch { namespace, .. }
            | ClientRequest::Unwatch { namespace, .. }
            | ClientRequest::DeleteByPattern { namespace, .. }
            | ClientRequest::SetTtlByPattern { namespace, .. } => {
                Some(namespace.clone().unwrap_or_else(|| {
                    self.config
                        .lock()
                        .unwrap()
                        .tenancy
                        .default_namespace
                        .clone()
                }))
            }
            ClientRequest::Ping | ClientRequest::Auth { .. } => None,
        }
    }

    pub(super) fn request_hot_key_label(
        &self,
        req: &ClientRequest,
        namespace: Option<&str>,
    ) -> Option<String> {
        let key = match req {
            ClientRequest::Get { key, .. }
            | ClientRequest::Set { key, .. }
            | ClientRequest::SetNx { key, .. }
            | ClientRequest::Incr { key, .. }
            | ClientRequest::Delete { key, .. }
            | ClientRequest::Watch { key, .. }
            | ClientRequest::Unwatch { key, .. } => Some(key.as_str()),
            ClientRequest::Ping
            | ClientRequest::Auth { .. }
            | ClientRequest::DeleteByPattern { .. }
            | ClientRequest::SetTtlByPattern { .. } => None,
        }?;
        let ns = namespace.unwrap_or("default");
        Some(format!("{}::{}", ns, key))
    }

    pub(super) fn observe_namespace_latency(
        &self,
        namespace_label: Option<&str>,
        bucket_idx: usize,
    ) {
        let Some(namespace) = namespace_label else {
            return;
        };
        let mut map = self.namespace_latency_runtime.lock().unwrap();
        if !map.contains_key(namespace) && map.len() >= NAMESPACE_LATENCY_STATE_MAX {
            if let Some(evict_key) = map
                .iter()
                .min_by_key(|(_, s)| s.request_total)
                .map(|(k, _)| k.clone())
            {
                map.remove(&evict_key);
            }
        }
        let entry = map.entry(namespace.to_string()).or_default();
        entry.request_total = entry.request_total.saturating_add(1);
        if bucket_idx < entry.buckets.len() {
            entry.buckets[bucket_idx] = entry.buckets[bucket_idx].saturating_add(1);
        }
    }

    pub(super) fn observe_hot_key_usage(&self, hot_key_label: Option<&str>) {
        let Some(key) = hot_key_label else {
            return;
        };
        let mut map = self.hot_key_usage_runtime.lock().unwrap();
        if !map.contains_key(key) && map.len() >= HOT_KEY_STATE_MAX {
            if let Some(evict_key) = map
                .iter()
                .min_by_key(|(_, count)| *count)
                .map(|(k, _)| k.clone())
            {
                map.remove(&evict_key);
            }
        }
        let count = map.entry(key.to_string()).or_insert(0);
        *count = count.saturating_add(1);
    }

    pub(super) fn namespace_latency_top(&self, limit: usize) -> Vec<NamespaceLatencySummary> {
        let mut rows: Vec<NamespaceLatencySummary> = self
            .namespace_latency_runtime
            .lock()
            .unwrap()
            .iter()
            .map(|(namespace, runtime)| NamespaceLatencySummary {
                namespace: namespace.clone(),
                request_total: runtime.request_total,
                latency_p95_estimate_ms: estimated_latency_percentile_ms(runtime.buckets, 95),
                latency_p99_estimate_ms: estimated_latency_percentile_ms(runtime.buckets, 99),
            })
            .collect();
        rows.sort_by_key(|row| Reverse(row.request_total));
        rows.truncate(limit);
        rows
    }

    pub(super) fn hot_key_top_usage(&self, limit: usize) -> Vec<HotKeyUsage> {
        let mut rows: Vec<HotKeyUsage> = self
            .hot_key_usage_runtime
            .lock()
            .unwrap()
            .iter()
            .map(|(key, request_total)| HotKeyUsage {
                key: key.clone(),
                request_total: *request_total,
            })
            .collect();
        rows.sort_by_key(|row| Reverse(row.request_total));
        rows.truncate(limit);
        rows
    }

    pub(super) fn namespace_quota_top_usage(
        &self,
        tenancy_enabled: bool,
        max_keys_per_namespace: usize,
        top_n: usize,
    ) -> Vec<NamespaceQuotaUsage> {
        if !tenancy_enabled || max_keys_per_namespace == 0 || top_n == 0 {
            return Vec::new();
        }

        let mut counts: HashMap<String, u64> = HashMap::new();
        for key in self.store.keys(None) {
            if let Some((namespace, _)) = key.split_once("::") {
                *counts.entry(namespace.to_string()).or_insert(0) += 1;
            }
        }

        let quota_limit = max_keys_per_namespace as u64;
        let mut usage: Vec<NamespaceQuotaUsage> = counts
            .into_iter()
            .map(|(namespace, key_count)| {
                let usage_pct = key_count
                    .saturating_mul(100)
                    .checked_div(quota_limit)
                    .unwrap_or(0);
                let remaining_keys = quota_limit.saturating_sub(key_count);
                NamespaceQuotaUsage {
                    namespace,
                    key_count,
                    quota_limit,
                    usage_pct,
                    remaining_keys,
                }
            })
            .collect();

        usage.sort_by(|a, b| {
            b.usage_pct
                .cmp(&a.usage_pct)
                .then_with(|| b.key_count.cmp(&a.key_count))
                .then_with(|| a.namespace.cmp(&b.namespace))
        });
        usage.truncate(top_n);
        usage
    }

    pub(super) fn namespace_quota_reject_velocity(&self, current_total: u64) -> (u64, String) {
        let now_ms = Self::now_millis();
        let prev_total = self
            .namespace_quota_reject_last_total
            .swap(current_total, Ordering::Relaxed);
        let prev_ts = self
            .namespace_quota_reject_last_ts_ms
            .swap(now_ms, Ordering::Relaxed);

        if prev_ts == 0 || now_ms <= prev_ts {
            return (0, "steady".to_string());
        }

        let delta_total = current_total.saturating_sub(prev_total);
        let delta_ms = now_ms.saturating_sub(prev_ts).max(1);
        let rate_per_min = delta_total.saturating_mul(60_000) / delta_ms;
        let trend = if rate_per_min >= 10 {
            "surging"
        } else if rate_per_min > 0 {
            "rising"
        } else {
            "steady"
        };
        (rate_per_min, trend.to_string())
    }

    pub async fn stats(&self) -> NodeStats {
        let s = self.store.stats();
        let set = self.active_set.lock().await;
        let log = self.write_log.lock().await;
        let hot_key_inflight_keys = self.get_flights.lock().await.len() as u64;
        let hot_key_stale_cache_entries = self.hot_key_stale_cache.lock().await.len() as u64;
        let hot_key_adaptive_state_keys = self.hot_key_adaptive_state.lock().unwrap().len() as u64;
        let cfg = self.config.lock().unwrap();
        let backup_dir_bytes = super::util::dir_size_bytes(&cfg.backup.path);
        let snapshot_last_load_path = self.snapshot_last_load_path.lock().unwrap().clone();
        let snapshot_last_load_age_secs = if snapshot_last_load_path.is_some() {
            let completed_ms = self
                .snapshot_last_load_completed_at_ms
                .load(Ordering::Relaxed);
            if completed_ms > 0 {
                Some(Self::now_millis().saturating_sub(completed_ms) / 1000)
            } else {
                Some(0)
            }
        } else {
            None
        };
        let (
            persistence_platform_allowed,
            persistence_runtime_enabled,
            persistence_enabled,
            persistence_backup_enabled,
            persistence_export_enabled,
            persistence_import_enabled,
        ) = Self::persistence_states(&cfg);
        let rate_limit_enabled = cfg.rate_limit.enabled;
        let circuit_breaker_enabled = cfg.circuit_breaker.enabled;
        let hot_key_enabled = cfg.hot_key.enabled;
        let read_repair_enabled = cfg.replication.read_repair_on_miss_enabled;
        let tenancy_enabled = cfg.tenancy.enabled;
        let tenancy_default_namespace = cfg.tenancy.default_namespace.clone();
        let tenancy_max_keys_per_namespace = cfg.tenancy.max_keys_per_namespace;
        let namespace_quota_top_usage =
            self.namespace_quota_top_usage(tenancy_enabled, tenancy_max_keys_per_namespace, 3);
        let namespace_latency_top = self.namespace_latency_top(NAMESPACE_LATENCY_TOP_LIMIT);
        let hot_key_top_usage = self.hot_key_top_usage(HOT_KEY_TOP_LIMIT);
        let namespace_quota_reject_total =
            self.namespace_quota_reject_total.load(Ordering::Relaxed);
        let (namespace_quota_reject_rate_per_min, namespace_quota_reject_trend) =
            self.namespace_quota_reject_velocity(namespace_quota_reject_total);
        let snapshot_restore_attempt_total =
            self.snapshot_restore_attempt_total.load(Ordering::Relaxed);
        let snapshot_restore_success_total =
            self.snapshot_restore_success_total.load(Ordering::Relaxed);
        let snapshot_restore_failure_total =
            self.snapshot_restore_failure_total.load(Ordering::Relaxed);
        let snapshot_restore_not_found_total = self
            .snapshot_restore_not_found_total
            .load(Ordering::Relaxed);
        let snapshot_restore_policy_block_total = self
            .snapshot_restore_policy_block_total
            .load(Ordering::Relaxed);
        let snapshot_restore_success_ratio_pct = snapshot_restore_success_total
            .saturating_mul(100)
            .checked_div(snapshot_restore_attempt_total)
            .unwrap_or(100)
            .min(100);
        let circuit_breaker_state = if circuit_breaker_enabled {
            self.circuit.lock().unwrap().state.as_str().to_string()
        } else {
            "disabled".to_string()
        };
        let client_request_latency_buckets = [
            self.client_request_latency_le_1ms_total
                .load(Ordering::Relaxed),
            self.client_request_latency_le_5ms_total
                .load(Ordering::Relaxed),
            self.client_request_latency_le_20ms_total
                .load(Ordering::Relaxed),
            self.client_request_latency_le_100ms_total
                .load(Ordering::Relaxed),
            self.client_request_latency_le_500ms_total
                .load(Ordering::Relaxed),
            self.client_request_latency_gt_500ms_total
                .load(Ordering::Relaxed),
        ];
        let client_latency_p50_estimate_ms =
            estimated_latency_percentile_ms(client_request_latency_buckets, 50);
        let client_latency_p90_estimate_ms =
            estimated_latency_percentile_ms(client_request_latency_buckets, 90);
        let client_latency_p95_estimate_ms =
            estimated_latency_percentile_ms(client_request_latency_buckets, 95);
        let client_latency_p99_estimate_ms =
            estimated_latency_percentile_ms(client_request_latency_buckets, 99);
        NodeStats {
            node_id: self.id,
            status: set.local_status(),
            is_primary: set.is_primary(),
            committed_index: log.committed_index(),
            key_count: s.key_count,
            memory_used_bytes: s.memory_used_bytes,
            memory_max_bytes: s.memory_max_bytes,
            evictions: s.evictions,
            hit_count: s.hit_count,
            miss_count: s.miss_count,
            uptime_secs: self.started_at.elapsed().as_secs(),
            value_size_limit_bytes: s.value_size_limit_bytes,
            max_keys_limit: s.max_keys_limit,
            compression_enabled: s.compression_enabled,
            compression_threshold_bytes: s.compression_threshold_bytes,
            node_name: cfg.node.id.clone(),
            backup_dir_bytes,
            snapshot_last_load_path,
            snapshot_last_load_duration_ms: self
                .snapshot_last_load_duration_ms
                .load(Ordering::Relaxed),
            snapshot_last_load_entries: self.snapshot_last_load_entries.load(Ordering::Relaxed),
            snapshot_last_load_age_secs,
            snapshot_restore_attempt_total,
            snapshot_restore_success_total,
            snapshot_restore_failure_total,
            snapshot_restore_not_found_total,
            snapshot_restore_policy_block_total,
            snapshot_restore_success_ratio_pct,
            persistence_platform_allowed,
            persistence_runtime_enabled,
            persistence_enabled,
            persistence_backup_enabled,
            persistence_export_enabled,
            persistence_import_enabled,
            tenancy_enabled,
            tenancy_default_namespace,
            tenancy_max_keys_per_namespace,
            rate_limit_enabled,
            rate_limited_requests_total: self.rate_limited_requests_total.load(Ordering::Relaxed),
            circuit_breaker_enabled,
            hot_key_enabled,
            hot_key_adaptive_waiters_enabled: cfg.hot_key.adaptive_waiters_enabled,
            read_repair_enabled,
            hot_key_coalesced_hits_total: self.hot_key_coalesced_hits_total.load(Ordering::Relaxed),
            hot_key_fallback_exec_total: self.hot_key_fallback_exec_total.load(Ordering::Relaxed),
            hot_key_wait_timeout_total: self.hot_key_wait_timeout_total.load(Ordering::Relaxed),
            hot_key_stale_served_total: self.hot_key_stale_served_total.load(Ordering::Relaxed),
            hot_key_inflight_keys,
            hot_key_stale_cache_entries,
            hot_key_adaptive_state_keys,
            hot_key_adaptive_limit_increase_total: self
                .hot_key_adaptive_limit_increase_total
                .load(Ordering::Relaxed),
            hot_key_adaptive_limit_decrease_total: self
                .hot_key_adaptive_limit_decrease_total
                .load(Ordering::Relaxed),
            read_repair_trigger_total: self.read_repair_trigger_total.load(Ordering::Relaxed),
            read_repair_success_total: self.read_repair_success_total.load(Ordering::Relaxed),
            read_repair_throttled_total: self.read_repair_throttled_total.load(Ordering::Relaxed),
            read_repair_budget_exhausted_total: self
                .read_repair_budget_exhausted_total
                .load(Ordering::Relaxed),
            namespace_quota_reject_total,
            namespace_quota_reject_rate_per_min,
            namespace_quota_reject_trend,
            namespace_quota_top_usage,
            namespace_latency_top,
            hot_key_top_usage,
            anti_entropy_runs_total: self.anti_entropy_runs_total.load(Ordering::Relaxed),
            anti_entropy_repair_trigger_total: self
                .anti_entropy_repair_trigger_total
                .load(Ordering::Relaxed),
            anti_entropy_repair_throttled_total: self
                .anti_entropy_repair_throttled_total
                .load(Ordering::Relaxed),
            anti_entropy_last_detected_lag: self
                .anti_entropy_last_detected_lag
                .load(Ordering::Relaxed),
            anti_entropy_key_checks_total: self
                .anti_entropy_key_checks_total
                .load(Ordering::Relaxed),
            anti_entropy_key_mismatch_total: self
                .anti_entropy_key_mismatch_total
                .load(Ordering::Relaxed),
            anti_entropy_full_reconcile_runs_total: self
                .anti_entropy_full_reconcile_runs_total
                .load(Ordering::Relaxed),
            anti_entropy_full_reconcile_key_checks_total: self
                .anti_entropy_full_reconcile_key_checks_total
                .load(Ordering::Relaxed),
            anti_entropy_full_reconcile_mismatch_total: self
                .anti_entropy_full_reconcile_mismatch_total
                .load(Ordering::Relaxed),
            anti_entropy_budget_exhausted_total: self
                .anti_entropy_budget_exhausted_total
                .load(Ordering::Relaxed),
            mixed_version_probe_runs_total: self
                .mixed_version_probe_runs_total
                .load(Ordering::Relaxed),
            mixed_version_peers_detected_total: self
                .mixed_version_peers_detected_total
                .load(Ordering::Relaxed),
            mixed_version_probe_errors_total: self
                .mixed_version_probe_errors_total
                .load(Ordering::Relaxed),
            mixed_version_last_detected_peer_count: self
                .mixed_version_last_detected_peer_count
                .load(Ordering::Relaxed),
            circuit_breaker_state,
            circuit_breaker_open_total: self.circuit_breaker_open_total.load(Ordering::Relaxed),
            circuit_breaker_reject_total: self.circuit_breaker_reject_total.load(Ordering::Relaxed),
            client_requests_total: self.client_requests_total.load(Ordering::Relaxed),
            client_requests_tcp_total: self.client_requests_tcp_total.load(Ordering::Relaxed),
            client_requests_http_total: self.client_requests_http_total.load(Ordering::Relaxed),
            client_requests_internal_total: self
                .client_requests_internal_total
                .load(Ordering::Relaxed),
            client_request_latency_le_1ms_total: client_request_latency_buckets[0],
            client_request_latency_le_5ms_total: client_request_latency_buckets[1],
            client_request_latency_le_20ms_total: client_request_latency_buckets[2],
            client_request_latency_le_100ms_total: client_request_latency_buckets[3],
            client_request_latency_le_500ms_total: client_request_latency_buckets[4],
            client_request_latency_gt_500ms_total: client_request_latency_buckets[5],
            client_latency_p50_estimate_ms,
            client_latency_p90_estimate_ms,
            client_latency_p95_estimate_ms,
            client_latency_p99_estimate_ms,
            client_error_total: self.client_error_total.load(Ordering::Relaxed),
            client_errors_tcp_total: self.client_errors_tcp_total.load(Ordering::Relaxed),
            client_errors_http_total: self.client_errors_http_total.load(Ordering::Relaxed),
            client_errors_internal_total: self.client_errors_internal_total.load(Ordering::Relaxed),
            client_error_auth_total: self.client_error_auth_total.load(Ordering::Relaxed),
            client_error_throttle_total: self.client_error_throttle_total.load(Ordering::Relaxed),
            client_error_availability_total: self
                .client_error_availability_total
                .load(Ordering::Relaxed),
            client_error_validation_total: self
                .client_error_validation_total
                .load(Ordering::Relaxed),
            client_error_internal_total: self.client_error_internal_total.load(Ordering::Relaxed),
            client_error_other_total: self.client_error_other_total.load(Ordering::Relaxed),
        }
    }
}
