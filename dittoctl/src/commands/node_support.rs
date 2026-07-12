use crate::client::{enc, mgmt_get};
use anyhow::{bail, Result};
use serde_json::Value;
use std::collections::HashMap;

pub(crate) fn fmt_uptime(secs: u64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        let h = secs / 3600;
        let m = (secs % 3600) / 60;
        let s = secs % 60;
        format!("{}h {}m {}s", h, m, s)
    }
}

pub(crate) fn fmt_namespace_quota_top_usage(v: &Value) -> String {
    let Some(items) = v.as_array() else {
        return "-".to_string();
    };
    if items.is_empty() {
        return "-".to_string();
    }
    items
        .iter()
        .map(|item| {
            let ns = item["namespace"].as_str().unwrap_or("?");
            let count = item["key_count"].as_u64().unwrap_or(0);
            let quota = item["quota_limit"].as_u64().unwrap_or(0);
            let pct = item["usage_pct"].as_u64().unwrap_or(0);
            format!("{}:{}/{}({}%)", ns, count, quota, pct)
        })
        .collect::<Vec<_>>()
        .join(",")
}

pub(crate) fn top_quota_usage_pct(v: &Value) -> u64 {
    let Some(items) = v.as_array() else {
        return 0;
    };
    items
        .iter()
        .filter_map(|item| item["usage_pct"].as_u64())
        .max()
        .unwrap_or(0)
}

pub(crate) fn build_describe_property_index(data: &Value) -> HashMap<String, HashMap<String, String>> {
    let mut index = HashMap::new();
    let Some(entries) = data.as_array() else {
        return index;
    };

    for entry in entries {
        let Some(addr) = entry["addr"].as_str() else {
            continue;
        };
        if entry.get("error").and_then(|value| value.as_str()).is_some() {
            continue;
        }
        let props = describe_properties(entry);
        if !props.is_empty() {
            index.insert(addr.to_string(), props);
        }
    }

    index
}

pub(crate) fn tcp_doctor_reason(props: &HashMap<String, String>) -> Option<String> {
    if property_bool(props, "tcp-production-safe") != Some(false) {
        return None;
    }

    let auth_enabled = property_bool(props, "client-auth-enabled").unwrap_or(false);
    let loopback_only = property_bool(props, "tcp-client-bind-loopback-only").unwrap_or(false);

    Some(format!(
        "tcp exposure unsupported (auth-enabled={}, loopback-only={})",
        auth_enabled, loopback_only
    ))
}

pub(crate) fn insecure_runtime_reason(props: &HashMap<String, String>) -> Option<String> {
    if property_bool(props, "insecure-runtime-enabled") == Some(true) {
        return Some("insecure runtime bypass enabled".to_string());
    }
    None
}

pub(crate) async fn run_doctor(base: &str, target: &str, client: &reqwest::Client) -> Result<()> {
    let status_url = format!("{}/api/nodes/{}/status", base, enc(target));
    let describe_url = format!("{}/api/nodes/{}/describe", base, enc(target));
    let cluster_url = format!("{}/api/cluster", base);
    let status_data = mgmt_get(client, &status_url).await?;
    let describe_data = mgmt_get(client, &describe_url).await?;
    let cluster_data = mgmt_get(client, &cluster_url).await?;
    let nodes = status_data.as_array().cloned().unwrap_or_default();
    let describe_by_addr = build_describe_property_index(&describe_data);

    let mut critical = 0usize;
    let mut warn = 0usize;
    let mut notes: Vec<String> = Vec::new();

    println!("  dittoctl doctor");
    println!("  {}", "-".repeat(52));

    let total = cluster_data["total"].as_u64().unwrap_or(0);
    let active = cluster_data["active"].as_u64().unwrap_or(0);
    let syncing = cluster_data["syncing"].as_u64().unwrap_or(0);
    let inactive = cluster_data["inactive"].as_u64().unwrap_or(0);
    let offline = cluster_data["offline"].as_u64().unwrap_or(0);
    println!(
        "  cluster: total={} active={} syncing={} inactive={} offline={}",
        total, active, syncing, inactive, offline
    );

    if inactive > 0 || offline > 0 {
        critical += 1;
        notes.push(format!(
            "cluster has inactive/offline nodes (inactive={}, offline={})",
            inactive, offline
        ));
    } else if syncing > 0 {
        warn += 1;
        notes.push(format!("cluster has syncing nodes ({})", syncing));
    }

    for node in nodes {
        let report = evaluate_doctor_node(&node, &describe_by_addr);
        critical += report.critical_hits;
        warn += report.warn_hits;

        println!(
            "  [{}] {} status={} hb={}ms quota={}rpm trend={} top={}%",
            report.severity,
            report.addr,
            report.status,
            report.heartbeat_ms,
            report.quota_rpm,
            report.quota_trend,
            report.quota_peak
        );
        println!("       notes={}", report.reasons_text());
    }

    let verdict = doctor_verdict(critical, warn);
    println!("  {}", "-".repeat(52));
    println!("  verdict: {}", verdict);

    if !notes.is_empty() {
        for note in notes {
            println!("  note: {}", note);
        }
    }

    if critical > 0 {
        bail!("doctor found {} critical issue(s) (warnings={})", critical, warn);
    }

    Ok(())
}

pub(crate) fn render_node_status(node: &Value) {
    println!("\n  Node: {}", node["addr"].as_str().unwrap_or("?"));
    if node["reachable"].as_bool() == Some(false) {
        println!("  (unreachable)");
        return;
    }

    println!("  {:<22} {}", "id", node["node_name"].as_str().unwrap_or("?"));
    println!(
        "  {:<22} {}",
        "committed-index",
        node["committed_index"]
            .as_u64()
            .map(|v| v.to_string())
            .as_deref()
            .unwrap_or("?")
    );
    println!(
        "  {:<22} {} / {} bytes",
        "memory",
        node["memory_used_bytes"].as_u64().unwrap_or(0),
        node["memory_max_bytes"].as_u64().unwrap_or(0)
    );
    println!("  {:<22} {}ms", "heartbeat", node["heartbeat_ms"].as_u64().unwrap_or(0));
    println!("  {:<22} {}", "uptime", fmt_uptime(node["uptime_secs"].as_u64().unwrap_or(0)));
    println!(
        "  {:<22} {} bytes",
        "backup-storage",
        node["backup_dir_bytes"].as_u64().unwrap_or(0)
    );
    println!("  {:<22} {}", "snapshot-load-path", node["snapshot_last_load_path"].as_str().unwrap_or("-"));
    println!(
        "  {:<22} {}ms",
        "snapshot-load-ms",
        node["snapshot_last_load_duration_ms"]
            .as_u64()
            .map(|v| v.to_string())
            .as_deref()
            .unwrap_or("0")
    );
    println!(
        "  {:<22} {}",
        "snapshot-load-entries",
        node["snapshot_last_load_entries"]
            .as_u64()
            .map(|v| v.to_string())
            .as_deref()
            .unwrap_or("0")
    );
    println!(
        "  {:<22} {}s",
        "snapshot-load-age",
        node["snapshot_last_load_age_secs"]
            .as_u64()
            .map(|v| v.to_string())
            .as_deref()
            .unwrap_or("0")
    );
    println!(
        "  {:<22} attempts:{} success:{} fail:{} no-snap:{} policy-block:{}",
        "snapshot-restore",
        node["snapshot_restore_attempt_total"].as_u64().unwrap_or(0),
        node["snapshot_restore_success_total"].as_u64().unwrap_or(0),
        node["snapshot_restore_failure_total"].as_u64().unwrap_or(0),
        node["snapshot_restore_not_found_total"].as_u64().unwrap_or(0),
        node["snapshot_restore_policy_block_total"].as_u64().unwrap_or(0)
    );
    println!("  {:<22} {}", "persistence", bool_text(node, "persistence_enabled"));
    println!("  {:<22} {}", "persistence-backup", bool_text(node, "persistence_backup_enabled"));
    println!("  {:<22} {}", "persistence-export", bool_text(node, "persistence_export_enabled"));
    println!("  {:<22} {}", "persistence-import", bool_text(node, "persistence_import_enabled"));
    println!("  {:<22} {}", "tenancy-enabled", bool_text(node, "tenancy_enabled"));
    println!("  {:<22} {}", "tenancy-default-ns", node["tenancy_default_namespace"].as_str().unwrap_or("?"));
    println!("  {:<22} {}", "tenancy-max-keys", num_text(node, "tenancy_max_keys_per_namespace"));
    println!("  {:<22} {}", "rate-limit", bool_text(node, "rate_limit_enabled"));
    println!("  {:<22} {}", "rate-limited-total", num_text(node, "rate_limited_requests_total"));
    println!("  {:<22} {}", "hot-key", bool_text(node, "hot_key_enabled"));
    println!("  {:<22} {}", "hot-key-coalesced", num_text(node, "hot_key_coalesced_hits_total"));
    println!("  {:<22} {}", "hot-key-fallback", num_text(node, "hot_key_fallback_exec_total"));
    println!("  {:<22} {}", "hot-key-wait-timeout", num_text(node, "hot_key_wait_timeout_total"));
    println!("  {:<22} {}", "hot-key-stale-served", num_text(node, "hot_key_stale_served_total"));
    println!("  {:<22} {}", "hot-key-inflight", num_text(node, "hot_key_inflight_keys"));
    println!("  {:<22} {}", "hot-key-stale-entries", num_text(node, "hot_key_stale_cache_entries"));
    println!("  {:<22} {}", "read-repair", bool_text(node, "read_repair_enabled"));
    println!("  {:<22} {}", "read-repair-triggered", num_text(node, "read_repair_trigger_total"));
    println!("  {:<22} {}", "read-repair-success", num_text(node, "read_repair_success_total"));
    println!("  {:<22} {}", "read-repair-throttled", num_text(node, "read_repair_throttled_total"));
    println!("  {:<22} {}", "namespace-quota-rej", num_text(node, "namespace_quota_reject_total"));
    println!("  {:<22} {}", "namespace-quota-rej-rpm", num_text(node, "namespace_quota_reject_rate_per_min"));
    println!("  {:<22} {}", "namespace-quota-trend", node["namespace_quota_reject_trend"].as_str().unwrap_or("?"));
    println!("  {:<22} {}", "namespace-quota-top", fmt_namespace_quota_top_usage(&node["namespace_quota_top_usage"]));
    println!("  {:<22} {}", "anti-entropy-runs", num_text(node, "anti_entropy_runs_total"));
    println!("  {:<22} {}", "anti-entropy-repair", num_text(node, "anti_entropy_repair_trigger_total"));
    println!("  {:<22} {}", "anti-entropy-last-lag", num_text(node, "anti_entropy_last_detected_lag"));
    println!("  {:<22} {}", "anti-entropy-key-checks", num_text(node, "anti_entropy_key_checks_total"));
    println!("  {:<22} {}", "anti-entropy-mismatch", num_text(node, "anti_entropy_key_mismatch_total"));
    println!("  {:<22} {}", "anti-entropy-full-runs", num_text(node, "anti_entropy_full_reconcile_runs_total"));
    println!("  {:<22} {}", "anti-entropy-full-checks", num_text(node, "anti_entropy_full_reconcile_key_checks_total"));
    println!("  {:<22} {}", "anti-entropy-full-miss", num_text(node, "anti_entropy_full_reconcile_mismatch_total"));
    println!("  {:<22} {}", "mixed-version-runs", num_text(node, "mixed_version_probe_runs_total"));
    println!("  {:<22} {}", "mixed-version-peers", num_text(node, "mixed_version_peers_detected_total"));
    println!("  {:<22} {}", "mixed-version-errors", num_text(node, "mixed_version_probe_errors_total"));
    println!("  {:<22} {}", "mixed-version-last", num_text(node, "mixed_version_last_detected_peer_count"));
    println!("  {:<22} {}", "circuit-breaker", bool_text(node, "circuit_breaker_enabled"));
    println!("  {:<22} {}", "circuit-state", node["circuit_breaker_state"].as_str().unwrap_or("?"));
    println!("  {:<22} {}", "circuit-open-total", num_text(node, "circuit_breaker_open_total"));
    println!("  {:<22} {}", "circuit-reject-total", num_text(node, "circuit_breaker_reject_total"));
    println!("  {:<22} {}", "client-requests", num_text(node, "client_requests_total"));
    println!(
        "  {:<22} tcp:{} http:{} internal:{}",
        "client-requests-by-src",
        node["client_requests_tcp_total"].as_u64().unwrap_or(0),
        node["client_requests_http_total"].as_u64().unwrap_or(0),
        node["client_requests_internal_total"].as_u64().unwrap_or(0)
    );
    println!(
        "  {:<22} <=1ms:{} <=5ms:{} <=20ms:{} <=100ms:{} <=500ms:{} >500ms:{}",
        "client-latency",
        node["client_request_latency_le_1ms_total"].as_u64().unwrap_or(0),
        node["client_request_latency_le_5ms_total"].as_u64().unwrap_or(0),
        node["client_request_latency_le_20ms_total"].as_u64().unwrap_or(0),
        node["client_request_latency_le_100ms_total"].as_u64().unwrap_or(0),
        node["client_request_latency_le_500ms_total"].as_u64().unwrap_or(0),
        node["client_request_latency_gt_500ms_total"].as_u64().unwrap_or(0)
    );
    println!(
        "  {:<22} p50:{}ms p90:{}ms p95:{}ms p99:{}ms",
        "client-latency-est",
        node["client_latency_p50_estimate_ms"].as_u64().unwrap_or(0),
        node["client_latency_p90_estimate_ms"].as_u64().unwrap_or(0),
        node["client_latency_p95_estimate_ms"].as_u64().unwrap_or(0),
        node["client_latency_p99_estimate_ms"].as_u64().unwrap_or(0)
    );
    println!(
        "  {:<22} tcp:{} http:{} internal:{}",
        "client-errors-by-src",
        node["client_errors_tcp_total"].as_u64().unwrap_or(0),
        node["client_errors_http_total"].as_u64().unwrap_or(0),
        node["client_errors_internal_total"].as_u64().unwrap_or(0)
    );
    println!(
        "  {:<22} total:{} auth:{} throttle:{} avail:{} valid:{} internal:{} other:{}",
        "client-errors",
        node["client_error_total"].as_u64().unwrap_or(0),
        node["client_error_auth_total"].as_u64().unwrap_or(0),
        node["client_error_throttle_total"].as_u64().unwrap_or(0),
        node["client_error_availability_total"].as_u64().unwrap_or(0),
        node["client_error_validation_total"].as_u64().unwrap_or(0),
        node["client_error_internal_total"].as_u64().unwrap_or(0),
        node["client_error_other_total"].as_u64().unwrap_or(0)
    );
}

fn describe_properties(entry: &Value) -> HashMap<String, String> {
    let mut props = HashMap::new();
    let Some(items) = entry["properties"].as_array() else {
        return props;
    };

    for pair in items {
        let Some(arr) = pair.as_array() else {
            continue;
        };
        let Some(key) = arr.first().and_then(|value| value.as_str()) else {
            continue;
        };
        let Some(value) = arr.get(1).and_then(|value| value.as_str()) else {
            continue;
        };
        props.insert(key.to_string(), value.to_string());
    }

    props
}

struct DoctorNodeReport {
    severity: &'static str,
    addr: String,
    status: String,
    heartbeat_ms: u64,
    quota_rpm: u64,
    quota_trend: String,
    quota_peak: u64,
    critical_hits: usize,
    warn_hits: usize,
    reasons: Vec<String>,
}

impl DoctorNodeReport {
    fn reasons_text(&self) -> String {
        if self.reasons.is_empty() {
            "-".to_string()
        } else {
            self.reasons.join(",")
        }
    }
}

fn doctor_verdict(critical: usize, warn: usize) -> &'static str {
    if critical > 0 {
        "CRITICAL"
    } else if warn > 0 {
        "WARN"
    } else {
        "OK"
    }
}

fn evaluate_doctor_node(
    node: &Value,
    describe_by_addr: &HashMap<String, HashMap<String, String>>,
) -> DoctorNodeReport {
    let addr = node["addr"].as_str().unwrap_or("?").to_string();
    let reachable = node["reachable"].as_bool().unwrap_or(false);
    let status = node["status"].as_str().unwrap_or("Unknown").to_string();
    let heartbeat_ms = node["heartbeat_ms"].as_u64().unwrap_or(0);
    let quota_rpm = node["namespace_quota_reject_rate_per_min"].as_u64().unwrap_or(0);
    let quota_trend = node["namespace_quota_reject_trend"].as_str().unwrap_or("?").to_string();
    let quota_peak = top_quota_usage_pct(&node["namespace_quota_top_usage"]);

    let mut severity = "OK";
    let mut critical_hits = 0usize;
    let mut warn_hits = 0usize;
    let mut reasons = Vec::new();

    if !reachable {
        severity = "CRITICAL";
        critical_hits += 1;
        reasons.push("unreachable".to_string());
    } else {
        if matches!(status.as_str(), "Inactive" | "Offline") {
            severity = "CRITICAL";
            critical_hits += 1;
            reasons.push("status unhealthy".to_string());
        } else if status == "Syncing" {
            if severity != "CRITICAL" {
                severity = "WARN";
            }
            warn_hits += 1;
            reasons.push("status syncing".to_string());
        }

        if heartbeat_ms > 2_000 {
            if severity == "OK" {
                severity = "WARN";
            }
            warn_hits += 1;
            reasons.push("high heartbeat".to_string());
        }

        if matches!(quota_trend.as_str(), "rising" | "surging") || quota_rpm >= 10 || quota_peak >= 90 {
            if severity == "OK" {
                severity = "WARN";
            }
            warn_hits += 1;
            reasons.push("quota pressure".to_string());
        }

        if let Some(props) = describe_by_addr.get(addr.as_str()) {
            if let Some(reason) = insecure_runtime_reason(props) {
                severity = "CRITICAL";
                critical_hits += 1;
                reasons.push(reason);
            }
            if let Some(reason) = tcp_doctor_reason(props) {
                severity = "CRITICAL";
                critical_hits += 1;
                reasons.push(reason);
            }
        }
    }

    DoctorNodeReport {
        severity,
        addr,
        status,
        heartbeat_ms,
        quota_rpm,
        quota_trend,
        quota_peak,
        critical_hits,
        warn_hits,
        reasons,
    }
}

fn property_bool(props: &HashMap<String, String>, key: &str) -> Option<bool> {
    match props.get(key).map(String::as_str) {
        Some("true") => Some(true),
        Some("false") => Some(false),
        _ => None,
    }
}

fn bool_text<'a>(node: &'a Value, key: &str) -> &'a str {
    match node[key].as_bool() {
        Some(true) => "true",
        Some(false) => "false",
        None => "?",
    }
}

fn num_text(node: &Value, key: &str) -> String {
    node[key]
        .as_u64()
        .map(|v| v.to_string())
        .unwrap_or_else(|| "?".to_string())
}
