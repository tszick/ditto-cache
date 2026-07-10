//! Production-readiness endpoint for the management UI.

use crate::api::SharedState;
use crate::app::nodes;
use crate::node_client::admin_rpc;
use axum::{extract::State, response::IntoResponse, Json};
use ditto_protocol::{AdminRequest, AdminResponse};
use serde::Serialize;
use std::{collections::HashMap, net::SocketAddr};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Ok,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Serialize)]
pub struct DoctorFinding {
    pub severity: Severity,
    pub scope: String,
    pub message: String,
    pub action: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DoctorSummary {
    pub total_nodes: usize,
    pub reachable_nodes: usize,
    pub active_nodes: usize,
    pub primary_count: usize,
    pub committed_index_spread: u64,
    pub quota_reject_rate_per_min: u64,
    pub quota_peak_usage_pct: u64,
    pub client_error_rate_pct: f64,
    pub p99_latency_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DoctorResponse {
    pub verdict: Severity,
    pub summary: DoctorSummary,
    pub findings: Vec<DoctorFinding>,
}

pub async fn doctor_status(State(state): State<SharedState>) -> impl IntoResponse {
    let node_infos = nodes::collect_nodes(state.clone()).await;
    let descriptions = collect_descriptions(state, &node_infos).await;
    Json(evaluate_doctor(&node_infos, &descriptions))
}

async fn collect_descriptions(
    state: SharedState,
    node_infos: &[nodes::NodeInfo],
) -> HashMap<String, HashMap<String, String>> {
    let mut descriptions = HashMap::new();
    for node in node_infos.iter().filter(|n| n.reachable) {
        let Ok(addr) = node.addr.parse::<SocketAddr>() else {
            continue;
        };
        if let Ok(AdminResponse::Properties(props)) =
            admin_rpc(addr, AdminRequest::Describe, state.tls.as_ref()).await
        {
            descriptions.insert(node.addr.clone(), props.into_iter().collect());
        }
    }
    descriptions
}

pub fn evaluate_doctor(
    nodes: &[nodes::NodeInfo],
    descriptions: &HashMap<String, HashMap<String, String>>,
) -> DoctorResponse {
    let reachable = nodes.iter().filter(|n| n.reachable).count();
    let active = nodes
        .iter()
        .filter(|n| n.reachable && n.status.as_deref() == Some("Active"))
        .count();
    let primary_count = nodes
        .iter()
        .filter(|n| n.reachable && n.is_primary == Some(true))
        .count();
    let committed: Vec<u64> = nodes.iter().filter_map(|n| n.committed_index).collect();
    let committed_index_spread = match (committed.iter().min(), committed.iter().max()) {
        (Some(min), Some(max)) => max.saturating_sub(*min),
        _ => 0,
    };
    let quota_reject_rate_per_min = sum(nodes, |n| n.namespace_quota_reject_rate_per_min);
    let quota_peak_usage_pct = nodes
        .iter()
        .flat_map(|n| n.namespace_quota_top_usage.as_deref().unwrap_or(&[]))
        .map(|q| q.usage_pct)
        .max()
        .unwrap_or(0);
    let client_requests = sum(nodes, |n| n.client_requests_total);
    let client_errors = sum(nodes, |n| n.client_error_total);
    let client_error_rate_pct = if client_requests > 0 {
        (client_errors as f64 * 1000.0 / client_requests as f64).round() / 10.0
    } else {
        0.0
    };
    let p99_latency_ms = nodes
        .iter()
        .filter_map(|n| n.client_latency_p99_estimate_ms)
        .max()
        .unwrap_or(0);

    let mut findings = Vec::new();

    if nodes.is_empty() {
        findings.push(critical(
            "cluster",
            "No nodes are visible to management.",
            "Check management seed configuration and cluster DNS.",
        ));
    }
    let unreachable = nodes.len().saturating_sub(reachable);
    if unreachable > 0 {
        findings.push(critical(
            "cluster",
            format!("{unreachable} node(s) are unreachable."),
            "Inspect node health, networking, and cluster/admin mTLS connectivity.",
        ));
    }
    if active == 0 {
        findings.push(critical(
            "cluster",
            "No active nodes are available.",
            "Restore at least one active node before accepting production traffic.",
        ));
    }
    if primary_count != 1 {
        findings.push(critical(
            "cluster",
            format!("Primary count is {primary_count}."),
            "Check election state and node membership before writes continue.",
        ));
    }
    if committed_index_spread > 10 {
        findings.push(warning(
            "replication",
            format!("Committed index spread is {committed_index_spread}."),
            "Watch recovery progress and inspect lagging nodes if the spread persists.",
        ));
    }

    for node in nodes.iter().filter(|n| n.reachable) {
        if node.persistence_enabled == Some(false) {
            findings.push(warning(
                node_label(node),
                "Persistence is disabled.",
                "Confirm this is intended for the environment before production use.",
            ));
        }
        if node.persistence_backup_enabled == Some(false) {
            findings.push(warning(
                node_label(node),
                "Backup capability is disabled.",
                "Enable backup gates before relying on local snapshot operations.",
            ));
        }
        let restore_failures = node.snapshot_restore_failure_total.unwrap_or(0)
            + node.snapshot_restore_policy_block_total.unwrap_or(0);
        if restore_failures > 0 {
            findings.push(warning(
                node_label(node),
                format!("{restore_failures} restore failure/block event(s)."),
                "Inspect snapshot policy gates and the latest restore logs.",
            ));
        }
        if matches!(
            node.namespace_quota_reject_trend.as_deref(),
            Some("rising" | "surging")
        ) || node.namespace_quota_reject_rate_per_min.unwrap_or(0) >= 10
        {
            findings.push(warning(
                node_label(node),
                "Namespace quota rejects are elevated.",
                "Review top namespace usage and either reduce load or raise the quota.",
            ));
        }
        if node.client_latency_p99_estimate_ms.unwrap_or(0) >= 500 {
            findings.push(warning(
                node_label(node),
                format!(
                    "Client p99 latency estimate is {}ms.",
                    node.client_latency_p99_estimate_ms.unwrap_or(0)
                ),
                "Inspect hot keys, circuit breaker state, and resource pressure.",
            ));
        }
        if let Some(props) = descriptions.get(&node.addr) {
            if property_bool(props, "insecure-runtime-enabled") == Some(true) {
                findings.push(critical(
                    node_label(node),
                    "Insecure runtime bypass is enabled.",
                    "Remove DITTO_INSECURE before any non-dev deployment.",
                ));
            }
            if property_bool(props, "strict-security-enforced") == Some(false) {
                findings.push(critical(
                    node_label(node),
                    "Strict security is not enforced.",
                    "Enable mTLS, HTTP auth, and production startup guardrails.",
                ));
            }
            if property_bool(props, "tcp-production-safe") == Some(false) {
                findings.push(critical(
                    node_label(node),
                    "TCP client exposure is not production-safe.",
                    "Bind TCP to loopback or configure TCP client auth before exposure.",
                ));
            }
        }
    }

    if findings.is_empty() {
        findings.push(DoctorFinding {
            severity: Severity::Ok,
            scope: "cluster".into(),
            message: "No production readiness issues detected.".into(),
            action: "Continue routine monitoring.".into(),
        });
    }

    let verdict = findings
        .iter()
        .map(|f| f.severity)
        .max()
        .unwrap_or(Severity::Ok);

    DoctorResponse {
        verdict,
        summary: DoctorSummary {
            total_nodes: nodes.len(),
            reachable_nodes: reachable,
            active_nodes: active,
            primary_count,
            committed_index_spread,
            quota_reject_rate_per_min,
            quota_peak_usage_pct,
            client_error_rate_pct,
            p99_latency_ms,
        },
        findings,
    }
}

fn node_label(node: &nodes::NodeInfo) -> String {
    node.node_name.clone().unwrap_or_else(|| node.addr.clone())
}

fn property_bool(props: &HashMap<String, String>, name: &str) -> Option<bool> {
    props
        .get(name)
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1" || v.eq_ignore_ascii_case("yes"))
}

fn sum<F>(nodes: &[nodes::NodeInfo], field: F) -> u64
where
    F: Fn(&nodes::NodeInfo) -> Option<u64>,
{
    nodes.iter().filter_map(field).sum()
}

fn critical(
    scope: impl Into<String>,
    message: impl Into<String>,
    action: impl Into<String>,
) -> DoctorFinding {
    DoctorFinding {
        severity: Severity::Critical,
        scope: scope.into(),
        message: message.into(),
        action: action.into(),
    }
}

fn warning(
    scope: impl Into<String>,
    message: impl Into<String>,
    action: impl Into<String>,
) -> DoctorFinding {
    DoctorFinding {
        severity: Severity::Warning,
        scope: scope.into(),
        message: message.into(),
        action: action.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str) -> nodes::NodeInfo {
        nodes::NodeInfo {
            addr: format!("{name}:7779"),
            reachable: true,
            heartbeat_ms: Some(1),
            node_name: Some(name.into()),
            node_id: Some(name.into()),
            status: Some("Active".into()),
            is_primary: Some(name == "node-1"),
            committed_index: Some(10),
            memory_used_bytes: Some(10),
            memory_max_bytes: Some(100),
            uptime_secs: Some(60),
            backup_dir_bytes: Some(0),
            snapshot_last_load_path: None,
            snapshot_last_load_duration_ms: Some(0),
            snapshot_last_load_entries: Some(0),
            snapshot_last_load_age_secs: None,
            snapshot_restore_attempt_total: Some(0),
            snapshot_restore_success_total: Some(0),
            snapshot_restore_failure_total: Some(0),
            snapshot_restore_not_found_total: Some(0),
            snapshot_restore_policy_block_total: Some(0),
            persistence_enabled: Some(true),
            persistence_backup_enabled: Some(true),
            persistence_export_enabled: Some(true),
            persistence_import_enabled: Some(true),
            tenancy_enabled: Some(true),
            tenancy_default_namespace: Some("default".into()),
            tenancy_max_keys_per_namespace: Some(0),
            rate_limit_enabled: Some(false),
            rate_limited_requests_total: Some(0),
            hot_key_enabled: Some(true),
            hot_key_coalesced_hits_total: Some(0),
            hot_key_fallback_exec_total: Some(0),
            hot_key_wait_timeout_total: Some(0),
            hot_key_stale_served_total: Some(0),
            hot_key_inflight_keys: Some(0),
            hot_key_stale_cache_entries: Some(0),
            read_repair_enabled: Some(true),
            read_repair_trigger_total: Some(0),
            read_repair_success_total: Some(0),
            read_repair_throttled_total: Some(0),
            namespace_quota_reject_total: Some(0),
            namespace_quota_reject_rate_per_min: Some(0),
            namespace_quota_reject_trend: Some("steady".into()),
            namespace_quota_top_usage: Some(vec![]),
            anti_entropy_runs_total: Some(0),
            anti_entropy_repair_trigger_total: Some(0),
            anti_entropy_last_detected_lag: Some(0),
            anti_entropy_key_checks_total: Some(0),
            anti_entropy_key_mismatch_total: Some(0),
            anti_entropy_full_reconcile_runs_total: Some(0),
            anti_entropy_full_reconcile_key_checks_total: Some(0),
            anti_entropy_full_reconcile_mismatch_total: Some(0),
            mixed_version_probe_runs_total: Some(0),
            mixed_version_peers_detected_total: Some(0),
            mixed_version_probe_errors_total: Some(0),
            mixed_version_last_detected_peer_count: Some(0),
            circuit_breaker_enabled: Some(false),
            circuit_breaker_state: Some("closed".into()),
            circuit_breaker_open_total: Some(0),
            circuit_breaker_reject_total: Some(0),
            client_requests_total: Some(100),
            client_requests_tcp_total: Some(50),
            client_requests_http_total: Some(50),
            client_requests_internal_total: Some(0),
            client_request_latency_le_1ms_total: Some(0),
            client_request_latency_le_5ms_total: Some(0),
            client_request_latency_le_20ms_total: Some(0),
            client_request_latency_le_100ms_total: Some(0),
            client_request_latency_le_500ms_total: Some(0),
            client_request_latency_gt_500ms_total: Some(0),
            client_latency_p50_estimate_ms: Some(1),
            client_latency_p90_estimate_ms: Some(5),
            client_latency_p95_estimate_ms: Some(20),
            client_latency_p99_estimate_ms: Some(100),
            client_error_total: Some(0),
            client_errors_tcp_total: Some(0),
            client_errors_http_total: Some(0),
            client_errors_internal_total: Some(0),
            client_error_auth_total: Some(0),
            client_error_throttle_total: Some(0),
            client_error_availability_total: Some(0),
            client_error_validation_total: Some(0),
            client_error_internal_total: Some(0),
            client_error_other_total: Some(0),
            key_count: Some(0),
            evictions: Some(0),
            hit_count: Some(0),
            miss_count: Some(0),
        }
    }

    #[test]
    fn doctor_reports_ok_for_healthy_nodes() {
        let nodes = vec![node("node-1"), node("node-2")];
        let response = evaluate_doctor(&nodes, &HashMap::new());
        assert_eq!(response.verdict, Severity::Ok);
        assert_eq!(response.summary.primary_count, 1);
    }

    #[test]
    fn doctor_flags_insecure_runtime_and_unreachable_nodes() {
        let mut nodes = vec![node("node-1"), node("node-2")];
        nodes[1].reachable = false;
        let mut props = HashMap::new();
        props.insert("insecure-runtime-enabled".into(), "true".into());
        let descriptions = HashMap::from([(nodes[0].addr.clone(), props)]);

        let response = evaluate_doctor(&nodes, &descriptions);
        assert_eq!(response.verdict, Severity::Critical);
        assert!(response
            .findings
            .iter()
            .any(|f| f.message.contains("Insecure runtime")));
        assert!(response
            .findings
            .iter()
            .any(|f| f.message.contains("unreachable")));
    }
}
