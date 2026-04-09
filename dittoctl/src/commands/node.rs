use crate::{
    client::{enc, mgmt_get, mgmt_post},
    config::CtlConfig,
};
use anyhow::{bail, Result};
use clap::Subcommand;

#[derive(Debug, Subcommand)]
pub enum NodeCommand {
    /// Show all properties of a node.
    Describe { target: String },
    /// Get a single property value.
    Get { property: String, target: String },
    /// Set a property on a node (or a local dittoctl config value).
    Set {
        property: String,
        target: String,
        value: String,
    },
    /// List sub-resources of a node.
    List {
        /// "ports"
        what: String,
        target: String,
    },
    /// Trigger an immediate backup on a node.
    Backup { target: String },
    /// Restore the latest local snapshot on a node.
    RestoreSnapshot { target: String },
    /// Show node health status (id, memory, heartbeat, uptime, backup storage).
    Status { target: String },
    /// Run quick diagnostics and return non-zero on critical findings.
    Doctor { target: String },
}

pub async fn run(cmd: NodeCommand, cfg: &mut CtlConfig, client: &reqwest::Client) -> Result<()> {
    let base = &cfg.mgmt.url;

    match cmd {
        NodeCommand::Describe { target } => {
            let url = format!("{}/api/nodes/{}/describe", base, enc(&target));
            let data = mgmt_get(client, &url).await?;
            let entries = data.as_array().cloned().unwrap_or_default();
            for entry in entries {
                println!("\n  Node: {}", entry["addr"].as_str().unwrap_or("?"));
                if let Some(err) = entry["error"].as_str() {
                    println!("  Error: {}", err);
                    continue;
                }
                println!("  {:<22} {}", "property", "value");
                println!("  {}", "─".repeat(50));
                if let Some(props) = entry["properties"].as_array() {
                    for pair in props {
                        if let Some(arr) = pair.as_array() {
                            let k = arr.get(0).and_then(|v| v.as_str()).unwrap_or("");
                            let v = arr.get(1).and_then(|v| v.as_str()).unwrap_or("");
                            println!("  {:<22} {}", k, v);
                        }
                    }
                }
            }
        }

        NodeCommand::Get { property, target } => {
            let url = format!(
                "{}/api/nodes/{}/property/{}",
                base,
                enc(&target),
                enc(&property)
            );
            let data = mgmt_get(client, &url).await?;
            match data["value"].as_str() {
                Some(v) => println!("{}", v),
                None => println!("(not found)"),
            }
        }

        NodeCommand::Set {
            property,
            target,
            value,
        } => {
            // Local dittoctl config mutations (target == "local" and special keys)
            if target == "local" {
                match property.as_str() {
                    "timeout" => {
                        match value.parse::<u64>() {
                            Ok(ms) => {
                                cfg.mgmt.timeout_ms = ms;
                                cfg.save()?;
                                println!("  timeout → {}ms", ms);
                            }
                            Err(_) => eprintln!("  Invalid timeout value (expected integer ms)."),
                        }
                        return Ok(());
                    }
                    "format" => {
                        cfg.output.format = value.clone();
                        cfg.save()?;
                        println!("  output format → {}", value);
                        return Ok(());
                    }
                    "url" => {
                        cfg.mgmt.url = value.clone();
                        cfg.save()?;
                        println!("  mgmt url → {}", value);
                        return Ok(());
                    }
                    "seeds" => {
                        eprintln!("  'seeds' is now configured in ditto-mgmt. Set it in ~/.config/ditto/mgmt.toml");
                        return Ok(());
                    }
                    _ => {} // fall through to remote set
                }
            }

            let url = format!(
                "{}/api/nodes/{}/property/{}",
                base,
                enc(&target),
                enc(&property)
            );
            let data = mgmt_post(client, &url, serde_json::json!({ "value": value })).await?;
            if let Some(results) = data.as_array() {
                for r in results {
                    let addr = r["addr"].as_str().unwrap_or("?");
                    if r["ok"].as_bool().unwrap_or(false) {
                        println!("  {} → {} = {}", addr, property, value);
                    } else {
                        eprintln!(
                            "  Error from {}: {}",
                            addr,
                            r["error"].as_str().unwrap_or("unknown")
                        );
                    }
                }
            }
        }

        NodeCommand::List { what, target } => match what.as_str() {
            "ports" => {
                let url = format!("{}/api/nodes/{}/describe", base, enc(&target));
                let data = mgmt_get(client, &url).await?;
                let entries = data.as_array().cloned().unwrap_or_default();
                for entry in entries {
                    println!("\n  Node: {}", entry["addr"].as_str().unwrap_or("?"));
                    if let Some(props) = entry["properties"].as_array() {
                        for pair in props {
                            if let Some(arr) = pair.as_array() {
                                let k = arr.get(0).and_then(|v| v.as_str()).unwrap_or("");
                                let v = arr.get(1).and_then(|v| v.as_str()).unwrap_or("");
                                if k.contains("port") {
                                    println!("  {:<22} {}", k, v);
                                }
                            }
                        }
                    }
                }
            }
            other => eprintln!("Unknown list target '{}'. Use: ports", other),
        },

        NodeCommand::Backup { target } => {
            let url = format!("{}/api/nodes/{}/backup", base, enc(&target));
            let data = mgmt_post(client, &url, serde_json::json!({})).await?;
            let results = data.as_array().cloned().unwrap_or_default();
            for r in results {
                let addr = r["addr"].as_str().unwrap_or("?");
                if r["ok"].as_bool().unwrap_or(false) {
                    println!(
                        "  {} ← backup written: {}",
                        addr,
                        r["path"].as_str().unwrap_or("?")
                    );
                } else {
                    eprintln!(
                        "  Error from {}: {}",
                        addr,
                        r["error"].as_str().unwrap_or("unknown")
                    );
                }
            }
        }

        NodeCommand::RestoreSnapshot { target } => {
            let url = format!("{}/api/nodes/{}/restore-snapshot", base, enc(&target));
            let data = mgmt_post(client, &url, serde_json::json!({})).await?;
            let results = data.as_array().cloned().unwrap_or_default();
            for r in results {
                let addr = r["addr"].as_str().unwrap_or("?");
                if r["ok"].as_bool().unwrap_or(false) {
                    println!(
                        "  {} <- snapshot restored: {} (entries={}, {}ms)",
                        addr,
                        r["path"].as_str().unwrap_or("?"),
                        r["entries"].as_u64().unwrap_or(0),
                        r["duration_ms"].as_u64().unwrap_or(0)
                    );
                } else {
                    eprintln!(
                        "  Error from {}: {}",
                        addr,
                        r["error"].as_str().unwrap_or("unknown")
                    );
                }
            }
        }

        NodeCommand::Status { target } => {
            let url = format!("{}/api/nodes/{}/status", base, enc(&target));
            let data = mgmt_get(client, &url).await?;
            let nodes = data.as_array().cloned().unwrap_or_default();
            for node in nodes {
                println!("\n  Node: {}", node["addr"].as_str().unwrap_or("?"));
                if node["reachable"].as_bool() == Some(false) {
                    println!("  (unreachable)");
                    continue;
                }
                println!(
                    "  {:<22} {}",
                    "id",
                    node["node_name"].as_str().unwrap_or("?")
                );
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
                println!(
                    "  {:<22} {}ms",
                    "heartbeat",
                    node["heartbeat_ms"].as_u64().unwrap_or(0)
                );
                println!(
                    "  {:<22} {}",
                    "uptime",
                    fmt_uptime(node["uptime_secs"].as_u64().unwrap_or(0))
                );
                println!(
                    "  {:<22} {} bytes",
                    "backup-storage",
                    node["backup_dir_bytes"].as_u64().unwrap_or(0)
                );
                println!(
                    "  {:<22} {}",
                    "snapshot-load-path",
                    node["snapshot_last_load_path"].as_str().unwrap_or("-")
                );
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
                    node["snapshot_restore_not_found_total"]
                        .as_u64()
                        .unwrap_or(0),
                    node["snapshot_restore_policy_block_total"]
                        .as_u64()
                        .unwrap_or(0)
                );
                println!(
                    "  {:<22} {}",
                    "persistence",
                    node["persistence_enabled"]
                        .as_bool()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "persistence-backup",
                    node["persistence_backup_enabled"]
                        .as_bool()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "persistence-export",
                    node["persistence_export_enabled"]
                        .as_bool()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "persistence-import",
                    node["persistence_import_enabled"]
                        .as_bool()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "tenancy-enabled",
                    node["tenancy_enabled"]
                        .as_bool()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "tenancy-default-ns",
                    node["tenancy_default_namespace"].as_str().unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "tenancy-max-keys",
                    node["tenancy_max_keys_per_namespace"]
                        .as_u64()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "rate-limit",
                    node["rate_limit_enabled"]
                        .as_bool()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "rate-limited-total",
                    node["rate_limited_requests_total"]
                        .as_u64()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "hot-key",
                    node["hot_key_enabled"]
                        .as_bool()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "hot-key-coalesced",
                    node["hot_key_coalesced_hits_total"]
                        .as_u64()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "hot-key-fallback",
                    node["hot_key_fallback_exec_total"]
                        .as_u64()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "hot-key-inflight",
                    node["hot_key_inflight_keys"]
                        .as_u64()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "read-repair",
                    node["read_repair_enabled"]
                        .as_bool()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "read-repair-triggered",
                    node["read_repair_trigger_total"]
                        .as_u64()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "read-repair-success",
                    node["read_repair_success_total"]
                        .as_u64()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "read-repair-throttled",
                    node["read_repair_throttled_total"]
                        .as_u64()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "namespace-quota-rej",
                    node["namespace_quota_reject_total"]
                        .as_u64()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "namespace-quota-rej-rpm",
                    node["namespace_quota_reject_rate_per_min"]
                        .as_u64()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "namespace-quota-trend",
                    node["namespace_quota_reject_trend"].as_str().unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "namespace-quota-top",
                    fmt_namespace_quota_top_usage(&node["namespace_quota_top_usage"])
                );
                println!(
                    "  {:<22} {}",
                    "anti-entropy-runs",
                    node["anti_entropy_runs_total"]
                        .as_u64()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "anti-entropy-repair",
                    node["anti_entropy_repair_trigger_total"]
                        .as_u64()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "anti-entropy-last-lag",
                    node["anti_entropy_last_detected_lag"]
                        .as_u64()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "anti-entropy-key-checks",
                    node["anti_entropy_key_checks_total"]
                        .as_u64()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "anti-entropy-mismatch",
                    node["anti_entropy_key_mismatch_total"]
                        .as_u64()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "anti-entropy-full-runs",
                    node["anti_entropy_full_reconcile_runs_total"]
                        .as_u64()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "anti-entropy-full-checks",
                    node["anti_entropy_full_reconcile_key_checks_total"]
                        .as_u64()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "anti-entropy-full-miss",
                    node["anti_entropy_full_reconcile_mismatch_total"]
                        .as_u64()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "mixed-version-runs",
                    node["mixed_version_probe_runs_total"]
                        .as_u64()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "mixed-version-peers",
                    node["mixed_version_peers_detected_total"]
                        .as_u64()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "mixed-version-errors",
                    node["mixed_version_probe_errors_total"]
                        .as_u64()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "mixed-version-last",
                    node["mixed_version_last_detected_peer_count"]
                        .as_u64()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "circuit-breaker",
                    node["circuit_breaker_enabled"]
                        .as_bool()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "circuit-state",
                    node["circuit_breaker_state"].as_str().unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "circuit-open-total",
                    node["circuit_breaker_open_total"]
                        .as_u64()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "circuit-reject-total",
                    node["circuit_breaker_reject_total"]
                        .as_u64()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
                println!(
                    "  {:<22} {}",
                    "client-requests",
                    node["client_requests_total"]
                        .as_u64()
                        .map(|v| v.to_string())
                        .as_deref()
                        .unwrap_or("?")
                );
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
                    node["client_request_latency_le_1ms_total"]
                        .as_u64()
                        .unwrap_or(0),
                    node["client_request_latency_le_5ms_total"]
                        .as_u64()
                        .unwrap_or(0),
                    node["client_request_latency_le_20ms_total"]
                        .as_u64()
                        .unwrap_or(0),
                    node["client_request_latency_le_100ms_total"]
                        .as_u64()
                        .unwrap_or(0),
                    node["client_request_latency_le_500ms_total"]
                        .as_u64()
                        .unwrap_or(0),
                    node["client_request_latency_gt_500ms_total"]
                        .as_u64()
                        .unwrap_or(0)
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
                    node["client_error_availability_total"]
                        .as_u64()
                        .unwrap_or(0),
                    node["client_error_validation_total"].as_u64().unwrap_or(0),
                    node["client_error_internal_total"].as_u64().unwrap_or(0),
                    node["client_error_other_total"].as_u64().unwrap_or(0)
                );
            }
        }

        NodeCommand::Doctor { target } => {
            let status_url = format!("{}/api/nodes/{}/status", base, enc(&target));
            let cluster_url = format!("{}/api/cluster", base);
            let status_data = mgmt_get(client, &status_url).await?;
            let cluster_data = mgmt_get(client, &cluster_url).await?;
            let nodes = status_data.as_array().cloned().unwrap_or_default();

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
                let addr = node["addr"].as_str().unwrap_or("?");
                let reachable = node["reachable"].as_bool().unwrap_or(false);
                let status = node["status"].as_str().unwrap_or("Unknown");
                let heartbeat = node["heartbeat_ms"].as_u64().unwrap_or(0);
                let quota_rpm = node["namespace_quota_reject_rate_per_min"]
                    .as_u64()
                    .unwrap_or(0);
                let quota_trend = node["namespace_quota_reject_trend"].as_str().unwrap_or("?");
                let quota_peak = top_quota_usage_pct(&node["namespace_quota_top_usage"]);

                let mut sev = "OK";
                let mut reasons: Vec<&str> = Vec::new();

                if !reachable {
                    sev = "CRITICAL";
                    critical += 1;
                    reasons.push("unreachable");
                } else {
                    if matches!(status, "Inactive" | "Offline") {
                        sev = "CRITICAL";
                        critical += 1;
                        reasons.push("status unhealthy");
                    } else if status == "Syncing" {
                        if sev != "CRITICAL" {
                            sev = "WARN";
                        }
                        warn += 1;
                        reasons.push("status syncing");
                    }

                    if heartbeat > 2_000 {
                        if sev == "OK" {
                            sev = "WARN";
                        }
                        warn += 1;
                        reasons.push("high heartbeat");
                    }

                    if matches!(quota_trend, "rising" | "surging")
                        || quota_rpm >= 10
                        || quota_peak >= 90
                    {
                        if sev == "OK" {
                            sev = "WARN";
                        }
                        warn += 1;
                        reasons.push("quota pressure");
                    }
                }

                let reason_str = if reasons.is_empty() {
                    "-".to_string()
                } else {
                    reasons.join(",")
                };

                println!(
                    "  [{}] {} status={} hb={}ms quota={}rpm trend={} top={}%",
                    sev, addr, status, heartbeat, quota_rpm, quota_trend, quota_peak
                );
                println!("       notes={}", reason_str);
            }

            let verdict = if critical > 0 {
                "CRITICAL"
            } else if warn > 0 {
                "WARN"
            } else {
                "OK"
            };
            println!("  {}", "-".repeat(52));
            println!("  verdict: {}", verdict);

            if !notes.is_empty() {
                for note in notes {
                    println!("  note: {}", note);
                }
            }

            if critical > 0 {
                bail!(
                    "doctor found {} critical issue(s) (warnings={})",
                    critical,
                    warn
                );
            }
        }
    }
    Ok(())
}

fn fmt_uptime(secs: u64) -> String {
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

fn fmt_namespace_quota_top_usage(v: &serde_json::Value) -> String {
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

fn top_quota_usage_pct(v: &serde_json::Value) -> u64 {
    let Some(items) = v.as_array() else {
        return 0;
    };
    items
        .iter()
        .filter_map(|item| item["usage_pct"].as_u64())
        .max()
        .unwrap_or(0)
}
