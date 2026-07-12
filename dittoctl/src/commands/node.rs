use crate::{
    client::{enc, mgmt_get, mgmt_post},
    commands::node_support::{render_node_status, run_doctor},
    config::CtlConfig,
};
use anyhow::Result;
use clap::{Subcommand, ValueEnum};

#[derive(Clone, Debug, ValueEnum)]
pub enum NodeListWhat {
    Ports,
}

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
        what: NodeListWhat,
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
    let base = cfg.mgmt.url.clone();

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
                println!("  {:<22} value", "property");
                println!("  {}", "-".repeat(50));
                if let Some(props) = entry["properties"].as_array() {
                    for pair in props {
                        if let Some(arr) = pair.as_array() {
                            let k = arr.first().and_then(|v| v.as_str()).unwrap_or("");
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
            if handle_local_config_set(cfg, &target, &property, &value)? {
                return Ok(());
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
                        println!("  {} -> {} = {}", addr, property, value);
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
        NodeCommand::List { what, target } => match what {
            NodeListWhat::Ports => {
                let url = format!("{}/api/nodes/{}/describe", base, enc(&target));
                let data = mgmt_get(client, &url).await?;
                let entries = data.as_array().cloned().unwrap_or_default();
                for entry in entries {
                    println!("\n  Node: {}", entry["addr"].as_str().unwrap_or("?"));
                    if let Some(props) = entry["properties"].as_array() {
                        for pair in props {
                            if let Some(arr) = pair.as_array() {
                                let k = arr.first().and_then(|v| v.as_str()).unwrap_or("");
                                let v = arr.get(1).and_then(|v| v.as_str()).unwrap_or("");
                                if k.contains("port") {
                                    println!("  {:<22} {}", k, v);
                                }
                            }
                        }
                    }
                }
            }
        },

        NodeCommand::Backup { target } => {
            let url = format!("{}/api/nodes/{}/backup", base, enc(&target));
            let data = mgmt_post(client, &url, serde_json::json!({})).await?;
            let results = data.as_array().cloned().unwrap_or_default();
            for r in results {
                let addr = r["addr"].as_str().unwrap_or("?");
                if r["ok"].as_bool().unwrap_or(false) {
                    println!("  {} <- backup written: {}", addr, r["path"].as_str().unwrap_or("?"));
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
                render_node_status(&node);
            }
        }

        NodeCommand::Doctor { target } => {
            run_doctor(&base, &target, client).await?;
        }
    }
    Ok(())
}

fn handle_local_config_set(
    cfg: &mut CtlConfig,
    target: &str,
    property: &str,
    value: &str,
) -> Result<bool> {
    if target != "local" {
        return Ok(false);
    }

    match property {
        "timeout" => {
            match value.parse::<u64>() {
                Ok(ms) => {
                    cfg.mgmt.timeout_ms = ms;
                    cfg.save()?;
                    println!("  timeout -> {}ms", ms);
                }
                Err(_) => eprintln!("  Invalid timeout value (expected integer ms)."),
            }
            Ok(true)
        }
        "format" => {
            cfg.output.format = value.to_string();
            cfg.save()?;
            println!("  output format -> {}", value);
            Ok(true)
        }
        "url" => {
            cfg.mgmt.url = value.to_string();
            cfg.save()?;
            println!("  mgmt url -> {}", value);
            Ok(true)
        }
        "seeds" => {
            eprintln!(
                "  'seeds' is now configured in ditto-mgmt. Set it in ~/.config/ditto/mgmt.toml"
            );
            Ok(true)
        }
        _ => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::node_support::{
        build_describe_property_index, fmt_namespace_quota_top_usage, fmt_uptime,
        insecure_runtime_reason, tcp_doctor_reason, top_quota_usage_pct,
    };
    use serde_json::json;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    async fn http_responses(
        bodies: Vec<&'static str>,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            let mut requests = Vec::new();
            for body in bodies {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_request(&mut stream).await;
                let body_bytes = body.as_bytes();
                let header = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    body_bytes.len()
                );
                stream.write_all(header.as_bytes()).await.unwrap();
                stream.write_all(body_bytes).await.unwrap();
                requests.push(request);
            }
            requests
        });

        (format!("http://{addr}"), handle)
    }

    async fn read_request(stream: &mut tokio::net::TcpStream) -> String {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 1024];
        let header_end;
        loop {
            let n = stream.read(&mut chunk).await.unwrap();
            assert!(n > 0, "connection closed before headers");
            buf.extend_from_slice(&chunk[..n]);
            if let Some(pos) = find_header_end(&buf) {
                header_end = pos;
                break;
            }
        }

        let headers = String::from_utf8_lossy(&buf[..header_end]).to_string();
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.strip_prefix("content-length: ")
                    .or_else(|| line.strip_prefix("Content-Length: "))
                    .and_then(|value| value.trim().parse::<usize>().ok())
            })
            .unwrap_or(0);
        let body_start = header_end + 4;
        while buf.len() < body_start + content_length {
            let n = stream.read(&mut chunk).await.unwrap();
            assert!(n > 0, "connection closed before body");
            buf.extend_from_slice(&chunk[..n]);
        }

        String::from_utf8_lossy(&buf).to_string()
    }

    fn find_header_end(buf: &[u8]) -> Option<usize> {
        buf.windows(4).position(|w| w == b"\r\n\r\n")
    }

    fn cfg(base: String) -> CtlConfig {
        let mut cfg = CtlConfig::default();
        cfg.mgmt.url = base;
        cfg
    }

    #[test]
    fn fmt_uptime_formats_short_and_long_ranges() {
        assert_eq!(fmt_uptime(5), "5s");
        assert_eq!(fmt_uptime(125), "2m 5s");
        assert_eq!(fmt_uptime(3_726), "1h 2m 6s");
    }

    #[test]
    fn fmt_namespace_quota_top_usage_handles_missing_and_empty_values() {
        assert_eq!(fmt_namespace_quota_top_usage(&json!(null)), "-");
        assert_eq!(fmt_namespace_quota_top_usage(&json!([])), "-");
    }

    #[test]
    fn fmt_namespace_quota_top_usage_renders_compact_summary() {
        let value = json!([
            { "namespace": "tenant-a", "key_count": 9, "quota_limit": 10, "usage_pct": 90 },
            { "namespace": "tenant-b", "key_count": 3, "quota_limit": 20, "usage_pct": 15 }
        ]);
        assert_eq!(
            fmt_namespace_quota_top_usage(&value),
            "tenant-a:9/10(90%),tenant-b:3/20(15%)"
        );
    }

    #[test]
    fn top_quota_usage_pct_returns_highest_percentage() {
        let value = json!([
            { "usage_pct": 33 },
            { "usage_pct": 88 },
            { "usage_pct": 57 }
        ]);
        assert_eq!(top_quota_usage_pct(&value), 88);
    }

    #[test]
    fn top_quota_usage_pct_returns_zero_for_invalid_input() {
        assert_eq!(top_quota_usage_pct(&json!(null)), 0);
        assert_eq!(
            top_quota_usage_pct(&json!([{ "namespace": "tenant-a" }])),
            0
        );
    }

    #[test]
    fn build_describe_property_index_skips_error_entries() {
        let value = json!([
            {
                "addr": "node-a",
                "properties": [
                    ["client-auth-enabled", "true"],
                    ["tcp-production-safe", "true"]
                ]
            },
            {
                "addr": "node-b",
                "error": "timeout"
            }
        ]);

        let index = build_describe_property_index(&value);
        assert_eq!(index.len(), 1);
        assert_eq!(
            index
                .get("node-a")
                .and_then(|props| props.get("client-auth-enabled"))
                .map(String::as_str),
            Some("true")
        );
        assert!(!index.contains_key("node-b"));
    }

    #[test]
    fn tcp_doctor_reason_flags_unsupported_exposure() {
        let props = std::collections::HashMap::from([
            ("client-auth-enabled".to_string(), "false".to_string()),
            (
                "tcp-client-bind-loopback-only".to_string(),
                "false".to_string(),
            ),
            ("tcp-production-safe".to_string(), "false".to_string()),
        ]);

        assert_eq!(
            tcp_doctor_reason(&props).as_deref(),
            Some("tcp exposure unsupported (auth-enabled=false, loopback-only=false)")
        );
    }

    #[test]
    fn tcp_doctor_reason_ignores_supported_exposure() {
        let props = std::collections::HashMap::from([
            ("client-auth-enabled".to_string(), "true".to_string()),
            (
                "tcp-client-bind-loopback-only".to_string(),
                "false".to_string(),
            ),
            ("tcp-production-safe".to_string(), "true".to_string()),
        ]);

        assert_eq!(tcp_doctor_reason(&props), None);
    }

    #[test]
    fn insecure_runtime_reason_flags_dev_bypass() {
        let props = std::collections::HashMap::from([(
            "insecure-runtime-enabled".to_string(),
            "true".to_string(),
        )]);

        assert_eq!(
            insecure_runtime_reason(&props).as_deref(),
            Some("insecure runtime bypass enabled")
        );
    }

    #[tokio::test]
    async fn describe_and_list_ports_fetch_describe_endpoint() {
        let (base, requests) = http_responses(vec![
            r#"[{"addr":"node","properties":[["client-port","7777"],["cluster-port","7779"]]}]"#,
            r#"[{"addr":"node","properties":[["client-port","7777"],["name","node-a"]]}]"#,
        ])
        .await;
        let mut cfg = cfg(base);
        let client = reqwest::Client::new();

        run(
            NodeCommand::Describe {
                target: "127.0.0.1:7779".into(),
            },
            &mut cfg,
            &client,
        )
        .await
        .unwrap();
        run(
            NodeCommand::List {
                what: NodeListWhat::Ports,
                target: "127.0.0.1:7779".into(),
            },
            &mut cfg,
            &client,
        )
        .await
        .unwrap();

        let requests = requests.await.unwrap();
        assert!(requests[0].starts_with("GET /api/nodes/127.0.0.1%3A7779/describe HTTP/1.1"));
        assert!(requests[1].starts_with("GET /api/nodes/127.0.0.1%3A7779/describe HTTP/1.1"));
    }

    #[tokio::test]
    async fn get_and_remote_set_property_use_encoded_property_endpoint() {
        let (base, requests) = http_responses(vec![
            r#"{"value":"Active"}"#,
            r#"[{"addr":"node","ok":true}]"#,
        ])
        .await;
        let mut cfg = cfg(base);
        let client = reqwest::Client::new();

        run(
            NodeCommand::Get {
                property: "runtime mode".into(),
                target: "node-1:7779".into(),
            },
            &mut cfg,
            &client,
        )
        .await
        .unwrap();
        run(
            NodeCommand::Set {
                property: "active".into(),
                target: "node-1:7779".into(),
                value: "false".into(),
            },
            &mut cfg,
            &client,
        )
        .await
        .unwrap();

        let requests = requests.await.unwrap();
        assert!(requests[0]
            .starts_with("GET /api/nodes/node-1%3A7779/property/runtime%20mode HTTP/1.1"));
        assert!(requests[1].starts_with("POST /api/nodes/node-1%3A7779/property/active HTTP/1.1"));
        assert!(requests[1].contains(r#""value":"false""#));
    }

    #[tokio::test]
    async fn backup_restore_and_status_hit_expected_node_endpoints() {
        let (base, requests) = http_responses(vec![
            r#"[{"addr":"node","ok":true,"path":"backup.json"}]"#,
            r#"[{"addr":"node","ok":true,"path":"snapshot.json","entries":4,"duration_ms":12}]"#,
            r#"[{"addr":"node","reachable":false}]"#,
        ])
        .await;
        let mut cfg = cfg(base);
        let client = reqwest::Client::new();

        run(
            NodeCommand::Backup {
                target: "local".into(),
            },
            &mut cfg,
            &client,
        )
        .await
        .unwrap();
        run(
            NodeCommand::RestoreSnapshot {
                target: "local".into(),
            },
            &mut cfg,
            &client,
        )
        .await
        .unwrap();
        run(
            NodeCommand::Status {
                target: "local".into(),
            },
            &mut cfg,
            &client,
        )
        .await
        .unwrap();

        let requests = requests.await.unwrap();
        assert!(requests[0].starts_with("POST /api/nodes/local/backup HTTP/1.1"));
        assert!(requests[1].starts_with("POST /api/nodes/local/restore-snapshot HTTP/1.1"));
        assert!(requests[2].starts_with("GET /api/nodes/local/status HTTP/1.1"));
    }

    #[tokio::test]
    async fn status_renders_reachable_node_observability_fields() {
        let status = r#"[{
            "addr":"node",
            "reachable":true,
            "node_name":"node-a",
            "committed_index":12,
            "memory_used_bytes":1024,
            "memory_max_bytes":4096,
            "heartbeat_ms":25,
            "uptime_secs":3723,
            "backup_dir_bytes":2048,
            "snapshot_last_load_path":"snapshot.pb",
            "snapshot_last_load_duration_ms":7,
            "snapshot_last_load_entries":3,
            "snapshot_last_load_age_secs":9,
            "snapshot_restore_attempt_total":4,
            "snapshot_restore_success_total":3,
            "snapshot_restore_failure_total":1,
            "snapshot_restore_not_found_total":0,
            "snapshot_restore_policy_block_total":0,
            "persistence_enabled":true,
            "persistence_backup_enabled":true,
            "persistence_export_enabled":false,
            "persistence_import_enabled":true,
            "tenancy_enabled":true,
            "tenancy_default_namespace":"tenant",
            "tenancy_max_keys_per_namespace":100,
            "rate_limit_enabled":true,
            "rate_limited_requests_total":2,
            "hot_key_enabled":true,
            "hot_key_coalesced_hits_total":11,
            "hot_key_fallback_exec_total":1,
            "hot_key_wait_timeout_total":2,
            "hot_key_stale_served_total":3,
            "hot_key_inflight_keys":4,
            "hot_key_stale_cache_entries":5,
            "read_repair_enabled":true,
            "read_repair_trigger_total":6,
            "read_repair_success_total":5,
            "read_repair_throttled_total":1,
            "namespace_quota_reject_total":8,
            "namespace_quota_reject_rate_per_min":2,
            "namespace_quota_reject_trend":"steady",
            "namespace_quota_top_usage":[{"namespace":"tenant","key_count":9,"quota_limit":10,"usage_pct":90}],
            "anti_entropy_runs_total":13,
            "anti_entropy_repair_trigger_total":2,
            "anti_entropy_repair_throttled_total":1,
            "anti_entropy_last_detected_lag":7,
            "anti_entropy_key_checks_total":200,
            "anti_entropy_key_mismatch_total":3,
            "anti_entropy_full_reconcile_runs_total":4,
            "anti_entropy_full_reconcile_key_checks_total":300,
            "anti_entropy_full_reconcile_mismatch_total":5,
            "mixed_version_probe_runs_total":6,
            "mixed_version_peers_detected_total":1,
            "mixed_version_probe_errors_total":0,
            "mixed_version_last_detected_peer_count":1,
            "circuit_breaker_enabled":true,
            "circuit_breaker_state":"closed",
            "circuit_breaker_open_total":0,
            "circuit_breaker_reject_total":0,
            "client_requests_total":44,
            "client_requests_tcp_total":10,
            "client_requests_http_total":20,
            "client_requests_internal_total":14,
            "client_request_latency_le_1ms_total":1,
            "client_request_latency_le_5ms_total":2,
            "client_request_latency_le_20ms_total":3,
            "client_request_latency_le_100ms_total":4,
            "client_request_latency_le_500ms_total":5,
            "client_request_latency_gt_500ms_total":6,
            "client_latency_p50_estimate_ms":5,
            "client_latency_p90_estimate_ms":20,
            "client_latency_p95_estimate_ms":100,
            "client_latency_p99_estimate_ms":500,
            "client_errors_tcp_total":1,
            "client_errors_http_total":2,
            "client_errors_internal_total":3,
            "client_error_total":6,
            "client_error_auth_total":1,
            "client_error_throttle_total":1,
            "client_error_availability_total":1,
            "client_error_validation_total":1,
            "client_error_internal_total":1,
            "client_error_other_total":1
        }]"#;
        let (base, requests) = http_responses(vec![status]).await;
        let mut cfg = cfg(base);
        let client = reqwest::Client::new();

        run(
            NodeCommand::Status {
                target: "node-1:7779".into(),
            },
            &mut cfg,
            &client,
        )
        .await
        .unwrap();

        let requests = requests.await.unwrap();
        assert!(requests[0].starts_with("GET /api/nodes/node-1%3A7779/status HTTP/1.1"));
    }

    #[tokio::test]
    async fn doctor_fetches_status_describe_and_cluster_and_succeeds_when_healthy() {
        let (base, requests) = http_responses(vec![
            r#"[{"addr":"node","reachable":true,"status":"Active","heartbeat_ms":12,"namespace_quota_reject_rate_per_min":0,"namespace_quota_reject_trend":"steady","namespace_quota_top_usage":[]}]"#,
            r#"[{"addr":"node","properties":[["tcp-production-safe","true"],["insecure-runtime-enabled","false"]]}]"#,
            r#"{"total":1,"active":1,"syncing":0,"inactive":0,"offline":0,"nodes":[]}"#,
        ])
        .await;
        let mut cfg = cfg(base);
        let client = reqwest::Client::new();

        run(
            NodeCommand::Doctor {
                target: "local".into(),
            },
            &mut cfg,
            &client,
        )
        .await
        .unwrap();

        let requests = requests.await.unwrap();
        assert!(requests[0].starts_with("GET /api/nodes/local/status HTTP/1.1"));
        assert!(requests[1].starts_with("GET /api/nodes/local/describe HTTP/1.1"));
        assert!(requests[2].starts_with("GET /api/cluster HTTP/1.1"));
    }

    #[tokio::test]
    async fn doctor_reports_critical_for_unreachable_and_insecure_nodes() {
        let (base, requests) = http_responses(vec![
            r#"[
                {"addr":"node-a","reachable":false,"status":"Offline","heartbeat_ms":0,"namespace_quota_reject_rate_per_min":0,"namespace_quota_reject_trend":"steady","namespace_quota_top_usage":[]},
                {"addr":"node-b","reachable":true,"status":"Active","heartbeat_ms":3001,"namespace_quota_reject_rate_per_min":11,"namespace_quota_reject_trend":"surging","namespace_quota_top_usage":[{"usage_pct":95}]}
            ]"#,
            r#"[
                {"addr":"node-b","properties":[["tcp-production-safe","false"],["client-auth-enabled","false"],["tcp-client-bind-loopback-only","false"],["insecure-runtime-enabled","true"]]}
            ]"#,
            r#"{"total":2,"active":1,"syncing":0,"inactive":0,"offline":1,"nodes":[]}"#,
        ])
        .await;
        let mut cfg = cfg(base);
        let client = reqwest::Client::new();

        let err = run(
            NodeCommand::Doctor {
                target: "local".into(),
            },
            &mut cfg,
            &client,
        )
        .await
        .expect_err("critical doctor result should fail");

        assert!(err.to_string().contains("doctor found"));
        let requests = requests.await.unwrap();
        assert!(requests[0].starts_with("GET /api/nodes/local/status HTTP/1.1"));
        assert!(requests[1].starts_with("GET /api/nodes/local/describe HTTP/1.1"));
        assert!(requests[2].starts_with("GET /api/cluster HTTP/1.1"));
    }
}
