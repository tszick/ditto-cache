use crate::{client::mgmt_get, config::CtlConfig};
use anyhow::Result;
use clap::Subcommand;

#[derive(Debug, Subcommand)]
pub enum ClusterCommand {
    /// List cluster-level resources.
    List {
        /// "nodes" | "active-set"
        what: String,
    },
    /// Get a cluster property.
    Get {
        /// "status" | "primary" | "committed-index"
        what: String,
    },
}

pub async fn run(cmd: ClusterCommand, cfg: &CtlConfig, client: &reqwest::Client) -> Result<()> {
    let base = &cfg.mgmt.url;

    match cmd {
        ClusterCommand::List { what } => match what.as_str() {
            "nodes" | "active-set" => {
                let data = mgmt_get(client, &format!("{}/api/cluster", base)).await?;
                let nodes = data["nodes"].as_array().cloned().unwrap_or_default();
                println!(
                    "  {:<38} {:<12} {:<10} committed",
                    "node-id", "status", "primary"
                );
                println!("  {}", "─".repeat(76));
                for n in &nodes {
                    let status = n["status"].as_str().unwrap_or("?");
                    if what == "active-set" && status != "Active" {
                        continue;
                    }
                    println!(
                        "  {:<38} {:<12} {:<10} {}",
                        n["id"].as_str().unwrap_or("?"),
                        status,
                        if n["is_primary"].as_bool().unwrap_or(false) {
                            "yes"
                        } else {
                            "no"
                        },
                        n["last_applied"]
                            .as_u64()
                            .map(|v| v.to_string())
                            .as_deref()
                            .unwrap_or("?")
                    );
                }
                let total = data["total"].as_u64().unwrap_or(nodes.len() as u64);
                println!("\n  Total: {} node(s)", total);
            }
            other => eprintln!("Unknown list target '{}'. Use: nodes | active-set", other),
        },

        ClusterCommand::Get { what } => match what.as_str() {
            "status" => {
                let data = mgmt_get(client, &format!("{}/api/cluster", base)).await?;
                println!("  total:    {}", data["total"].as_u64().unwrap_or(0));
                println!("  active:   {}", data["active"].as_u64().unwrap_or(0));
                println!("  syncing:  {}", data["syncing"].as_u64().unwrap_or(0));
                println!("  inactive: {}", data["inactive"].as_u64().unwrap_or(0));
                println!("  offline:  {}", data["offline"].as_u64().unwrap_or(0));
            }
            "primary" => {
                let data = mgmt_get(client, &format!("{}/api/cluster/primary", base)).await?;
                match data["primary"].as_str() {
                    Some(id) => println!("  primary: {}", id),
                    None => println!("  (no primary elected)"),
                }
            }
            "committed-index" => {
                let data = mgmt_get(client, &format!("{}/api/nodes", base)).await?;
                let nodes = data["nodes"].as_array().cloned().unwrap_or_default();
                if let Some(first) = nodes.first() {
                    println!(
                        "  committed-index: {}",
                        first["committed_index"].as_u64().unwrap_or(0)
                    );
                }
            }
            other => eprintln!(
                "Unknown get target '{}'. Use: status | primary | committed-index",
                other
            ),
        },
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    async fn http_response(body: &'static str) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 2048];
            let n = stream.read(&mut buf).await.unwrap();
            let request = String::from_utf8_lossy(&buf[..n]).to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            request
        });

        (format!("http://{addr}"), handle)
    }

    fn cfg(base: String) -> CtlConfig {
        let mut cfg = CtlConfig::default();
        cfg.mgmt.url = base;
        cfg
    }

    #[tokio::test]
    async fn get_status_fetches_cluster_summary() {
        let (base, request) = http_response(
            r#"{"total":2,"active":1,"syncing":1,"inactive":0,"offline":0,"nodes":[]}"#,
        )
        .await;
        let cfg = cfg(base);
        let client = reqwest::Client::new();

        run(
            ClusterCommand::Get {
                what: "status".into(),
            },
            &cfg,
            &client,
        )
        .await
        .unwrap();

        assert!(request
            .await
            .unwrap()
            .starts_with("GET /api/cluster HTTP/1.1"));
    }

    #[tokio::test]
    async fn get_primary_fetches_primary_endpoint() {
        let (base, request) = http_response(r#"{"primary":"node-1"}"#).await;
        let cfg = cfg(base);
        let client = reqwest::Client::new();

        run(
            ClusterCommand::Get {
                what: "primary".into(),
            },
            &cfg,
            &client,
        )
        .await
        .unwrap();

        assert!(request
            .await
            .unwrap()
            .starts_with("GET /api/cluster/primary HTTP/1.1"));
    }

    #[tokio::test]
    async fn list_active_set_fetches_cluster_nodes() {
        let (base, request) = http_response(
            r#"{"total":2,"nodes":[{"id":"a","status":"Active","is_primary":true,"last_applied":3},{"id":"b","status":"Inactive","is_primary":false,"last_applied":2}]}"#,
        )
        .await;
        let cfg = cfg(base);
        let client = reqwest::Client::new();

        run(
            ClusterCommand::List {
                what: "active-set".into(),
            },
            &cfg,
            &client,
        )
        .await
        .unwrap();

        assert!(request
            .await
            .unwrap()
            .starts_with("GET /api/cluster HTTP/1.1"));
    }
}
