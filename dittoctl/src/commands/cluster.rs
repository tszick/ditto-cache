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
                    "  {:<38} {:<12} {:<10} {}",
                    "node-id", "status", "primary", "committed"
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
                        if n["is_primary"].as_bool().unwrap_or(false) { "yes" } else { "no" },
                        n["last_applied"].as_u64().map(|v| v.to_string()).as_deref().unwrap_or("?")
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
                    None     => println!("  (no primary elected)"),
                }
            }
            "committed-index" => {
                let data = mgmt_get(client, &format!("{}/api/nodes", base)).await?;
                let nodes = data["nodes"].as_array().cloned().unwrap_or_default();
                if let Some(first) = nodes.first() {
                    println!("  committed-index: {}", first["committed_index"].as_u64().unwrap_or(0));
                }
            }
            other => eprintln!("Unknown get target '{}'. Use: status | primary | committed-index", other),
        },
    }
    Ok(())
}
