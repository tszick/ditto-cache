use crate::{client::{enc, mgmt_delete, mgmt_get, mgmt_post, mgmt_put}, config::CtlConfig};
use anyhow::Result;
use clap::Subcommand;

#[derive(Debug, Subcommand)]
pub enum CacheCommand {
    /// List keys or stats.
    List {
        /// "keys" | "stats"
        what:    String,
        target:  String,
        #[arg(long)]
        pattern: Option<String>,
    },
    /// Get the value or TTL of a key.
    Get {
        /// "key" | "ttl"
        what:   String,
        target: String,
        key:    String,
    },
    /// Set a key-value pair on a node.
    Set {
        target: String,
        key:    String,
        value:  String,
        #[arg(long)]
        ttl:    Option<u64>,
    },
    /// Delete a key from a node.
    Delete {
        target: String,
        key:    String,
    },
    /// Flush all keys from a node (requires confirmation for "all").
    Flush {
        target: String,
    },
    /// Set the compressed flag on a key (true / false).
    SetCompressed {
        target: String,
        key:    String,
        value:  String,
    },
    /// Set TTL for all keys matching a glob pattern (e.g. "user:1234:*").
    SetTtl {
        target:  String,
        /// Glob pattern — supports `*` wildcard (e.g. "user:*", "session:*:data").
        pattern: String,
        /// TTL in seconds. Omit (or pass 0) to remove TTL entirely.
        #[arg(long)]
        ttl:     Option<u64>,
    },
}

pub async fn run(cmd: CacheCommand, cfg: &CtlConfig, client: &reqwest::Client) -> Result<()> {
    let base = &cfg.mgmt.url;

    match cmd {
        CacheCommand::List { what, target, pattern } => {
            match what.as_str() {
                "stats" => {
                    let url  = format!("{}/api/cache/{}/stats", base, enc(&target));
                    let data = mgmt_get(client, &url).await?;
                    let results = data.as_array().cloned().unwrap_or_default();
                    for r in results {
                        println!("\n  Node: {}", r["addr"].as_str().unwrap_or("?"));
                        if let Some(err) = r["error"].as_str() {
                            println!("  Error: {}", err);
                            continue;
                        }
                        println!("  {:<22} {}", "keys",      r["key_count"].as_u64().unwrap_or(0));
                        println!("  {:<22} {}/{} bytes", "memory",
                            r["memory_used_bytes"].as_u64().unwrap_or(0),
                            r["memory_max_bytes"].as_u64().unwrap_or(0));
                        println!("  {:<22} {}", "evictions",  r["evictions"].as_u64().unwrap_or(0));
                        println!("  {:<22} {}", "hits",       r["hit_count"].as_u64().unwrap_or(0));
                        println!("  {:<22} {}", "misses",     r["miss_count"].as_u64().unwrap_or(0));
                        println!("  {:<22} {}%", "hit-rate",  r["hit_rate_pct"].as_u64().unwrap_or(0));
                    }
                }
                "keys" => {
                    let mut url = format!("{}/api/cache/{}/keys", base, enc(&target));
                    if let Some(pat) = pattern {
                        url.push_str(&format!("?pattern={}", enc(&pat)));
                    }
                    let data = mgmt_get(client, &url).await?;
                    let results = data.as_array().cloned().unwrap_or_default();
                    for r in results {
                        println!("\n  Node: {}", r["addr"].as_str().unwrap_or("?"));
                        if let Some(keys) = r["keys"].as_array() {
                            if keys.is_empty() {
                                println!("  (empty)");
                            } else {
                                for k in keys {
                                    println!("  {}", k.as_str().unwrap_or("?"));
                                }
                                println!("  ─── {} key(s)", keys.len());
                            }
                        } else if let Some(err) = r["error"].as_str() {
                            println!("  Error: {}", err);
                        }
                    }
                }
                other => eprintln!("Unknown list target '{}'. Use: keys | stats", other),
            }
        }

        CacheCommand::Get { what, target, key } => {
            let url  = format!("{}/api/cache/{}/keys/{}", base, enc(&target), enc(&key));
            let data = mgmt_get(client, &url).await;
            match what.as_str() {
                "key" => match data {
                    Ok(d) => println!("{}", d["value"].as_str().unwrap_or("(empty)")),
                    Err(_) => println!("  (not found)"),
                },
                "ttl" => {
                    // TTL info is available via the admin describe; for HTTP port we just show value
                    match data {
                        Ok(d) => println!("{}", d["value"].as_str().unwrap_or("(not found)")),
                        Err(_) => println!("  (not found)"),
                    }
                }
                other => eprintln!("  Unknown property '{}'. Use: key | ttl", other),
            }
        }

        CacheCommand::Set { target, key, value, ttl } => {
            let url  = format!("{}/api/cache/{}/keys/{}", base, enc(&target), enc(&key));
            let body = serde_json::json!({ "value": value, "ttl_secs": ttl });
            match mgmt_put(client, &url, body).await {
                Ok(_)  => println!("  set ok"),
                Err(e) => eprintln!("  Error: {}", e),
            }
        }

        CacheCommand::Delete { target, key } => {
            let url = format!("{}/api/cache/{}/keys/{}", base, enc(&target), enc(&key));
            match mgmt_delete(client, &url).await {
                Ok(_)  => println!("  deleted"),
                Err(e) => eprintln!("  Error: {}", e),
            }
        }

        CacheCommand::Flush { target } => {
            // Require confirmation when flushing all nodes.
            if target == "all" {
                print!("  ⚠ This will flush cache on ALL nodes. Type \"yes\" to confirm: ");
                std::io::Write::flush(&mut std::io::stdout())?;
                let mut input = String::new();
                std::io::stdin().read_line(&mut input)?;
                if input.trim() != "yes" {
                    println!("  Aborted.");
                    return Ok(());
                }
            }

            let url  = format!("{}/api/cache/{}/flush", base, enc(&target));
            let data = mgmt_post(client, &url, serde_json::json!({})).await?;
            let results = data.as_array().cloned().unwrap_or_default();
            for r in results {
                let addr = r["addr"].as_str().unwrap_or("?");
                if r["ok"].as_bool().unwrap_or(false) {
                    println!("  {} flushed.", addr);
                } else {
                    eprintln!("  Error from {}: {}", addr, r["error"].as_str().unwrap_or("unknown"));
                }
            }
        }

        CacheCommand::SetTtl { target, pattern, ttl } => {
            let ttl_secs = ttl.filter(|&s| s > 0);
            let url  = format!("{}/api/cache/{}/ttl", base, enc(&target));
            let body = serde_json::json!({ "pattern": pattern, "ttl_secs": ttl_secs });
            match mgmt_post(client, &url, body).await {
                Ok(data) => {
                    let results = data.as_array().cloned().unwrap_or_default();
                    for r in results {
                        let addr = r["addr"].as_str().unwrap_or("?");
                        if let Some(err) = r["error"].as_str() {
                            eprintln!("  Error from {}: {}", addr, err);
                        } else {
                            let n = r["updated"].as_u64().unwrap_or(0);
                            println!("  {} ← {} key(s) updated", addr, n);
                        }
                    }
                }
                Err(e) => eprintln!("  Error: {}", e),
            }
        }

        CacheCommand::SetCompressed { target, key, value } => {
            let compressed = value == "true";
            let url  = format!("{}/api/cache/{}/keys/{}/compressed", base, enc(&target), enc(&key));
            let data = mgmt_post(client, &url, serde_json::json!({ "compressed": compressed })).await?;
            let results = data.as_array().cloned().unwrap_or_default();
            for r in results {
                let addr = r["addr"].as_str().unwrap_or("?");
                if r["ok"].as_bool().unwrap_or(false) {
                    println!("  {} ← compressed={}", key, value);
                } else {
                    eprintln!("  Error from {}: {}", addr, r["error"].as_str().unwrap_or("unknown"));
                }
            }
        }
    }
    Ok(())
}
