use crate::{
    client::{enc, mgmt_delete, mgmt_get, mgmt_post, mgmt_put},
    config::CtlConfig,
};
use anyhow::Result;
use clap::Subcommand;

#[derive(Debug, Subcommand)]
pub enum CacheCommand {
    /// List keys or stats.
    List {
        /// "keys" | "stats"
        what: String,
        target: String,
        #[arg(long)]
        pattern: Option<String>,
        #[arg(long)]
        namespace: Option<String>,
    },
    /// Get the value or TTL of a key.
    Get {
        /// "key" | "ttl"
        what: String,
        target: String,
        key: String,
        #[arg(long)]
        namespace: Option<String>,
    },
    /// Set a key-value pair on a node.
    Set {
        target: String,
        key: String,
        value: String,
        #[arg(long)]
        ttl: Option<u64>,
        #[arg(long)]
        namespace: Option<String>,
    },
    /// Delete a key from a node.
    Delete {
        target: String,
        key: String,
        #[arg(long)]
        namespace: Option<String>,
    },
    /// Flush all keys from a node (requires confirmation for "all").
    Flush { target: String },
    /// Set the compressed flag on a key (true / false).
    SetCompressed {
        target: String,
        key: String,
        value: String,
        #[arg(long)]
        namespace: Option<String>,
    },
    /// Set TTL for all keys matching a glob pattern (e.g. "user:1234:*").
    SetTtl {
        target: String,
        /// Glob pattern — supports `*` wildcard (e.g. "user:*", "session:*:data").
        pattern: String,
        /// TTL in seconds. Omit (or pass 0) to remove TTL entirely.
        #[arg(long)]
        ttl: Option<u64>,
        #[arg(long)]
        namespace: Option<String>,
    },
}

fn append_namespace_query(url: &mut String, namespace: Option<String>) {
    if let Some(ns) = namespace
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
    {
        let sep = if url.contains('?') { '&' } else { '?' };
        url.push(sep);
        url.push_str("namespace=");
        url.push_str(&enc(&ns));
    }
}

pub async fn run(cmd: CacheCommand, cfg: &CtlConfig, client: &reqwest::Client) -> Result<()> {
    let base = &cfg.mgmt.url;

    match cmd {
        CacheCommand::List {
            what,
            target,
            pattern,
            namespace,
        } => match what.as_str() {
            "stats" => {
                let url = format!("{}/api/cache/{}/stats", base, enc(&target));
                let data = mgmt_get(client, &url).await?;
                let results = data.as_array().cloned().unwrap_or_default();
                for r in results {
                    println!("\n  Node: {}", r["addr"].as_str().unwrap_or("?"));
                    if let Some(err) = r["error"].as_str() {
                        println!("  Error: {}", err);
                        continue;
                    }
                    println!("  {:<22} {}", "keys", r["key_count"].as_u64().unwrap_or(0));
                    println!(
                        "  {:<22} {}/{} bytes",
                        "memory",
                        r["memory_used_bytes"].as_u64().unwrap_or(0),
                        r["memory_max_bytes"].as_u64().unwrap_or(0)
                    );
                    println!(
                        "  {:<22} {}",
                        "evictions",
                        r["evictions"].as_u64().unwrap_or(0)
                    );
                    println!("  {:<22} {}", "hits", r["hit_count"].as_u64().unwrap_or(0));
                    println!(
                        "  {:<22} {}",
                        "misses",
                        r["miss_count"].as_u64().unwrap_or(0)
                    );
                    println!(
                        "  {:<22} {}%",
                        "hit-rate",
                        r["hit_rate_pct"].as_u64().unwrap_or(0)
                    );
                }
            }
            "keys" => {
                let mut url = format!("{}/api/cache/{}/keys", base, enc(&target));
                if let Some(pat) = pattern {
                    url.push_str(&format!("?pattern={}", enc(&pat)));
                }
                append_namespace_query(&mut url, namespace);
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
        },

        CacheCommand::Get {
            what,
            target,
            key,
            namespace,
        } => {
            let mut url = format!("{}/api/cache/{}/keys/{}", base, enc(&target), enc(&key));
            append_namespace_query(&mut url, namespace);
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

        CacheCommand::Set {
            target,
            key,
            value,
            ttl,
            namespace,
        } => {
            let mut url = format!("{}/api/cache/{}/keys/{}", base, enc(&target), enc(&key));
            append_namespace_query(&mut url, namespace);
            let body = serde_json::json!({ "value": value, "ttl_secs": ttl });
            match mgmt_put(client, &url, body).await {
                Ok(_) => println!("  set ok"),
                Err(e) => eprintln!("  Error: {}", e),
            }
        }

        CacheCommand::Delete {
            target,
            key,
            namespace,
        } => {
            let mut url = format!("{}/api/cache/{}/keys/{}", base, enc(&target), enc(&key));
            append_namespace_query(&mut url, namespace);
            match mgmt_delete(client, &url).await {
                Ok(_) => println!("  deleted"),
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

            let url = format!("{}/api/cache/{}/flush", base, enc(&target));
            let data = mgmt_post(client, &url, serde_json::json!({})).await?;
            let results = data.as_array().cloned().unwrap_or_default();
            for r in results {
                let addr = r["addr"].as_str().unwrap_or("?");
                if r["ok"].as_bool().unwrap_or(false) {
                    println!("  {} flushed.", addr);
                } else {
                    eprintln!(
                        "  Error from {}: {}",
                        addr,
                        r["error"].as_str().unwrap_or("unknown")
                    );
                }
            }
        }

        CacheCommand::SetTtl {
            target,
            pattern,
            ttl,
            namespace,
        } => {
            let ttl_secs = ttl.filter(|&s| s > 0);
            let mut url = format!("{}/api/cache/{}/ttl", base, enc(&target));
            append_namespace_query(&mut url, namespace);
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

        CacheCommand::SetCompressed {
            target,
            key,
            value,
            namespace,
        } => {
            let compressed = value == "true";
            let mut url = format!(
                "{}/api/cache/{}/keys/{}/compressed",
                base,
                enc(&target),
                enc(&key)
            );
            append_namespace_query(&mut url, namespace);
            let data = mgmt_post(
                client,
                &url,
                serde_json::json!({ "compressed": compressed }),
            )
            .await?;
            let results = data.as_array().cloned().unwrap_or_default();
            for r in results {
                let addr = r["addr"].as_str().unwrap_or("?");
                if r["ok"].as_bool().unwrap_or(false) {
                    println!("  {} ← compressed={}", key, value);
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
            let request = read_request(&mut stream).await;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            request
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

    #[tokio::test]
    async fn list_keys_sends_encoded_pattern_and_namespace_query() {
        let (base, request) = http_response(r#"[{"addr":"node","keys":["alpha"]}]"#).await;
        let cfg = cfg(base);
        let client = reqwest::Client::new();

        run(
            CacheCommand::List {
                what: "keys".into(),
                target: "127.0.0.1:7779".into(),
                pattern: Some("user:*".into()),
                namespace: Some("tenant a".into()),
            },
            &cfg,
            &client,
        )
        .await
        .unwrap();

        let request = request.await.unwrap();
        assert!(request.starts_with(
            "GET /api/cache/127.0.0.1%3A7779/keys?pattern=user%3A%2A&namespace=tenant%20a HTTP/1.1"
        ));
    }

    #[tokio::test]
    async fn set_key_sends_value_ttl_and_namespace() {
        let (base, request) = http_response(r#"{"ok":true}"#).await;
        let cfg = cfg(base);
        let client = reqwest::Client::new();

        run(
            CacheCommand::Set {
                target: "local".into(),
                key: "alpha:1".into(),
                value: "value".into(),
                ttl: Some(60),
                namespace: Some("tenant".into()),
            },
            &cfg,
            &client,
        )
        .await
        .unwrap();

        let request = request.await.unwrap();
        assert!(
            request.starts_with("PUT /api/cache/local/keys/alpha%3A1?namespace=tenant HTTP/1.1")
        );
        assert!(request.contains(r#""value":"value""#));
        assert!(request.contains(r#""ttl_secs":60"#));
    }

    #[tokio::test]
    async fn set_compressed_posts_boolean_payload() {
        let (base, request) = http_response(r#"[{"addr":"node","ok":true}]"#).await;
        let cfg = cfg(base);
        let client = reqwest::Client::new();

        run(
            CacheCommand::SetCompressed {
                target: "local".into(),
                key: "alpha".into(),
                value: "false".into(),
                namespace: None,
            },
            &cfg,
            &client,
        )
        .await
        .unwrap();

        let request = request.await.unwrap();
        assert!(request.starts_with("POST /api/cache/local/keys/alpha/compressed HTTP/1.1"));
        assert!(request.contains(r#""compressed":false"#));
    }
}
