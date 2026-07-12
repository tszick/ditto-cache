//! dittoctl — Ditto cluster admin CLI.
//!
//! A thin HTTP client that forwards every command to `ditto-mgmt` (default
//! `http://localhost:7781`).  Does **not** communicate with nodes directly.
//!
//! # Quick start
//! ```
//! dittoctl node status local
//! dittoctl cache list stats all
//! dittoctl cluster list nodes
//! ```

mod bootstrap;
mod client;
mod commands;
mod config;
mod password;

use anyhow::Result;
use clap::{Parser, Subcommand};
use commands::{cache::CacheCommand, cluster::ClusterCommand, node::NodeCommand};

/// dittoctl – Ditto cluster admin CLI (talks to ditto-mgmt)
#[derive(Debug, Parser)]
#[command(name = "dittoctl", version, about)]
struct Cli {
    /// Override config file path.
    #[arg(long, global = true)]
    config: Option<std::path::PathBuf>,

    #[command(subcommand)]
    resource: Resource,
}

#[derive(Debug, Subcommand)]
enum Resource {
    /// Node-level commands.
    Node {
        #[command(subcommand)]
        cmd: NodeCommand,
    },
    /// Cache-level commands.
    Cache {
        #[command(subcommand)]
        cmd: CacheCommand,
    },
    /// Cluster-level commands.
    Cluster {
        #[command(subcommand)]
        cmd: ClusterCommand,
    },
    /// Generate a bcrypt password hash suitable for ditto config files.
    ///
    /// Example:
    ///   dittoctl hash-password
    ///   Enter password: ****
    ///   $2b$12$abc123...
    ///
    /// Paste the output into `[admin] password_hash` (ditto-mgmt) or
    /// `[http_auth] password_hash` (dittod).
    #[command(name = "hash-password")]
    HashPassword,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    if let Resource::HashPassword = cli.resource {
        return password::cmd_hash_password();
    }

    let mut cfg = bootstrap::load_ctl_config(cli.config.as_deref())?;
    cfg.mgmt.resolve_credentials()?;
    let http_client = bootstrap::build_http_client(&cfg)?;

    match cli.resource {
        Resource::Node { cmd } => commands::node::run(cmd, &mut cfg, &http_client).await?,
        Resource::Cache { cmd } => commands::cache::run(cmd, &cfg, &http_client).await?,
        Resource::Cluster { cmd } => commands::cluster::run(cmd, &cfg, &http_client).await?,
        Resource::HashPassword => unreachable!(),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use crate::{bootstrap, config::CtlConfig, password};
    use reqwest::header::AUTHORIZATION;

    #[test]
    fn cli_parses_hash_password_without_config() {
        let cli = Cli::try_parse_from(["dittoctl", "hash-password"]).expect("parse cli");
        assert!(cli.config.is_none());
        assert!(matches!(cli.resource, Resource::HashPassword));
    }

    #[test]
    fn cli_parses_global_config_before_resource() {
        let cli = Cli::try_parse_from([
            "dittoctl",
            "--config",
            "custom.toml",
            "cluster",
            "get",
            "status",
        ])
        .expect("parse cli");
        assert_eq!(cli.config.unwrap(), std::path::PathBuf::from("custom.toml"));
        assert!(matches!(cli.resource, Resource::Cluster { .. }));
    }

    #[test]
    fn ctl_config_default_uses_local_management_endpoint() {
        let cfg = CtlConfig::default();
        assert_eq!(cfg.mgmt.url, "http://localhost:7781");
        assert_eq!(cfg.mgmt.timeout_ms, 3000);
        assert!(cfg.mgmt.username.is_none());
        assert!(cfg.mgmt.password.is_none());
        assert!(cfg.mgmt.bearer_token.is_none());
        assert!(!cfg.mgmt.insecure_skip_verify);
        assert_eq!(cfg.output.format, "binary");
    }

    #[test]
    fn auth_headers_encode_basic_or_bearer_and_reject_invalid_config() {
        let mut cfg = CtlConfig::default();
        assert!(bootstrap::auth_headers(&cfg).unwrap().is_none());

        cfg.mgmt.username = Some("admin".into());
        let password = format!("test-password-{}", std::process::id());
        cfg.mgmt.password = Some(password.clone());
        let headers = bootstrap::auth_headers(&cfg).unwrap().unwrap();
        let expected = STANDARD.encode(format!("admin:{password}"));
        assert_eq!(
            headers.get(AUTHORIZATION).unwrap(),
            format!("Basic {expected}").as_str()
        );

        cfg.mgmt.password = None;
        let err = bootstrap::auth_headers(&cfg).unwrap_err();
        assert!(err.to_string().contains("requires both"));

        let mut cfg = CtlConfig::default();
        cfg.mgmt.bearer_token = Some("token-123".into());
        let headers = bootstrap::auth_headers(&cfg).unwrap().unwrap();
        assert_eq!(headers.get(AUTHORIZATION).unwrap(), "Bearer token-123");

        cfg.mgmt.username = Some("admin".into());
        cfg.mgmt.password = Some("pass".into());
        let err = bootstrap::auth_headers(&cfg).unwrap_err();
        assert!(err.to_string().contains("must not set both"));
    }

    #[test]
    fn load_ctl_config_uses_explicit_config_path() {
        let dir =
            std::env::temp_dir().join(format!("dittoctl-main-config-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("custom.toml");
        std::fs::write(
            &path,
            r#"
[mgmt]
url = "https://localhost:9443"
timeout_ms = 1234
username = "ditto"
password = "secret"
insecure_skip_verify = true

[output]
format = "json"
"#,
        )
        .unwrap();

        let cfg = bootstrap::load_ctl_config(Some(&path)).unwrap();

        assert_eq!(cfg.mgmt.url, "https://localhost:9443");
        assert_eq!(cfg.mgmt.timeout_ms, 1234);
        assert_eq!(cfg.mgmt.username.as_deref(), Some("ditto"));
        assert_eq!(cfg.mgmt.password.as_deref(), Some("secret"));
        assert!(cfg.mgmt.insecure_skip_verify);
        assert_eq!(cfg.output.format, "json");

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn hash_password_value_rejects_empty_and_produces_bcrypt_hash() {
        let empty_password = String::new();
        let err = password::hash_password_value(&empty_password).unwrap_err();
        assert!(err.to_string().contains("must not be empty"));

        let password = format!("test-password-{}", std::process::id());
        let hash = password::hash_password_value(&password).unwrap();
        assert!(bcrypt::verify(&password, &hash).unwrap());
        let wrong_password = format!("wrong-{password}");
        assert!(!bcrypt::verify(&wrong_password, &hash).unwrap());
    }
}
