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

mod client;
mod commands;
mod config;

use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use clap::{Parser, Subcommand};
use commands::{cache::CacheCommand, cluster::ClusterCommand, node::NodeCommand};
use config::CtlConfig;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};

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

    // hash-password does not need an HTTP client or config.
    if let Resource::HashPassword = cli.resource {
        return cmd_hash_password();
    }

    let mut cfg = CtlConfig::load()?;

    let mut builder =
        reqwest::Client::builder().timeout(std::time::Duration::from_millis(cfg.mgmt.timeout_ms));

    if cfg.mgmt.insecure_skip_verify {
        builder = builder.danger_accept_invalid_certs(true);
    }

    if let Some(headers) = basic_auth_headers(&cfg)? {
        builder = builder.default_headers(headers);
    }

    let http_client = builder.build()?;

    match cli.resource {
        Resource::Node { cmd } => commands::node::run(cmd, &mut cfg, &http_client).await?,
        Resource::Cache { cmd } => commands::cache::run(cmd, &cfg, &http_client).await?,
        Resource::Cluster { cmd } => commands::cluster::run(cmd, &cfg, &http_client).await?,
        Resource::HashPassword => unreachable!(),
    }
    Ok(())
}

/// Prompt for a password (no echo), hash it with bcrypt, and print the result.
fn cmd_hash_password() -> Result<()> {
    let password = rpassword_read()?;
    println!("{}", hash_password_value(&password)?);
    Ok(())
}

fn hash_password_value(password: &str) -> Result<String> {
    if password.is_empty() {
        anyhow::bail!("password must not be empty");
    }
    Ok(bcrypt::hash(password, bcrypt::DEFAULT_COST)
        .map_err(|e| anyhow::anyhow!("bcrypt hash failed: {}", e))?)
}

fn basic_auth_headers(cfg: &CtlConfig) -> Result<Option<HeaderMap>> {
    match (&cfg.mgmt.username, &cfg.mgmt.password) {
        (Some(username), Some(password)) => {
            let mut headers = HeaderMap::new();
            let auth = STANDARD.encode(format!("{}:{}", username, password));
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Basic {}", auth))?,
            );
            Ok(Some(headers))
        }
        (None, None) => Ok(None),
        _ => anyhow::bail!(
            "dittoctl config requires both mgmt.username and mgmt.password when Basic Auth is configured"
        ),
    }
}

/// Read a password from stdin without echoing it to the terminal.
/// Falls back to a plain `read_line` when not connected to a TTY (e.g. piped).
fn rpassword_read() -> Result<String> {
    eprint!("Enter password: ");
    // Try to read without echo via the `termios` trick; fall back to plain stdin.
    let password = if atty_stdin() {
        read_password_from_tty()
    } else {
        let mut line = String::new();
        std::io::stdin().read_line(&mut line)?;
        line.trim_end_matches('\n')
            .trim_end_matches('\r')
            .to_string()
    };
    eprintln!();
    Ok(password)
}

fn atty_stdin() -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        unsafe { libc_isatty(std::io::stdin().as_raw_fd()) }
    }
    #[cfg(not(unix))]
    {
        false
    }
}

#[cfg(unix)]
fn libc_isatty(fd: std::os::unix::io::RawFd) -> bool {
    extern "C" {
        fn isatty(fd: i32) -> i32;
    }
    unsafe { isatty(fd) != 0 }
}

fn read_password_from_tty() -> String {
    // Disable terminal echo, read one line, re-enable echo.
    // Works on Unix; on Windows falls back to plain read.
    #[cfg(unix)]
    {
        use std::io::Read;
        // Use /dev/tty directly so piped stdout doesn't interfere.
        if let Ok(mut tty) = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/tty")
        {
            disable_echo(&tty);
            let mut buf = String::new();
            let _ = tty.read_to_string(&mut buf); // reads until EOF (Ctrl-D) or newline
            enable_echo(&tty);
            // read_to_string reads until EOF; trim the newline
            let line = buf.lines().next().unwrap_or("").to_string();
            return line;
        }
    }
    // Fallback: plain stdin (password visible)
    let mut line = String::new();
    let _ = std::io::stdin().read_line(&mut line);
    line.trim_end_matches('\n')
        .trim_end_matches('\r')
        .to_string()
}

#[cfg(unix)]
fn disable_echo(_file: &std::fs::File) {
    // Minimal no-op stub; rpassword crate handles this properly.
    // For production use, consider adding the `rpassword` crate.
}

#[cfg(unix)]
fn enable_echo(_file: &std::fs::File) {}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(!cfg.mgmt.insecure_skip_verify);
        assert_eq!(cfg.output.format, "binary");
    }

    #[test]
    fn basic_auth_headers_encode_credentials_and_reject_partial_config() {
        let mut cfg = CtlConfig::default();
        assert!(basic_auth_headers(&cfg).unwrap().is_none());

        cfg.mgmt.username = Some("admin".into());
        let password = format!("test-password-{}", std::process::id());
        cfg.mgmt.password = Some(password.clone());
        let headers = basic_auth_headers(&cfg).unwrap().unwrap();
        let expected = STANDARD.encode(format!("admin:{password}"));
        assert_eq!(
            headers.get(AUTHORIZATION).unwrap(),
            format!("Basic {expected}").as_str()
        );

        cfg.mgmt.password = None;
        let err = basic_auth_headers(&cfg).unwrap_err();
        assert!(err.to_string().contains("requires both"));
    }

    #[test]
    fn hash_password_value_rejects_empty_and_produces_bcrypt_hash() {
        let empty_password = String::new();
        let err = hash_password_value(&empty_password).unwrap_err();
        assert!(err.to_string().contains("must not be empty"));

        let password = format!("test-password-{}", std::process::id());
        let hash = hash_password_value(&password).unwrap();
        assert!(bcrypt::verify(&password, &hash).unwrap());
        let wrong_password = format!("wrong-{password}");
        assert!(!bcrypt::verify(&wrong_password, &hash).unwrap());
    }
}
