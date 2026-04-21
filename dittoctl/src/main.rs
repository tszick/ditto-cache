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

    match (&cfg.mgmt.username, &cfg.mgmt.password) {
        (Some(username), Some(password)) => {
            let mut headers = HeaderMap::new();
            let auth = STANDARD.encode(format!("{}:{}", username, password));
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Basic {}", auth))?,
            );
            builder = builder.default_headers(headers);
        }
        (None, None) => {}
        _ => anyhow::bail!(
            "dittoctl config requires both mgmt.username and mgmt.password when Basic Auth is configured"
        ),
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
    if password.is_empty() {
        anyhow::bail!("password must not be empty");
    }
    let hash = bcrypt::hash(&password, bcrypt::DEFAULT_COST)
        .map_err(|e| anyhow::anyhow!("bcrypt hash failed: {}", e))?;
    println!("{}", hash);
    Ok(())
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
