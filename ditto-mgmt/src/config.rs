//! Configuration for `ditto-mgmt`.
//!
//! The config file lives at `~/.config/ditto/mgmt.toml`.
//! If the file does not exist, [`MgmtConfig::load`] returns built-in defaults:
//! bind `0.0.0.0:7781`, seed `127.0.0.1:7779`, TLS disabled.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{fs, path::PathBuf};

/// Top-level configuration for the management service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MgmtConfig {
    pub server:           ServerConfig,
    pub connection:       ConnectionConfig,
    #[serde(default)]
    pub tls:              TlsConfig,
    /// Optional HTTP Basic Auth for the management API (7781).
    #[serde(default)]
    pub admin:            AdminConfig,
    /// Credentials ditto-mgmt uses when proxying cache requests to dittod's
    /// HTTP REST port (7778), when auth is enabled there.
    #[serde(default)]
    pub http_client_auth: HttpClientAuthConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Bind address for the management HTTP server.
    pub bind: String,
    /// HTTP port for the management server.
    pub port: u16,
    /// Optional TLS certificate (PEM) for the management HTTPS server.
    /// When absent the server uses plain HTTP.
    #[serde(default)]
    pub tls_cert: Option<String>,
    /// Optional TLS private key (PEM) for the management HTTPS server.
    #[serde(default)]
    pub tls_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    /// Bootstrap seed addresses in "host:cluster_port" format.
    pub seeds:        Vec<String>,
    /// Cluster/admin TCP port (default 7779).
    pub cluster_port: u16,
    /// RPC timeout in milliseconds.
    pub timeout_ms:   u64,
    /// Disable TLS hostname verification for node HTTP proxying.
    /// Development-only escape hatch for environments where node certificates
    /// are valid but accessed by dynamic IP addresses.
    #[serde(default)]
    pub insecure_http_hostnames: bool,
}

/// TLS configuration for mTLS connections to node cluster ports.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TlsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub ca_cert: String,
    #[serde(default)]
    pub cert:    String,
    #[serde(default)]
    pub key:     String,
}

/// Optional HTTP Basic Auth for the management server (port 7781).
///
/// When `password_hash` is set, all `/api/*` and `/` endpoints require
/// `Authorization: Basic <base64(user:pass)>`.  Browser clients will see a
/// native login dialog; `dittoctl` and `curl` pass `-u user:pass`.
///
/// Generate a hash with: `dittoctl hash-password`
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AdminConfig {
    /// Username (default: `"admin"` when only hash is set).
    #[serde(default)]
    pub username:      Option<String>,
    /// bcrypt hash of the password.  When absent, auth is disabled.
    #[serde(default)]
    pub password_hash: Option<String>,
}

/// Credentials used by ditto-mgmt when forwarding cache GET/PUT/DELETE
/// requests to dittod's HTTP REST port (7778).
///
/// Required only when `[http_auth]` is enabled on the dittod nodes.
/// Note: `password` is stored in **plain text** here because it must be
/// sent as-is in the `Authorization: Basic` header.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HttpClientAuthConfig {
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
}

impl Default for MgmtConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                bind:     "0.0.0.0".into(),
                port:     7781,
                tls_cert: None,
                tls_key:  None,
            },
            connection: ConnectionConfig {
                seeds:        vec!["127.0.0.1:7779".into()],
                cluster_port: 7779,
                timeout_ms:   3000,
                insecure_http_hostnames: false,
            },
            tls:              TlsConfig::default(),
            admin:            AdminConfig::default(),
            http_client_auth: HttpClientAuthConfig::default(),
        }
    }
}

impl MgmtConfig {
    pub fn load() -> Result<Self> {
        let path = config_path();
        if path.exists() {
            let raw = fs::read_to_string(&path)
                .with_context(|| format!("reading {:?}", path))?;
            toml::from_str(&raw).context("parsing ditto-mgmt config")
        } else {
            Ok(Self::default())
        }
    }
}

fn config_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("ditto")
        .join("mgmt.toml")
}
