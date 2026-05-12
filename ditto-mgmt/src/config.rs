//! Configuration for `ditto-mgmt`.
//!
//! The config file lives at `~/.config/ditto/mgmt.toml`.
//! If the file does not exist, [`MgmtConfig::load`] returns built-in defaults:
//! bind `0.0.0.0:7781`, seed `127.0.0.1:7779`, TLS disabled.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{fs, path::PathBuf, str::FromStr};

/// Top-level configuration for the management service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MgmtConfig {
    pub server: ServerConfig,
    pub connection: ConnectionConfig,
    #[serde(default)]
    pub tls: TlsConfig,
    /// Optional HTTP Basic Auth for the management API (7781).
    #[serde(default)]
    pub admin: AdminConfig,
    /// Credentials ditto-mgmt uses when proxying cache requests to dittod's
    /// HTTP REST port (7778), when auth is enabled there.
    #[serde(default)]
    pub http_client_auth: HttpClientAuthConfig,
    /// Policy for displaying cached values in the web UI/API.
    #[serde(default)]
    pub cache_values: CacheValuePolicy,
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
    pub seeds: Vec<String>,
    /// Cluster/admin TCP port (default 7779).
    pub cluster_port: u16,
    /// RPC timeout in milliseconds.
    pub timeout_ms: u64,
}

/// TLS configuration for mTLS connections to node cluster ports.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TlsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub ca_cert: String,
    #[serde(default)]
    pub cert: String,
    #[serde(default)]
    pub key: String,
}

/// Optional admin authentication for the management server (port 7781).
///
/// Basic auth is enabled when `password_hash` is set. Bearer auth is enabled
/// when either `bearer_token_sha256` or `bearer_introspection_url` is set.
///
/// Generate a hash with: `dittoctl hash-password`
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AdminConfig {
    /// Username (default: `"admin"` when only hash is set).
    #[serde(default)]
    pub username: Option<String>,
    /// bcrypt hash of the password.  When absent, auth is disabled.
    #[serde(default)]
    pub password_hash: Option<String>,
    /// SHA-256 hex digest of an accepted opaque Bearer token.
    #[serde(default)]
    pub bearer_token_sha256: Option<String>,
    /// OAuth2/OIDC token introspection endpoint for validating Bearer tokens.
    #[serde(default)]
    pub bearer_introspection_url: Option<String>,
    /// Optional client id for introspection endpoint authentication.
    #[serde(default)]
    pub bearer_introspection_client_id: Option<String>,
    /// Optional client secret for introspection endpoint authentication.
    #[serde(default)]
    pub bearer_introspection_client_secret: Option<String>,
    /// Optional scope required in the introspection response.
    #[serde(default)]
    pub bearer_required_scope: Option<String>,
    /// Optional audience required in the introspection response.
    #[serde(default)]
    pub bearer_required_audience: Option<String>,
    /// Role assigned to authenticated Basic Auth callers.
    #[serde(default = "default_admin_role")]
    pub basic_role: AdminRole,
    /// Role assigned to authenticated Bearer callers.
    #[serde(default = "default_admin_role")]
    pub bearer_role: AdminRole,
}

impl AdminConfig {
    pub fn auth_configured(&self) -> bool {
        self.password_hash.is_some()
            || self.bearer_token_sha256.is_some()
            || self.bearer_introspection_url.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum AdminRole {
    ReadOnly,
    Operator,
    #[default]
    Admin,
}

impl AdminRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read-only",
            Self::Operator => "operator",
            Self::Admin => "admin",
        }
    }
}

impl FromStr for AdminRole {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "read-only" | "readonly" | "reader" => Ok(Self::ReadOnly),
            "operator" | "ops" => Ok(Self::Operator),
            "admin" | "administrator" => Ok(Self::Admin),
            other => anyhow::bail!("invalid admin role '{other}'"),
        }
    }
}

fn default_admin_role() -> AdminRole {
    AdminRole::Admin
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

/// Controls how cached values are exposed by the management API.
///
/// Production default is intentionally conservative: values are masked and
/// fingerprinted server-side; full reveal must be explicitly enabled.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheValuePolicy {
    #[serde(default = "default_mask_values")]
    pub mask_values_by_default: bool,
    #[serde(default)]
    pub allow_value_reveal: bool,
    #[serde(default)]
    pub allow_sensitive_value_reveal: bool,
    #[serde(default = "default_sensitive_key_patterns")]
    pub sensitive_key_patterns: Vec<String>,
}

impl Default for CacheValuePolicy {
    fn default() -> Self {
        Self {
            mask_values_by_default: true,
            allow_value_reveal: false,
            allow_sensitive_value_reveal: false,
            sensitive_key_patterns: default_sensitive_key_patterns(),
        }
    }
}

fn default_mask_values() -> bool {
    true
}

fn default_sensitive_key_patterns() -> Vec<String> {
    [
        "token",
        "session",
        "secret",
        "password",
        "passwd",
        "credential",
        "auth",
        "jwt",
        "bearer",
        "api_key",
        "apikey",
        "refresh",
        "access",
        "csrf",
        "xsrf",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

impl Default for MgmtConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                bind: "0.0.0.0".into(),
                port: 7781,
                tls_cert: None,
                tls_key: None,
            },
            connection: ConnectionConfig {
                seeds: vec!["127.0.0.1:7779".into()],
                cluster_port: 7779,
                timeout_ms: 3000,
            },
            tls: TlsConfig::default(),
            admin: AdminConfig::default(),
            http_client_auth: HttpClientAuthConfig::default(),
            cache_values: CacheValuePolicy::default(),
        }
    }
}

impl MgmtConfig {
    pub fn load() -> Result<Self> {
        let path = config_path();
        if path.exists() {
            let raw = fs::read_to_string(&path).with_context(|| format!("reading {:?}", path))?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_matches_documented_local_management_defaults() {
        let cfg = MgmtConfig::default();
        assert_eq!(cfg.server.bind, "0.0.0.0");
        assert_eq!(cfg.server.port, 7781);
        assert!(cfg.server.tls_cert.is_none());
        assert!(cfg.server.tls_key.is_none());
        assert_eq!(cfg.connection.seeds, vec!["127.0.0.1:7779"]);
        assert_eq!(cfg.connection.cluster_port, 7779);
        assert_eq!(cfg.connection.timeout_ms, 3000);
        assert!(!cfg.tls.enabled);
        assert!(cfg.admin.username.is_none());
        assert!(cfg.admin.password_hash.is_none());
        assert!(cfg.admin.bearer_token_sha256.is_none());
        assert!(cfg.admin.bearer_introspection_url.is_none());
        assert_eq!(cfg.admin.basic_role, AdminRole::Admin);
        assert_eq!(cfg.admin.bearer_role, AdminRole::Admin);
        assert!(cfg.http_client_auth.username.is_none());
        assert!(cfg.http_client_auth.password.is_none());
        assert!(cfg.cache_values.mask_values_by_default);
        assert!(!cfg.cache_values.allow_value_reveal);
        assert!(!cfg.cache_values.allow_sensitive_value_reveal);
        assert!(cfg
            .cache_values
            .sensitive_key_patterns
            .contains(&"token".to_string()));
    }

    #[test]
    fn partial_toml_deserializes_defaulted_optional_sections() {
        let raw = r#"
            [server]
            bind = "127.0.0.1"
            port = 9000

            [connection]
            seeds = ["node-1:7779", "node-2:7779"]
            cluster_port = 7779
            timeout_ms = 1500
        "#;

        let cfg: MgmtConfig = toml::from_str(raw).expect("parse partial mgmt config");
        assert_eq!(cfg.server.bind, "127.0.0.1");
        assert_eq!(cfg.server.port, 9000);
        assert_eq!(cfg.connection.seeds.len(), 2);
        assert!(!cfg.tls.enabled);
        assert!(cfg.admin.password_hash.is_none());
        assert!(!cfg.admin.auth_configured());
        assert!(cfg.http_client_auth.password.is_none());
        assert!(cfg.cache_values.mask_values_by_default);
    }

    #[test]
    fn admin_roles_parse_from_config_and_strings() {
        let raw = r#"
            username = "viewer"
            password_hash = "$2b$hash"
            basic_role = "read-only"
            bearer_role = "operator"
        "#;

        let admin: AdminConfig = toml::from_str(raw).expect("parse admin role config");
        assert_eq!(admin.basic_role, AdminRole::ReadOnly);
        assert_eq!(admin.bearer_role, AdminRole::Operator);
        assert_eq!("admin".parse::<AdminRole>().unwrap(), AdminRole::Admin);
        assert!("root".parse::<AdminRole>().is_err());
    }
}
