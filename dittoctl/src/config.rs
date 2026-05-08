//! Configuration for `dittoctl`.
//!
//! Stored at `~/.config/ditto/kvctl.toml`.
//! If the file does not exist, [`CtlConfig::load`] returns built-in defaults
//! (`http://localhost:7781`, 3 s timeout, binary export format).

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{fs, path::PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CtlConfig {
    pub mgmt: MgmtConfig,
    pub output: OutputConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MgmtConfig {
    /// Base URL of the ditto-mgmt service.  e.g. "http://localhost:7781"
    pub url: String,
    /// HTTP request timeout in milliseconds.
    pub timeout_ms: u64,
    /// Optional HTTP Basic Auth username for ditto-mgmt.
    #[serde(default)]
    pub username: Option<String>,
    /// Optional HTTP Basic Auth password for ditto-mgmt.
    #[serde(default)]
    pub password: Option<String>,
    /// Optional Bearer token for ditto-mgmt.
    #[serde(default)]
    pub bearer_token: Option<String>,
    /// Accept invalid/self-signed TLS certs for local/dev management endpoints.
    #[serde(default)]
    pub insecure_skip_verify: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    /// Default export format: "binary" or "json".
    pub format: String,
}

impl Default for CtlConfig {
    fn default() -> Self {
        Self {
            mgmt: MgmtConfig {
                url: "http://localhost:7781".into(),
                timeout_ms: 3000,
                username: None,
                password: None,
                bearer_token: None,
                insecure_skip_verify: false,
            },
            output: OutputConfig {
                format: "binary".into(),
            },
        }
    }
}

impl CtlConfig {
    pub fn load() -> Result<Self> {
        Self::load_from_path(config_path())
    }

    pub(crate) fn load_from_path(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let path = path.as_ref();
        if path.exists() {
            let raw = fs::read_to_string(path).with_context(|| format!("reading {:?}", path))?;
            toml::from_str(&raw).context("parsing dittoctl config")
        } else {
            Ok(Self::default())
        }
    }

    pub fn save(&self) -> Result<()> {
        self.save_to_path(config_path())
    }

    fn save_to_path(&self, path: impl AsRef<std::path::Path>) -> Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let raw = toml::to_string_pretty(self)?;
        fs::write(path, raw)?;
        Ok(())
    }
}

fn config_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("ditto")
        .join("kvctl.toml")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_test_file(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir()
            .join(format!(
                "dittoctl-config-test-{name}-{}",
                std::process::id()
            ))
            .join(nanos.to_string())
            .join("kvctl.toml")
    }

    #[test]
    fn load_from_missing_path_returns_defaults() {
        let path = unique_test_file("missing");
        let cfg = CtlConfig::load_from_path(&path).expect("load default config");
        assert_eq!(cfg.mgmt.url, "http://localhost:7781");
        assert_eq!(cfg.mgmt.timeout_ms, 3000);
        assert_eq!(cfg.output.format, "binary");
    }

    #[test]
    fn save_and_load_round_trip_custom_config() {
        let path = unique_test_file("roundtrip");
        let cfg = CtlConfig {
            mgmt: MgmtConfig {
                url: "https://mgmt.example.test:9443".into(),
                timeout_ms: 1500,
                username: Some("alice".into()),
                password: Some("secret".into()),
                bearer_token: None,
                insecure_skip_verify: true,
            },
            output: OutputConfig {
                format: "json".into(),
            },
        };

        cfg.save_to_path(&path).expect("save config");
        let loaded = CtlConfig::load_from_path(&path).expect("reload config");

        assert_eq!(loaded.mgmt.url, cfg.mgmt.url);
        assert_eq!(loaded.mgmt.timeout_ms, 1500);
        assert_eq!(loaded.mgmt.username.as_deref(), Some("alice"));
        assert_eq!(loaded.mgmt.password.as_deref(), Some("secret"));
        assert!(loaded.mgmt.insecure_skip_verify);
        assert_eq!(loaded.output.format, "json");

        if let Some(root) = path.ancestors().nth(2) {
            fs::remove_dir_all(root).ok();
        }
    }

    #[test]
    fn bearer_token_config_round_trips() {
        let path = unique_test_file("bearer");
        let cfg = CtlConfig {
            mgmt: MgmtConfig {
                url: "https://mgmt.example.test:9443".into(),
                timeout_ms: 1500,
                username: None,
                password: None,
                bearer_token: Some("sso-access-token".into()),
                insecure_skip_verify: false,
            },
            output: OutputConfig {
                format: "json".into(),
            },
        };

        cfg.save_to_path(&path).expect("save bearer config");
        let loaded = CtlConfig::load_from_path(&path).expect("reload bearer config");

        assert_eq!(
            loaded.mgmt.bearer_token.as_deref(),
            Some("sso-access-token")
        );
        assert!(loaded.mgmt.username.is_none());
        assert!(loaded.mgmt.password.is_none());

        if let Some(root) = path.ancestors().nth(2) {
            fs::remove_dir_all(root).ok();
        }
    }

    #[test]
    fn load_from_path_reports_toml_parse_context() {
        let path = unique_test_file("invalid");
        fs::create_dir_all(path.parent().unwrap()).expect("create config dir");
        fs::write(&path, "not valid = [").expect("write invalid config");

        let err = CtlConfig::load_from_path(&path).expect_err("invalid TOML should fail");
        assert!(err.to_string().contains("parsing dittoctl config"));

        if let Some(root) = path.ancestors().nth(2) {
            fs::remove_dir_all(root).ok();
        }
    }
}
