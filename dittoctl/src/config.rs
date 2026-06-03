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
    /// Legacy direct plaintext Basic Auth password. Prefer `password_env` or `password_file`.
    #[serde(default)]
    pub password: Option<String>,
    /// Name of an environment variable containing the Basic Auth password.
    #[serde(default)]
    pub password_env: Option<String>,
    /// Path to a secret file containing the Basic Auth password.
    #[serde(default)]
    pub password_file: Option<String>,
    /// Legacy direct plaintext Bearer token. Prefer `bearer_token_env` or `bearer_token_file`.
    #[serde(default)]
    pub bearer_token: Option<String>,
    /// Name of an environment variable containing the Bearer token.
    #[serde(default)]
    pub bearer_token_env: Option<String>,
    /// Path to a secret file containing the Bearer token.
    #[serde(default)]
    pub bearer_token_file: Option<String>,
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
                password_env: None,
                password_file: None,
                bearer_token: None,
                bearer_token_env: None,
                bearer_token_file: None,
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

impl MgmtConfig {
    pub fn resolve_credentials(&mut self) -> Result<()> {
        if self.password.is_none() {
            self.password = self.resolve_password_with(
                |name| std::env::var(name).ok(),
                |path| fs::read_to_string(path),
            )?;
        }
        if self.bearer_token.is_none() {
            self.bearer_token = self.resolve_bearer_token_with(
                |name| std::env::var(name).ok(),
                |path| fs::read_to_string(path),
            )?;
        }
        Ok(())
    }

    fn resolve_password_with<F, R>(&self, get_env: F, read_file: R) -> Result<Option<String>>
    where
        F: Fn(&str) -> Option<String>,
        R: Fn(&str) -> std::io::Result<String>,
    {
        resolve_secret_source_with(
            "mgmt.password",
            self.password_env.as_deref(),
            self.password_file.as_deref(),
            get_env,
            read_file,
        )
    }

    fn resolve_bearer_token_with<F, R>(&self, get_env: F, read_file: R) -> Result<Option<String>>
    where
        F: Fn(&str) -> Option<String>,
        R: Fn(&str) -> std::io::Result<String>,
    {
        resolve_secret_source_with(
            "mgmt.bearer_token",
            self.bearer_token_env.as_deref(),
            self.bearer_token_file.as_deref(),
            get_env,
            read_file,
        )
    }
}

fn resolve_secret_source_with<F, R>(
    label: &str,
    env_source: Option<&str>,
    file_source: Option<&str>,
    get_env: F,
    read_file: R,
) -> Result<Option<String>>
where
    F: Fn(&str) -> Option<String>,
    R: Fn(&str) -> std::io::Result<String>,
{
    if let Some(env_name) = env_source.map(str::trim) {
        if env_name.is_empty() {
            anyhow::bail!("{label}_env must not be empty");
        }
        let secret = get_env(env_name).with_context(|| {
            format!("environment variable {env_name} from {label}_env is not set")
        })?;
        if secret.is_empty() {
            anyhow::bail!("environment variable {env_name} from {label}_env is empty");
        }
        return Ok(Some(secret));
    }

    if let Some(path) = file_source.map(str::trim) {
        if path.is_empty() {
            anyhow::bail!("{label}_file must not be empty");
        }
        let secret = read_file(path).with_context(|| format!("reading {label}_file {path}"))?;
        let secret = secret.trim_end_matches(['\r', '\n']).to_string();
        if secret.is_empty() {
            anyhow::bail!("{label}_file {path} is empty");
        }
        return Ok(Some(secret));
    }

    Ok(None)
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
                password_env: None,
                password_file: None,
                bearer_token: None,
                bearer_token_env: None,
                bearer_token_file: None,
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
                password_env: None,
                password_file: None,
                bearer_token: Some("sso-access-token".into()),
                bearer_token_env: None,
                bearer_token_file: None,
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
    fn mgmt_credentials_resolve_from_env_or_file_without_plaintext_toml() {
        let password_cfg = MgmtConfig {
            password_env: Some("DITTOCTL_MGMT_PASSWORD".into()),
            ..CtlConfig::default().mgmt
        };
        assert_eq!(
            password_cfg
                .resolve_password_with(
                    |name| (name == "DITTOCTL_MGMT_PASSWORD").then(|| "from-env".into()),
                    |_| unreachable!(),
                )
                .unwrap()
                .as_deref(),
            Some("from-env")
        );

        let bearer_cfg = MgmtConfig {
            bearer_token_file: Some("/run/secrets/dittoctl-token".into()),
            ..CtlConfig::default().mgmt
        };
        assert_eq!(
            bearer_cfg
                .resolve_bearer_token_with(
                    |_| unreachable!(),
                    |path| {
                        assert_eq!(path, "/run/secrets/dittoctl-token");
                        Ok("from-file\r\n".into())
                    },
                )
                .unwrap()
                .as_deref(),
            Some("from-file")
        );
    }

    #[test]
    fn mgmt_credentials_keep_direct_values_for_legacy_configs() {
        let mut cfg = MgmtConfig {
            password: Some("direct-password".into()),
            password_env: Some("DITTOCTL_MGMT_PASSWORD".into()),
            bearer_token: Some("direct-token".into()),
            bearer_token_file: Some("/run/secrets/dittoctl-token".into()),
            ..CtlConfig::default().mgmt
        };

        cfg.resolve_credentials().unwrap();

        assert_eq!(cfg.password.as_deref(), Some("direct-password"));
        assert_eq!(cfg.bearer_token.as_deref(), Some("direct-token"));
    }

    #[test]
    fn mgmt_credentials_reject_missing_or_empty_secret_sources() {
        let missing_env = MgmtConfig {
            password_env: Some("MISSING_DITTOCTL_MGMT_PASSWORD".into()),
            ..CtlConfig::default().mgmt
        };
        let err = missing_env
            .resolve_password_with(|_| None, |_| unreachable!())
            .unwrap_err();
        assert!(err.to_string().contains("is not set"));

        let empty_file = MgmtConfig {
            bearer_token_file: Some("/run/secrets/empty-dittoctl-token".into()),
            ..CtlConfig::default().mgmt
        };
        let err = empty_file
            .resolve_bearer_token_with(|_| unreachable!(), |_| Ok("\n".into()))
            .unwrap_err();
        assert!(err.to_string().contains("is empty"));
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
