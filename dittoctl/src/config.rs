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
            },
            output: OutputConfig {
                format: "binary".into(),
            },
        }
    }
}

impl CtlConfig {
    pub fn load() -> Result<Self> {
        let path = config_path();
        if path.exists() {
            let raw = fs::read_to_string(&path).with_context(|| format!("reading {:?}", path))?;
            toml::from_str(&raw).context("parsing dittoctl config")
        } else {
            Ok(Self::default())
        }
    }

    pub fn save(&self) -> Result<()> {
        let path = config_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let raw = toml::to_string_pretty(self)?;
        fs::write(&path, raw)?;
        Ok(())
    }
}

fn config_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("ditto")
        .join("kvctl.toml")
}
