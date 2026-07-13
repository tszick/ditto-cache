use crate::bootstrap::{apply_env_overrides, apply_replication_guardrails};
use crate::config::Config;
use anyhow::Result;
use std::path::PathBuf;

pub struct StartupState {
    pub config_path: String,
    pub config_missing: bool,
    pub config: Config,
    pub insecure: bool,
}

pub fn load_startup_state() -> Result<StartupState> {
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "node.toml".to_string());

    let config_missing = !PathBuf::from(&config_path).exists();
    let mut config = if config_missing {
        Config::default()
    } else {
        Config::load(&config_path)?
    };

    apply_env_overrides(&mut config);
    config.backup.resolve_encryption_key()?;
    config.client_auth.resolve_tokens()?;
    apply_replication_guardrails(&mut config);

    let insecure = std::env::var("DITTO_INSECURE")
        .unwrap_or_default()
        .eq_ignore_ascii_case("true");
    if insecure && !cfg!(debug_assertions) {
        anyhow::bail!(
            "DITTO_INSECURE is blocked in release builds. Use a debug/dev build for insecure local testing."
        );
    }

    Ok(StartupState {
        config_path,
        config_missing,
        config,
        insecure,
    })
}
