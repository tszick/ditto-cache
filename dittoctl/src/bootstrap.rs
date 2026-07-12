use crate::config::CtlConfig;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};

pub(crate) fn load_ctl_config(path: Option<&std::path::Path>) -> Result<CtlConfig> {
    match path {
        Some(path) => CtlConfig::load_from_path(path),
        None => CtlConfig::load(),
    }
}

pub(crate) fn build_http_client(cfg: &CtlConfig) -> Result<reqwest::Client> {
    let mut builder =
        reqwest::Client::builder().timeout(std::time::Duration::from_millis(cfg.mgmt.timeout_ms));

    if cfg.mgmt.insecure_skip_verify {
        builder = builder.danger_accept_invalid_certs(true);
    }

    if let Some(headers) = auth_headers(cfg)? {
        builder = builder.default_headers(headers);
    }

    Ok(builder.build()?)
}

pub(crate) fn auth_headers(cfg: &CtlConfig) -> Result<Option<HeaderMap>> {
    match (
        &cfg.mgmt.username,
        &cfg.mgmt.password,
        &cfg.mgmt.bearer_token,
    ) {
        (None, None, Some(token)) => {
            let mut headers = HeaderMap::new();
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {}", token))?,
            );
            Ok(Some(headers))
        }
        (Some(username), Some(password), None) => {
            let mut headers = HeaderMap::new();
            let auth = STANDARD.encode(format!("{}:{}", username, password));
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Basic {}", auth))?,
            );
            Ok(Some(headers))
        }
        (None, None, None) => Ok(None),
        (Some(_), Some(_), Some(_)) => {
            anyhow::bail!("dittoctl config must not set both Basic Auth and bearer_token")
        }
        _ => anyhow::bail!(
            "dittoctl config requires both mgmt.username and mgmt.password for Basic Auth, or only mgmt.bearer_token for Bearer auth"
        ),
    }
}
