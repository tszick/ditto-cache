use crate::config::MgmtConfig;
use anyhow::{Context, Result};

fn version_requested(arg: &str) -> bool {
    matches!(arg, "--version" | "-V")
}

pub fn apply_env_overrides(cfg: &mut MgmtConfig) -> Result<()> {
    if let Ok(v) = std::env::var("DITTO_MGMT_TLS_CERT") {
        cfg.server.tls_cert = Some(v);
    }
    if let Ok(v) = std::env::var("DITTO_MGMT_TLS_KEY") {
        cfg.server.tls_key = Some(v);
    }
    if let Ok(v) = std::env::var("DITTO_MGMT_ADMIN_USER") {
        cfg.admin.username = Some(v);
    }
    if let Ok(v) = std::env::var("DITTO_MGMT_ADMIN_PASSWORD_HASH") {
        cfg.admin.password_hash = Some(v);
    }
    if let Ok(v) = std::env::var("DITTO_MGMT_ADMIN_BEARER_TOKEN_SHA256") {
        cfg.admin.bearer_token_sha256 = Some(v);
    }
    if let Ok(v) = std::env::var("DITTO_MGMT_ADMIN_BEARER_INTROSPECTION_URL") {
        cfg.admin.bearer_introspection_url = Some(v);
    }
    if let Ok(v) = std::env::var("DITTO_MGMT_ADMIN_BEARER_INTROSPECTION_CLIENT_ID") {
        cfg.admin.bearer_introspection_client_id = Some(v);
    }
    if let Ok(v) = std::env::var("DITTO_MGMT_ADMIN_BEARER_INTROSPECTION_CLIENT_SECRET") {
        cfg.admin.bearer_introspection_client_secret = Some(v);
    }
    if let Ok(v) = std::env::var("DITTO_MGMT_ADMIN_BEARER_INTROSPECTION_CLIENT_SECRET_ENV") {
        cfg.admin.bearer_introspection_client_secret_env = Some(v);
    }
    if let Ok(v) = std::env::var("DITTO_MGMT_ADMIN_BEARER_INTROSPECTION_CLIENT_SECRET_FILE") {
        cfg.admin.bearer_introspection_client_secret_file = Some(v);
    }
    if let Ok(v) = std::env::var("DITTO_MGMT_ADMIN_BEARER_REQUIRED_SCOPE") {
        cfg.admin.bearer_required_scope = Some(v);
    }
    if let Ok(v) = std::env::var("DITTO_MGMT_ADMIN_BEARER_REQUIRED_AUDIENCE") {
        cfg.admin.bearer_required_audience = Some(v);
    }
    if let Ok(v) = std::env::var("DITTO_MGMT_ADMIN_BASIC_ROLE") {
        cfg.admin.basic_role = v.parse()?;
    }
    if let Ok(v) = std::env::var("DITTO_MGMT_ADMIN_BEARER_ROLE") {
        cfg.admin.bearer_role = v.parse()?;
    }
    if let Ok(v) = std::env::var("DITTO_MGMT_HTTP_AUTH_USER") {
        cfg.http_client_auth.username = Some(v);
    }
    if let Ok(v) = std::env::var("DITTO_MGMT_HTTP_AUTH_PASSWORD") {
        cfg.http_client_auth.password = Some(v);
    }
    if let Ok(v) = std::env::var("DITTO_MGMT_HTTP_AUTH_PASSWORD_ENV") {
        cfg.http_client_auth.password_env = Some(v);
    }
    if let Ok(v) = std::env::var("DITTO_MGMT_HTTP_AUTH_PASSWORD_FILE") {
        cfg.http_client_auth.password_file = Some(v);
    }
    if let Ok(v) = std::env::var("DITTO_MGMT_BIND") {
        cfg.server.bind = v;
    }
    Ok(())
}

pub fn validate_strict_security(cfg: &MgmtConfig) -> Result<()> {
    if !cfg.tls.enabled {
        anyhow::bail!(
            "Strict security: [tls].enabled must be true in ditto-mgmt. Refusing unsecured admin RPC to nodes."
        );
    }

    if !cfg.admin.auth_configured() {
        anyhow::bail!(
            "Strict security: [admin] Basic or Bearer auth must be configured. Refusing unauthenticated management API."
        );
    }

    if cfg.server.tls_cert.is_none() || cfg.server.tls_key.is_none() {
        anyhow::bail!(
            "Strict security: [server].tls_cert and [server].tls_key must be configured. Refusing plain HTTP management API."
        );
    }

    Ok(())
}

pub fn management_bind_addr(cfg: &MgmtConfig) -> Result<String> {
    let resolved_bind =
        ditto_config::resolve_bind_addr(&cfg.server.bind).context("resolving server.bind")?;
    Ok(format!("{}:{}", resolved_bind, cfg.server.port))
}

pub fn load_config_from_args() -> Result<MgmtConfig> {
    let cfg_path = std::env::args().nth(1);
    if let Some(path) = cfg_path {
        if version_requested(&path) {
            println!("ditto-mgmt {}", env!("CARGO_PKG_VERSION"));
            std::process::exit(0);
        }
        let raw = std::fs::read_to_string(&path)?;
        Ok(toml::from_str(&raw)?)
    } else {
        MgmtConfig::load()
    }
}

pub fn build_http_client(cfg: &MgmtConfig) -> Result<reqwest::Client> {
    let mut builder =
        reqwest::Client::builder().timeout(std::time::Duration::from_millis(cfg.connection.timeout_ms));
    if cfg.tls.enabled && !cfg.tls.ca_cert.is_empty() {
        match crate::tls::load_reqwest_ca_cert(&cfg.tls.ca_cert) {
            Ok(cert) => {
                builder = builder.add_root_certificate(cert);
            }
            Err(e) => eprintln!("warning: could not load CA cert for reqwest: {}", e),
        }
    }
    builder.build().map_err(Into::into)
}
