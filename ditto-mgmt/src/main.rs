//! ditto-mgmt — Ditto cluster management service.
//!
//! Exposes an HTTP REST API (default port **7781**) consumed by both
//! `dittoctl` (CLI) and the embedded Bootstrap 5 web UI.
//! Communicates with `dittod` nodes over TCP/mTLS on port 7779 using
//! the Ditto admin protocol.
//!
//! # Usage
//! ```
//! ditto-mgmt [config-file]
//! ```
//! When no config file is given, defaults are loaded from
//! `~/.config/ditto/mgmt.toml` (created automatically on first run).

mod app;
mod api;
mod audit;
mod auth;
mod bootstrap;
mod config;
mod node_client;
mod policy;
mod tls;
mod web;

use anyhow::Result;
use api::{build_router, AppState};
use bootstrap::{
    apply_env_overrides, build_http_client, load_config_from_args, management_bind_addr,
    validate_strict_security,
};
use std::sync::Arc;

const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> Result<()> {
    // Install the ring crypto provider for rustls (must happen before any TLS use).
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let mut cfg = load_config_from_args()?;

    apply_env_overrides(&mut cfg)?;
    cfg.admin.resolve_bearer_introspection_client_secret()?;
    cfg.http_client_auth.resolve_password()?;
    validate_strict_security(&cfg)?;

    let tls = tls::build_connector(&cfg.tls)?;
    let http_client = build_http_client(&cfg)?;

    let bind = management_bind_addr(&cfg)?;

    let state = Arc::new(AppState {
        cfg: Arc::new(cfg),
        tls,
        http_client,
        addr_cache: tokio::sync::Mutex::new(None),
    });

    let app = build_router(state.clone());

    // Serve over HTTPS when [server] tls_cert + tls_key are both configured;
    // otherwise fall back to plain HTTP.
    match (&state.cfg.server.tls_cert, &state.cfg.server.tls_key) {
        (Some(cert), Some(key)) => {
            println!("ditto-mgmt v{} listening on https://{}", APP_VERSION, bind);
            tls::serve_tls(&bind, app, cert, key).await?;
        }
        _ => {
            println!("ditto-mgmt v{} listening on http://{}", APP_VERSION, bind);
            let listener = tokio::net::TcpListener::bind(&bind).await?;
            axum::serve(listener, app).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MgmtConfig;

    fn clear_env_overrides() {
        for key in [
            "DITTO_MGMT_TLS_CERT",
            "DITTO_MGMT_TLS_KEY",
            "DITTO_MGMT_ADMIN_USER",
            "DITTO_MGMT_ADMIN_PASSWORD_HASH",
            "DITTO_MGMT_ADMIN_BEARER_TOKEN_SHA256",
            "DITTO_MGMT_ADMIN_BEARER_INTROSPECTION_URL",
            "DITTO_MGMT_ADMIN_BEARER_INTROSPECTION_CLIENT_ID",
            "DITTO_MGMT_ADMIN_BEARER_INTROSPECTION_CLIENT_SECRET",
            "DITTO_MGMT_ADMIN_BEARER_INTROSPECTION_CLIENT_SECRET_ENV",
            "DITTO_MGMT_ADMIN_BEARER_INTROSPECTION_CLIENT_SECRET_FILE",
            "DITTO_MGMT_ADMIN_BEARER_REQUIRED_SCOPE",
            "DITTO_MGMT_ADMIN_BEARER_REQUIRED_AUDIENCE",
            "DITTO_MGMT_ADMIN_BASIC_ROLE",
            "DITTO_MGMT_ADMIN_BEARER_ROLE",
            "DITTO_MGMT_HTTP_AUTH_USER",
            "DITTO_MGMT_HTTP_AUTH_PASSWORD",
            "DITTO_MGMT_HTTP_AUTH_PASSWORD_ENV",
            "DITTO_MGMT_HTTP_AUTH_PASSWORD_FILE",
            "DITTO_MGMT_BIND",
        ] {
            std::env::remove_var(key);
        }
    }

    #[test]
    fn env_overrides_update_management_config_fields() {
        clear_env_overrides();
        std::env::set_var("DITTO_MGMT_TLS_CERT", "cert.pem");
        std::env::set_var("DITTO_MGMT_TLS_KEY", "key.pem");
        std::env::set_var("DITTO_MGMT_ADMIN_USER", "admin-user");
        std::env::set_var("DITTO_MGMT_ADMIN_PASSWORD_HASH", "$2b$hash");
        std::env::set_var("DITTO_MGMT_ADMIN_BEARER_TOKEN_SHA256", "abc123");
        std::env::set_var(
            "DITTO_MGMT_ADMIN_BEARER_INTROSPECTION_URL",
            "https://sso.example/introspect",
        );
        std::env::set_var("DITTO_MGMT_ADMIN_BEARER_INTROSPECTION_CLIENT_ID", "ditto");
        std::env::set_var(
            "DITTO_MGMT_ADMIN_BEARER_INTROSPECTION_CLIENT_SECRET",
            "secret",
        );
        std::env::set_var(
            "DITTO_MGMT_ADMIN_BEARER_INTROSPECTION_CLIENT_SECRET_ENV",
            "OIDC_CLIENT_SECRET",
        );
        std::env::set_var(
            "DITTO_MGMT_ADMIN_BEARER_INTROSPECTION_CLIENT_SECRET_FILE",
            "/run/secrets/oidc-client-secret",
        );
        std::env::set_var("DITTO_MGMT_ADMIN_BEARER_REQUIRED_SCOPE", "ditto.mgmt");
        std::env::set_var("DITTO_MGMT_ADMIN_BEARER_REQUIRED_AUDIENCE", "ditto-mgmt");
        std::env::set_var("DITTO_MGMT_ADMIN_BASIC_ROLE", "operator");
        std::env::set_var("DITTO_MGMT_ADMIN_BEARER_ROLE", "read-only");
        std::env::set_var("DITTO_MGMT_HTTP_AUTH_USER", "node-user");
        std::env::set_var("DITTO_MGMT_HTTP_AUTH_PASSWORD", "node-pass");
        std::env::set_var("DITTO_MGMT_HTTP_AUTH_PASSWORD_ENV", "NODE_HTTP_PASSWORD");
        std::env::set_var(
            "DITTO_MGMT_HTTP_AUTH_PASSWORD_FILE",
            "/run/secrets/node-http-password",
        );
        std::env::set_var("DITTO_MGMT_BIND", "127.0.0.1");

        let mut cfg = MgmtConfig::default();
        apply_env_overrides(&mut cfg).unwrap();

        assert_eq!(cfg.server.tls_cert.as_deref(), Some("cert.pem"));
        assert_eq!(cfg.server.tls_key.as_deref(), Some("key.pem"));
        assert_eq!(cfg.admin.username.as_deref(), Some("admin-user"));
        assert_eq!(cfg.admin.password_hash.as_deref(), Some("$2b$hash"));
        assert_eq!(cfg.admin.bearer_token_sha256.as_deref(), Some("abc123"));
        assert_eq!(
            cfg.admin.bearer_introspection_url.as_deref(),
            Some("https://sso.example/introspect")
        );
        assert_eq!(
            cfg.admin.bearer_introspection_client_id.as_deref(),
            Some("ditto")
        );
        assert_eq!(
            cfg.admin.bearer_introspection_client_secret.as_deref(),
            Some("secret")
        );
        assert_eq!(
            cfg.admin.bearer_introspection_client_secret_env.as_deref(),
            Some("OIDC_CLIENT_SECRET")
        );
        assert_eq!(
            cfg.admin.bearer_introspection_client_secret_file.as_deref(),
            Some("/run/secrets/oidc-client-secret")
        );
        assert_eq!(
            cfg.admin.bearer_required_scope.as_deref(),
            Some("ditto.mgmt")
        );
        assert_eq!(
            cfg.admin.bearer_required_audience.as_deref(),
            Some("ditto-mgmt")
        );
        assert_eq!(cfg.admin.basic_role, config::AdminRole::Operator);
        assert_eq!(cfg.admin.bearer_role, config::AdminRole::ReadOnly);
        assert_eq!(cfg.http_client_auth.username.as_deref(), Some("node-user"));
        assert_eq!(cfg.http_client_auth.password.as_deref(), Some("node-pass"));
        assert_eq!(
            cfg.http_client_auth.password_env.as_deref(),
            Some("NODE_HTTP_PASSWORD")
        );
        assert_eq!(
            cfg.http_client_auth.password_file.as_deref(),
            Some("/run/secrets/node-http-password")
        );
        assert_eq!(cfg.server.bind, "127.0.0.1");

        clear_env_overrides();
    }

    #[test]
    fn strict_security_reports_each_missing_requirement() {
        let mut cfg = MgmtConfig::default();
        let err = validate_strict_security(&cfg).expect_err("missing TLS should fail");
        assert!(err.to_string().contains("[tls].enabled"));

        cfg.tls.enabled = true;
        let err = validate_strict_security(&cfg).expect_err("missing admin auth should fail");
        assert!(err.to_string().contains("[admin] Basic or Bearer auth"));

        cfg.admin.bearer_token_sha256 = Some("abc123".into());
        let err = validate_strict_security(&cfg).expect_err("missing HTTPS cert/key should fail");
        assert!(err.to_string().contains("[server].tls_cert"));

        cfg.server.tls_cert = Some("cert.pem".into());
        cfg.server.tls_key = Some("key.pem".into());
        validate_strict_security(&cfg).expect("complete strict config should pass");
    }

    #[test]
    fn management_bind_addr_resolves_loopback_and_port() {
        let mut cfg = MgmtConfig::default();
        cfg.server.bind = "localhost".into();
        cfg.server.port = 9901;
        assert_eq!(management_bind_addr(&cfg).unwrap(), "localhost:9901");
    }
}
