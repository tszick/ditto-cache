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

mod api;
mod auth;
mod config;
mod node_client;
mod tls;
mod web;

use anyhow::{Context, Result};
use api::{build_router, AppState};
use config::MgmtConfig;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    // Install the ring crypto provider for rustls (must happen before any TLS use).
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let cfg_path = std::env::args().nth(1);
    let mut cfg: MgmtConfig = if let Some(path) = cfg_path {
        let raw = std::fs::read_to_string(&path)?;
        toml::from_str(&raw)?
    } else {
        MgmtConfig::load()?
    };

    // Environment variable overrides.
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
    if let Ok(v) = std::env::var("DITTO_MGMT_HTTP_AUTH_USER") {
        cfg.http_client_auth.username = Some(v);
    }
    if let Ok(v) = std::env::var("DITTO_MGMT_HTTP_AUTH_PASSWORD") {
        cfg.http_client_auth.password = Some(v);
    }
    if let Ok(v) = std::env::var("DITTO_MGMT_BIND") {
        cfg.server.bind = v;
    }

    // Strict security mode: management-to-node admin RPC must use mTLS.
    if !cfg.tls.enabled {
        anyhow::bail!(
            "Strict security: [tls].enabled must be true in ditto-mgmt. Refusing unsecured admin RPC to nodes."
        );
    }

    // Strict security mode: management API must require authentication.
    if cfg.admin.password_hash.is_none() {
        anyhow::bail!(
            "Strict security: [admin].password_hash must be configured. Refusing unauthenticated management API."
        );
    }

    // Strict security mode: management API must be served over HTTPS.
    if cfg.server.tls_cert.is_none() || cfg.server.tls_key.is_none() {
        anyhow::bail!(
            "Strict security: [server].tls_cert and [server].tls_key must be configured. Refusing plain HTTP management API."
        );
    }

    let tls = tls::build_connector(&cfg.tls)?;

    // Build the reqwest client for proxying to dittod's HTTP port (7778).
    // When TLS is enabled we add the CA cert so node certs are trusted.
    let http_client = {
        let mut builder = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(cfg.connection.timeout_ms));
        if cfg.tls.enabled && !cfg.tls.ca_cert.is_empty() {
            match tls::load_reqwest_ca_cert(&cfg.tls.ca_cert) {
                Ok(cert) => {
                    builder = builder.add_root_certificate(cert);
                }
                Err(e) => eprintln!("warning: could not load CA cert for reqwest: {}", e),
            }
        }
        builder.build()?
    };

    let resolved_bind =
        ditto_config::resolve_bind_addr(&cfg.server.bind).context("resolving server.bind")?;
    let bind = format!("{}:{}", resolved_bind, cfg.server.port);

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
            println!("ditto-mgmt listening on https://{}", bind);
            tls::serve_tls(&bind, app, cert, key).await?;
        }
        _ => {
            println!("ditto-mgmt listening on http://{}", bind);
            let listener = tokio::net::TcpListener::bind(&bind).await?;
            axum::serve(listener, app).await?;
        }
    }

    Ok(())
}
