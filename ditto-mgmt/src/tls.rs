use crate::config::TlsConfig;
use anyhow::{Context, Result};
use axum::Router;
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer},
};
use rustls_pemfile::{certs, private_key};
use std::{fs, io::BufReader, sync::Arc};
use tokio_rustls::TlsConnector;

/// Build a TLS connector from the given config.
/// Returns `None` when TLS is disabled.
pub fn build_connector(cfg: &TlsConfig) -> Result<Option<TlsConnector>> {
    if !cfg.enabled {
        return Ok(None);
    }

    let ca_certs = load_certs(&cfg.ca_cert)
        .with_context(|| format!("loading CA cert '{}'", cfg.ca_cert))?;
    let mut root_store = RootCertStore::empty();
    for cert in ca_certs {
        root_store.add(cert).context("adding CA cert to root store")?;
    }

    let certs = load_certs(&cfg.cert)
        .with_context(|| format!("loading client cert '{}'", cfg.cert))?;
    let key = load_key(&cfg.key)
        .with_context(|| format!("loading client key '{}'", cfg.key))?;

    let client_cfg = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_client_auth_cert(certs, key)
        .context("building TLS ClientConfig")?;

    Ok(Some(TlsConnector::from(Arc::new(client_cfg))))
}

/// Build a reqwest Certificate from a PEM CA file so that the HTTP client can
/// validate self-signed node certificates on port 7778.
pub fn load_reqwest_ca_cert(ca_cert_path: &str) -> Result<reqwest::Certificate> {
    let pem = fs::read(ca_cert_path)
        .with_context(|| format!("reading CA cert '{}'", ca_cert_path))?;
    reqwest::Certificate::from_pem(&pem).context("parsing CA cert for reqwest")
}

/// Serve the management API over HTTPS using server-only TLS (no client cert).
///
/// Called from `main.rs` when `[server] tls_cert` and `tls_key` are set.
pub async fn serve_tls(
    bind: &str,
    app: Router,
    cert_path: &str,
    key_path: &str,
) -> Result<()> {
    use axum_server::tls_rustls::RustlsConfig;
    let config = RustlsConfig::from_pem_file(cert_path, key_path).await?;
    let addr: std::net::SocketAddr = bind.parse()
        .with_context(|| format!("parsing bind address '{}'", bind))?;
    axum_server::bind_rustls(addr, config)
        .serve(app.into_make_service())
        .await
        .context("HTTPS management server error")
}

fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    let f = fs::File::open(path).with_context(|| format!("opening '{}'", path))?;
    let mut reader = BufReader::new(f);
    certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .context("reading PEM certificates")
}

fn load_key(path: &str) -> Result<PrivateKeyDer<'static>> {
    let f = fs::File::open(path).with_context(|| format!("opening '{}'", path))?;
    let mut reader = BufReader::new(f);
    private_key(&mut reader)
        .context("reading private key")?
        .context("no private key found in file")
}
