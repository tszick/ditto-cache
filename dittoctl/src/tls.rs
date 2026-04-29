use crate::config::TlsConfig;
use anyhow::{Context, Result};
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer},
};
use rustls_pki_types::pem::PemObject;
use std::sync::Arc;
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

fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    CertificateDer::pem_file_iter(path)
        .with_context(|| format!("opening '{}'", path))?
        .collect::<Result<Vec<_>, _>>()
        .context("reading PEM certificates")
}

fn load_key(path: &str) -> Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_file(path).context("reading private key")
}
