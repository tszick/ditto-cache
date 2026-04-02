use crate::config::TlsConfig;
use anyhow::{Context, Result};
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
    server::WebPkiClientVerifier,
    ClientConfig, RootCertStore, ServerConfig,
};
use rustls_pemfile::{certs, private_key};
use std::{fs, io::BufReader, sync::Arc};
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// Build a TLS acceptor for the cluster/admin server (mTLS – requires client cert).
pub fn build_acceptor(cfg: &TlsConfig) -> Result<TlsAcceptor> {
    let ca_certs =
        load_certs(&cfg.ca_cert).with_context(|| format!("loading CA cert '{}'", cfg.ca_cert))?;
    let mut root_store = RootCertStore::empty();
    for cert in ca_certs {
        root_store
            .add(cert)
            .context("adding CA cert to root store")?;
    }

    let client_verifier = WebPkiClientVerifier::builder(Arc::new(root_store))
        .build()
        .context("building WebPkiClientVerifier")?;

    let certs =
        load_certs(&cfg.cert).with_context(|| format!("loading server cert '{}'", cfg.cert))?;
    let key = load_key(&cfg.key).with_context(|| format!("loading server key '{}'", cfg.key))?;

    let server_cfg = ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(certs, key)
        .context("building TLS ServerConfig")?;

    Ok(TlsAcceptor::from(Arc::new(server_cfg)))
}

/// Build a TLS connector for outbound cluster connections (mTLS – presents client cert).
pub fn build_connector(cfg: &TlsConfig) -> Result<TlsConnector> {
    let ca_certs =
        load_certs(&cfg.ca_cert).with_context(|| format!("loading CA cert '{}'", cfg.ca_cert))?;
    let mut root_store = RootCertStore::empty();
    for cert in ca_certs {
        root_store
            .add(cert)
            .context("adding CA cert to root store")?;
    }

    let certs =
        load_certs(&cfg.cert).with_context(|| format!("loading client cert '{}'", cfg.cert))?;
    let key = load_key(&cfg.key).with_context(|| format!("loading client key '{}'", cfg.key))?;

    let client_cfg = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_client_auth_cert(certs, key)
        .context("building TLS ClientConfig")?;

    Ok(TlsConnector::from(Arc::new(client_cfg)))
}

/// The DNS SAN used in all cluster TLS connections.
/// Every node and client cert must include this name.
pub fn cluster_server_name() -> ServerName<'static> {
    ServerName::try_from("ditto-cluster").expect("valid DNS name")
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
