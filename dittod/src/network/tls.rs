use crate::config::TlsConfig;
use anyhow::{Context, Result};
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
    server::WebPkiClientVerifier,
    ClientConfig, RootCertStore, ServerConfig,
};
use rustls_pki_types::pem::PemObject;
use std::sync::Arc;
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

pub fn build_server_only_acceptor(cfg: &TlsConfig) -> Result<TlsAcceptor> {
    let certs =
        load_certs(&cfg.cert).with_context(|| format!("loading server cert '{}'", cfg.cert))?;
    let key = load_key(&cfg.key).with_context(|| format!("loading server key '{}'", cfg.key))?;

    let server_cfg = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("building server-only TLS ServerConfig")?;

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
    CertificateDer::pem_file_iter(path)
        .with_context(|| format!("opening '{}'", path))?
        .collect::<Result<Vec<_>, _>>()
        .context("reading PEM certificates")
}

fn load_key(path: &str) -> Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_file(path).context("reading private key")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn unique_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("ditto-tls-test-{name}-{nanos}.pem"))
    }

    fn write_temp(name: &str, contents: &str) -> PathBuf {
        let path = unique_path(name);
        fs::write(&path, contents).unwrap();
        path
    }

    #[test]
    fn cluster_server_name_is_expected_dns_name() {
        let expected = ServerName::try_from("ditto-cluster").unwrap();
        assert_eq!(cluster_server_name(), expected);
    }

    #[test]
    fn load_certs_reports_missing_and_invalid_pem() {
        let missing = unique_path("missing-cert");
        let err = load_certs(missing.to_str().unwrap()).unwrap_err();
        assert!(err.to_string().contains("opening"));

        let invalid = write_temp(
            "invalid-cert",
            "-----BEGIN CERTIFICATE-----\nnot-base64\n-----END CERTIFICATE-----\n",
        );
        let err = load_certs(invalid.to_str().unwrap()).unwrap_err();
        assert!(err.to_string().contains("reading PEM certificates"));
        fs::remove_file(invalid).unwrap();
    }

    #[test]
    fn load_key_reports_missing_and_invalid_pem() {
        let missing = unique_path("missing-key");
        let err = load_key(missing.to_str().unwrap()).unwrap_err();
        assert!(err.to_string().contains("reading private key"));

        let invalid = write_temp("invalid-key", "not a pem");
        let err = load_key(invalid.to_str().unwrap()).unwrap_err();
        assert!(err.to_string().contains("reading private key"));
        fs::remove_file(invalid).unwrap();
    }

    #[test]
    fn build_acceptor_and_connector_include_path_context_on_ca_failure() {
        let cfg = TlsConfig {
            enabled: true,
            ca_cert: unique_path("missing-ca").to_string_lossy().into_owned(),
            cert: "missing-cert.pem".into(),
            key: "missing-key.pem".into(),
        };

        let acceptor_err = match build_acceptor(&cfg) {
            Ok(_) => panic!("acceptor build unexpectedly succeeded"),
            Err(err) => err.to_string(),
        };
        assert!(acceptor_err.contains("loading CA cert"));
        assert!(acceptor_err.contains(&cfg.ca_cert));

        let connector_err = match build_connector(&cfg) {
            Ok(_) => panic!("connector build unexpectedly succeeded"),
            Err(err) => err.to_string(),
        };
        assert!(connector_err.contains("loading CA cert"));
        assert!(connector_err.contains(&cfg.ca_cert));
    }

    #[test]
    fn build_server_only_acceptor_includes_cert_path_context() {
        let cfg = TlsConfig {
            enabled: true,
            ca_cert: "unused-ca.pem".into(),
            cert: unique_path("missing-server-cert").to_string_lossy().into_owned(),
            key: "missing-server-key.pem".into(),
        };

        let err = match build_server_only_acceptor(&cfg) {
            Ok(_) => panic!("server-only acceptor build unexpectedly succeeded"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains("loading server cert"));
        assert!(err.contains(&cfg.cert));
    }
}
