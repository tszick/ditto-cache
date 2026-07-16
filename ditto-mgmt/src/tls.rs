use crate::config::TlsConfig;
use anyhow::{Context, Result};
use axum::Router;
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer},
    ClientConfig, RootCertStore,
};
use rustls_pki_types::pem::PemObject;
use std::{fs, sync::Arc};
use tokio_rustls::TlsConnector;

/// Build a TLS connector from the given config.
/// Returns `None` when TLS is disabled.
pub fn build_connector(cfg: &TlsConfig) -> Result<Option<TlsConnector>> {
    if !cfg.enabled {
        return Ok(None);
    }

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

    Ok(Some(TlsConnector::from(Arc::new(client_cfg))))
}

/// Build a reqwest Certificate from a PEM CA file so that the HTTP client can
/// validate self-signed node certificates on port 7778.
pub fn load_reqwest_ca_cert(ca_cert_path: &str) -> Result<reqwest::Certificate> {
    let pem =
        fs::read(ca_cert_path).with_context(|| format!("reading CA cert '{}'", ca_cert_path))?;
    reqwest::Certificate::from_pem(&pem).context("parsing CA cert for reqwest")
}

/// Serve the management API over HTTPS using server-only TLS (no client cert).
///
/// Called from `main.rs` when `[server] tls_cert` and `tls_key` are set.
pub async fn serve_tls(bind: &str, app: Router, cert_path: &str, key_path: &str) -> Result<()> {
    use axum_server::tls_rustls::RustlsConfig;
    let config = RustlsConfig::from_pem_file(cert_path, key_path).await?;
    let addr: std::net::SocketAddr = bind
        .parse()
        .with_context(|| format!("parsing bind address '{}'", bind))?;
    axum_server::bind_rustls(addr, config)
        .serve(app.into_make_service())
        .await
        .context("HTTPS management server error")
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
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_test_file(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "ditto-mgmt-tls-test-{}-{}-{}",
            name,
            std::process::id(),
            nanos
        ))
    }

    #[test]
    fn build_connector_returns_none_when_tls_disabled() {
        let cfg = TlsConfig::default();
        let connector = build_connector(&cfg).expect("disabled TLS should not error");
        assert!(connector.is_none());
    }

    #[test]
    fn build_connector_reports_ca_path_context() {
        let cfg = TlsConfig {
            enabled: true,
            ca_cert: unique_test_file("missing-ca")
                .to_string_lossy()
                .into_owned(),
            cert: unique_test_file("missing-cert")
                .to_string_lossy()
                .into_owned(),
            key: unique_test_file("missing-key")
                .to_string_lossy()
                .into_owned(),
        };

        let err = match build_connector(&cfg) {
            Err(err) => err,
            Ok(_) => panic!("missing CA should fail"),
        };
        let msg = err.to_string();
        assert!(msg.contains("loading CA cert"));
        assert!(msg.contains(&cfg.ca_cert));
    }

    #[test]
    fn load_reqwest_ca_cert_reports_missing_path() {
        let missing = unique_test_file("missing-reqwest-ca");
        let err = load_reqwest_ca_cert(&missing.to_string_lossy()).expect_err("missing CA");
        assert!(err.to_string().contains("reading CA cert"));
    }

    #[test]
    fn load_certs_reports_missing_path_and_empty_pem_list() {
        let missing = unique_test_file("missing-cert");
        let err = load_certs(&missing.to_string_lossy()).expect_err("missing cert");
        assert!(err.to_string().contains("opening"));

        let invalid_cert = unique_test_file("invalid-cert");
        fs::write(&invalid_cert, b"not a certificate").expect("write invalid cert");
        let certs = load_certs(&invalid_cert.to_string_lossy()).expect("empty PEM list");
        assert!(certs.is_empty());

        fs::remove_file(invalid_cert).ok();
    }

    #[test]
    fn load_key_reports_missing_and_invalid_pem() {
        let missing = unique_test_file("missing-key");
        let err = load_key(&missing.to_string_lossy()).expect_err("missing key");
        assert!(err.to_string().contains("reading private key"));

        let invalid_key = unique_test_file("invalid-key");
        fs::write(&invalid_key, b"not a key").expect("write invalid key");
        let key_err = load_key(&invalid_key.to_string_lossy()).expect_err("invalid key");
        assert!(key_err.to_string().contains("reading private key"));
        fs::remove_file(invalid_key).ok();
    }
}
