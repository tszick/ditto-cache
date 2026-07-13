use crate::store::kv_store::ExportEntry;
use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use ditto_protocol::pb;
use prost::Message;
use rand::RngExt;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

/// Magic header identifying an encrypted backup file.
pub(crate) const MAGIC: &[u8; 4] = b"DENC";
/// Format version byte.
pub(crate) const VERSION: u8 = 0x01;
/// AES-256-GCM nonce length (96 bits).
const NONCE_LEN: usize = 12;

/// Current `pb::Snapshot.version` written by this build.
///
/// Bumped whenever the on-disk snapshot payload semantics change in a
/// non-backwards-compatible way. The decoder rejects snapshots whose
/// `version` is not in `SUPPORTED_SNAPSHOT_VERSIONS` so a stale build
/// cannot silently restore a payload it doesn't understand.
pub(crate) const SNAPSHOT_VERSION: u32 = 1;
const SUPPORTED_SNAPSHOT_VERSIONS: &[u32] = &[SNAPSHOT_VERSION];

pub(crate) fn checksum_sidecar_path(path: &Path) -> PathBuf {
    let mut sidecar = path.to_path_buf();
    let sidecar_ext = match path.extension().and_then(|e| e.to_str()) {
        Some(ext) if !ext.is_empty() => format!("{ext}.sha256"),
        _ => "sha256".to_string(),
    };
    sidecar.set_extension(sidecar_ext);
    sidecar
}

fn checksum_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

pub(crate) fn encode_snapshot(entries: &[(String, ExportEntry)]) -> Vec<u8> {
    let snapshot = pb::Snapshot {
        version: SNAPSHOT_VERSION,
        entries: entries
            .iter()
            .map(|(key, entry)| pb::SnapshotEntry {
                key: key.clone(),
                value: entry.value.clone(),
                version: entry.version,
                expires_at_ms: entry
                    .expires_at_ms
                    .map(|value| pb::OptionalUint64 { value }),
            })
            .collect(),
    };
    snapshot.encode_to_vec()
}

pub(crate) fn decode_snapshot(raw: &[u8]) -> anyhow::Result<Vec<(String, ExportEntry)>> {
    let snapshot = pb::Snapshot::decode(raw)?;
    if !SUPPORTED_SNAPSHOT_VERSIONS.contains(&snapshot.version) {
        anyhow::bail!(
            "unsupported snapshot version {} (this build supports {:?})",
            snapshot.version,
            SUPPORTED_SNAPSHOT_VERSIONS
        );
    }
    Ok(snapshot
        .entries
        .into_iter()
        .map(|entry| {
            (
                entry.key,
                ExportEntry {
                    value: entry.value,
                    version: entry.version,
                    expires_at_ms: entry.expires_at_ms.map(|value| value.value),
                },
            )
        })
        .collect())
}

pub(crate) fn write_checksum_sidecar(path: &Path, data: &[u8]) -> anyhow::Result<()> {
    let sidecar = checksum_sidecar_path(path);
    let digest = checksum_hex(data);
    std::fs::write(&sidecar, format!("{digest}\n"))?;
    Ok(())
}

pub(crate) fn verify_checksum_sidecar(path: &Path, data: &[u8]) -> anyhow::Result<()> {
    let sidecar = checksum_sidecar_path(path);
    if !sidecar.exists() {
        anyhow::bail!(
            "missing backup checksum sidecar: {}",
            sidecar.to_string_lossy()
        );
    }
    let expected = std::fs::read_to_string(&sidecar)?
        .lines()
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if expected.is_empty() {
        anyhow::bail!(
            "invalid backup checksum sidecar (empty digest): {}",
            sidecar.to_string_lossy()
        );
    }
    let actual = checksum_hex(data);
    if actual != expected {
        anyhow::bail!(
            "backup checksum mismatch for {} (expected {}, got {})",
            path.to_string_lossy(),
            expected,
            actual
        );
    }
    Ok(())
}

/// Encrypt `plaintext` with AES-256-GCM using a hex-encoded 32-byte key.
///
/// Output format:
/// ```text
/// [4 bytes magic "DENC"] [1 byte version 0x01] [12 bytes nonce] [ciphertext + 16 byte tag]
/// ```
pub(crate) fn encrypt_data(key_hex: &str, plaintext: &[u8]) -> anyhow::Result<Vec<u8>> {
    let key_bytes = hex::decode(key_hex.trim())
        .map_err(|e| anyhow::anyhow!("backup encryption key is not valid hex: {}", e))?;
    if key_bytes.len() != 32 {
        anyhow::bail!(
            "backup encryption key must be 32 bytes (64 hex chars), got {}",
            key_bytes.len()
        );
    }

    let cipher = Aes256Gcm::new_from_slice(&key_bytes)
        .map_err(|e| anyhow::anyhow!("AES-256-GCM key init failed: {}", e))?;

    let mut nonce_bytes = [0u8; NONCE_LEN];
    rand::rng().fill(&mut nonce_bytes);
    let nonce = Nonce::try_from(&nonce_bytes[..])
        .map_err(|_| anyhow::anyhow!("AES-256-GCM nonce init failed"))?;

    let ciphertext = cipher
        .encrypt(&nonce, plaintext)
        .map_err(|e| anyhow::anyhow!("AES-256-GCM encryption failed: {}", e))?;

    let mut out = Vec::with_capacity(MAGIC.len() + 1 + NONCE_LEN + ciphertext.len());
    out.extend_from_slice(MAGIC);
    out.push(VERSION);
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

/// Decrypt a backup file previously produced by [`encrypt_data`].
///
/// Returns the plaintext bytes on success.
#[allow(dead_code)]
pub(crate) fn decrypt_data(key_hex: &str, data: &[u8]) -> anyhow::Result<Vec<u8>> {
    let header_len = MAGIC.len() + 1 + NONCE_LEN;
    if data.len() < header_len {
        anyhow::bail!("data too short to be a valid encrypted backup");
    }
    if &data[..4] != MAGIC {
        anyhow::bail!("not an encrypted backup file (missing DENC magic header)");
    }
    if data[4] != VERSION {
        anyhow::bail!("unsupported encrypted backup version: {}", data[4]);
    }

    let key_bytes = hex::decode(key_hex.trim())
        .map_err(|e| anyhow::anyhow!("backup encryption key is not valid hex: {}", e))?;
    if key_bytes.len() != 32 {
        anyhow::bail!(
            "backup encryption key must be 32 bytes (64 hex chars), got {}",
            key_bytes.len()
        );
    }

    let cipher = Aes256Gcm::new_from_slice(&key_bytes)
        .map_err(|e| anyhow::anyhow!("AES-256-GCM key init failed: {}", e))?;

    let nonce = Nonce::try_from(&data[5..5 + NONCE_LEN])
        .map_err(|_| anyhow::anyhow!("AES-256-GCM nonce init failed"))?;
    let plaintext = cipher
        .decrypt(&nonce, &data[header_len..])
        .map_err(|_| anyhow::anyhow!("AES-256-GCM decryption failed: wrong key or corrupt data"))?;

    Ok(plaintext)
}
