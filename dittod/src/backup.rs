use crate::{config::BackupConfig, node::NodeHandle, store::kv_store::ExportEntry};
use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use ditto_protocol::{pb, NodeStatus};
use prost::Message;
use rand::RngExt;
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{atomic::Ordering, Arc},
    time::{Duration, SystemTime},
};

/// Magic header identifying an encrypted backup file.
const MAGIC: &[u8; 4] = b"DENC";
/// Format version byte.
const VERSION: u8 = 0x01;
/// AES-256-GCM nonce length (96 bits).
const NONCE_LEN: usize = 12;

/// Current `pb::Snapshot.version` written by this build.
///
/// Bumped whenever the on-disk snapshot payload semantics change in a
/// non-backwards-compatible way. The decoder rejects snapshots whose
/// `version` is not in `SUPPORTED_SNAPSHOT_VERSIONS` so a stale build
/// cannot silently restore a payload it doesn't understand.
const SNAPSHOT_VERSION: u32 = 1;
const SUPPORTED_SNAPSHOT_VERSIONS: &[u32] = &[SNAPSHOT_VERSION];

pub struct SnapshotLoadResult {
    pub path: String,
    pub entries: usize,
    pub duration_ms: u64,
}

fn checksum_sidecar_path(path: &Path) -> PathBuf {
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

fn encode_snapshot(entries: &[(String, ExportEntry)]) -> Vec<u8> {
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

fn decode_snapshot(raw: &[u8]) -> anyhow::Result<Vec<(String, ExportEntry)>> {
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

fn write_checksum_sidecar(path: &Path, data: &[u8]) -> anyhow::Result<()> {
    let sidecar = checksum_sidecar_path(path);
    let digest = checksum_hex(data);
    fs::write(&sidecar, format!("{digest}\n"))?;
    Ok(())
}

fn verify_checksum_sidecar(path: &Path, data: &[u8]) -> anyhow::Result<()> {
    let sidecar = checksum_sidecar_path(path);
    if !sidecar.exists() {
        anyhow::bail!(
            "missing backup checksum sidecar: {}",
            sidecar.to_string_lossy()
        );
    }
    let expected = fs::read_to_string(&sidecar)?
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

/// Run a single backup cycle:
///   1. Inactivate the node (if currently Active)
///   2. Serialize the store snapshot to a timestamped file
///   3. Sync from primary and reactivate (if we inactivated it)
///   4. Rotate old backup files beyond retain_days
///
/// Returns the path of the written backup file.
pub async fn run_backup(node: Arc<NodeHandle>, cfg: &BackupConfig) -> anyhow::Result<String> {
    {
        let guard = node.config.lock().unwrap();
        let p = &guard.persistence;
        let backup_enabled = p.platform_allowed && p.runtime_enabled && p.backup_allowed;
        if !backup_enabled {
            anyhow::bail!(
                "backup is disabled by persistence policy (require platform + runtime + backup feature enablement)"
            );
        }
    }

    // --- 1. Inactivate (save prior state so we can restore it) ---
    let was_active = node.active.load(Ordering::Relaxed);
    if was_active {
        node.active.store(false, Ordering::Relaxed);
        node.active_set
            .lock()
            .await
            .set_local_status(NodeStatus::Inactive);
        tracing::info!("Backup: node set Inactive for snapshot");
    }

    // --- 2. Export ---
    let result = write_snapshot(&node, cfg).await;

    // --- 3. Sync from primary and reactivate ---
    if was_active {
        tracing::info!("Backup: snapshot complete, syncing from primary before reactivation");
        node.clone().run_resync().await;
        // run_resync sets active=true and status=Active on success,
        // or active=false and status=Inactive on failure.
    }

    let path = result?;

    // --- 4. Rotation ---
    let node_id = node.config.lock().unwrap().node.id.clone();
    rotate_old_backups(&cfg.path, &node_id, cfg.retain_days);

    Ok(path)
}

/// Load the newest snapshot file for this node from backup path and restore it
/// into the in-memory store. Returns `Ok(None)` when no snapshot exists.
pub fn restore_latest_snapshot(
    node: &NodeHandle,
    cfg: &BackupConfig,
) -> anyhow::Result<Option<SnapshotLoadResult>> {
    let started = std::time::Instant::now();
    let node_id = node.config.lock().unwrap().node.id.clone();
    let dir = PathBuf::from(&cfg.path);
    if !dir.exists() {
        return Ok(None);
    }

    let mut newest: Option<(PathBuf, SystemTime)> = None;
    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(v) => v,
            None => continue,
        };
        if !name.starts_with(&format!("{}_backup_", node_id)) {
            continue;
        }
        if name.ends_with(".sha256") {
            continue;
        }
        let modified = entry
            .metadata()
            .and_then(|m| m.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH);
        match &newest {
            Some((_, ts)) if *ts >= modified => {}
            _ => newest = Some((path.clone(), modified)),
        }
    }

    let Some((path, _)) = newest else {
        return Ok(None);
    };
    let filename = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow::anyhow!("snapshot filename is not valid UTF-8"))?
        .to_string();
    if cfg.max_snapshot_bytes > 0 {
        let file_len = fs::metadata(&path)?.len();
        if file_len > cfg.max_snapshot_bytes {
            anyhow::bail!(
                "snapshot file {} exceeds backup.max_snapshot_bytes ({} > {})",
                path.to_string_lossy(),
                file_len,
                cfg.max_snapshot_bytes
            );
        }
    }
    let data = fs::read(&path)?;
    verify_checksum_sidecar(&path, &data)?;

    let raw = if filename.ends_with(".enc") {
        let key = cfg.encryption_key.as_ref().ok_or_else(|| {
            anyhow::anyhow!("encrypted snapshot found but backup.encryption_key is missing")
        })?;
        decrypt_data(key, &data)?
    } else {
        data
    };
    if cfg.max_snapshot_bytes > 0 && raw.len() as u64 > cfg.max_snapshot_bytes {
        anyhow::bail!(
            "snapshot payload exceeds backup.max_snapshot_bytes ({} > {})",
            raw.len(),
            cfg.max_snapshot_bytes
        );
    }

    let entries: Vec<(String, ExportEntry)> = if filename.contains(".json") {
        serde_json::from_slice(&raw)?
    } else {
        decode_snapshot(&raw)?
    };
    if cfg.max_restore_entries > 0 && entries.len() > cfg.max_restore_entries {
        anyhow::bail!(
            "snapshot entry count exceeds backup.max_restore_entries ({} > {})",
            entries.len(),
            cfg.max_restore_entries
        );
    }
    let count = entries.len();
    node.store.restore(entries);

    Ok(Some(SnapshotLoadResult {
        path: path.to_string_lossy().into_owned(),
        entries: count,
        duration_ms: started.elapsed().as_millis() as u64,
    }))
}

async fn write_snapshot(node: &NodeHandle, cfg: &BackupConfig) -> anyhow::Result<String> {
    let snapshot = node.store.snapshot();

    let is_json = cfg.format.trim().eq_ignore_ascii_case("json");
    let (plaintext, base_ext) = if is_json {
        (serde_json::to_vec(&snapshot)?, "json")
    } else {
        (encode_snapshot(&snapshot), "pb")
    };

    let (data, ext) = match &cfg.encryption_key {
        Some(key_hex) => {
            let ciphertext = encrypt_data(key_hex, &plaintext)?;
            (ciphertext, format!("{}.enc", base_ext))
        }
        None => (plaintext, base_ext.to_string()),
    };

    let node_id = node.config.lock().unwrap().node.id.clone();
    let now = chrono::Utc::now();
    let filename = format!(
        "{}_backup_{}_UTC.{}",
        node_id,
        now.format("%Y.%m.%d_%H-%M-%S"),
        ext,
    );

    let dir = PathBuf::from(&cfg.path);
    fs::create_dir_all(&dir)?;
    let full_path = dir.join(&filename);
    fs::write(&full_path, &data)?;
    write_checksum_sidecar(&full_path, &data)?;

    tracing::info!(
        "Backup written: {:?}  ({} bytes{}; checksum sidecar written)",
        full_path,
        data.len(),
        if cfg.encryption_key.is_some() {
            ", encrypted"
        } else {
            ""
        },
    );
    Ok(full_path.to_string_lossy().into_owned())
}

/// Encrypt `plaintext` with AES-256-GCM using a hex-encoded 32-byte key.
///
/// Output format:
/// ```text
/// [4 bytes magic "DENC"] [1 byte version 0x01] [12 bytes nonce] [ciphertext + 16 byte tag]
/// ```
fn encrypt_data(key_hex: &str, plaintext: &[u8]) -> anyhow::Result<Vec<u8>> {
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
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, plaintext)
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
pub fn decrypt_data(key_hex: &str, data: &[u8]) -> anyhow::Result<Vec<u8>> {
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

    let nonce = Nonce::from_slice(&data[5..5 + NONCE_LEN]);
    let plaintext = cipher
        .decrypt(nonce, &data[header_len..])
        .map_err(|_| anyhow::anyhow!("AES-256-GCM decryption failed: wrong key or corrupt data"))?;

    Ok(plaintext)
}

fn rotate_old_backups(path: &str, node_id: &str, retain_days: u64) {
    if retain_days == 0 {
        return;
    }
    let cutoff = SystemTime::now()
        .checked_sub(Duration::from_secs(retain_days * 86_400))
        .unwrap_or(SystemTime::UNIX_EPOCH);

    let dir = PathBuf::from(path);
    let entries = match fs::read_dir(&dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.starts_with(node_id) || !name.contains("_backup_") {
            continue; // also matches .bin.enc and .json.enc by the prefix/infix check
        }
        if let Ok(meta) = entry.metadata() {
            if let Ok(modified) = meta.modified() {
                if modified < cutoff && fs::remove_file(entry.path()).is_ok() {
                    tracing::info!("Backup rotation: removed {:?}", entry.path());
                }
            }
        }
    }
}

/// Long-running task: fires a backup on the cron schedule.
/// Keeps running until the process exits.
pub async fn run_scheduler(node: Arc<NodeHandle>, cfg: BackupConfig) {
    use cronexpr::jiff::Timestamp;
    use cronexpr::{parse_crontab_with, FallbackTimezoneOption, ParseOptions};

    if !cfg.enabled {
        tracing::info!("Backup scheduler disabled (enabled = false in config).");
        return;
    }

    let mut parse_options = ParseOptions::default();
    parse_options.fallback_timezone_option = FallbackTimezoneOption::UTC;

    let schedule = match parse_crontab_with(&cfg.schedule, parse_options) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(
                "Invalid backup schedule '{}': {}. Scheduler will not run.",
                cfg.schedule,
                e
            );
            return;
        }
    };

    tracing::info!("Backup scheduler started (schedule: '{}')", cfg.schedule);

    loop {
        let now = Timestamp::now();
        let next = match schedule.find_next(now) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(
                    "Backup schedule has no future occurrences ({}). Stopping.",
                    e
                );
                break;
            }
        };

        let now_secs = now.as_second();
        let next_secs = next.timestamp().as_second();
        let delay_secs = if next_secs > now_secs {
            (next_secs - now_secs) as u64
        } else {
            1
        };
        let delay = Duration::from_secs(delay_secs);

        tracing::info!("Next scheduled backup at {} (in {:?})", next, delay);
        tokio::time::sleep(delay).await;

        let backup_enabled = {
            let guard = node.config.lock().unwrap();
            let p = &guard.persistence;
            p.platform_allowed && p.runtime_enabled && p.backup_allowed
        };
        if !backup_enabled {
            tracing::info!("Scheduled backup skipped: persistence backup gate is disabled.");
            continue;
        }

        if let Err(e) = run_backup(node.clone(), &cfg).await {
            tracing::error!("Scheduled backup failed: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::Config, store::KvStore};
    use bytes::Bytes;

    fn sample_entries() -> Vec<(String, ExportEntry)> {
        vec![
            (
                "alpha".to_string(),
                ExportEntry {
                    value: b"hello".to_vec(),
                    version: 7,
                    expires_at_ms: Some(1_700_000_000_000),
                },
            ),
            (
                "beta".to_string(),
                ExportEntry {
                    value: b"world".to_vec(),
                    version: 11,
                    expires_at_ms: None,
                },
            ),
        ]
    }

    fn test_node(mut cfg: Config) -> Arc<NodeHandle> {
        cfg.node.id = "backup-node".into();
        let store = Arc::new(KvStore::new(
            cfg.cache.max_memory_mb,
            cfg.cache.default_ttl_secs,
            cfg.cache.value_size_limit_bytes,
            cfg.cache.max_keys,
            cfg.compression.enabled,
            cfg.compression.threshold_bytes,
        ));
        NodeHandle::new(
            cfg,
            "backup-node.toml".into(),
            store,
            None,
            "127.0.0.1".into(),
        )
    }

    fn backup_enabled_config(path: String) -> Config {
        let mut cfg = Config::default();
        cfg.node.active = false;
        cfg.backup.path = path;
        cfg.persistence.platform_allowed = true;
        cfg.persistence.runtime_enabled = true;
        cfg.persistence.backup_allowed = true;
        cfg
    }

    fn checksum_path_for(path: &str) -> PathBuf {
        checksum_sidecar_path(Path::new(path))
    }

    #[test]
    fn snapshot_encode_decode_round_trip() {
        let original = sample_entries();
        let encoded = encode_snapshot(&original);
        let decoded = decode_snapshot(&encoded).expect("decode current-version snapshot");
        assert_eq!(decoded.len(), original.len());
        for (got, want) in decoded.iter().zip(original.iter()) {
            assert_eq!(got.0, want.0);
            assert_eq!(got.1.value, want.1.value);
            assert_eq!(got.1.version, want.1.version);
            assert_eq!(got.1.expires_at_ms, want.1.expires_at_ms);
        }
    }

    #[test]
    fn snapshot_writer_stamps_current_version() {
        let encoded = encode_snapshot(&sample_entries());
        let snapshot = pb::Snapshot::decode(encoded.as_slice()).expect("re-decode raw protobuf");
        assert_eq!(snapshot.version, SNAPSHOT_VERSION);
    }

    #[test]
    fn snapshot_decoder_rejects_future_version() {
        let mut snapshot = pb::Snapshot {
            version: SNAPSHOT_VERSION + 1,
            entries: vec![],
        };
        snapshot.entries.push(pb::SnapshotEntry {
            key: "k".into(),
            value: b"v".to_vec(),
            version: 1,
            expires_at_ms: None,
        });
        let raw = snapshot.encode_to_vec();
        let err = decode_snapshot(&raw).expect_err("future-version snapshot must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("unsupported snapshot version"),
            "unexpected error message: {msg}"
        );
    }

    #[test]
    fn snapshot_decoder_rejects_unversioned_legacy_payload() {
        // Pre-versioning snapshots had no `version` field; with the new schema
        // the field defaults to 0, which is not in SUPPORTED_SNAPSHOT_VERSIONS.
        // The decoder must reject these rather than silently restoring them.
        let legacy = pb::Snapshot {
            version: 0,
            entries: vec![pb::SnapshotEntry {
                key: "k".into(),
                value: b"v".to_vec(),
                version: 1,
                expires_at_ms: None,
            }],
        };
        let raw = legacy.encode_to_vec();
        let err = decode_snapshot(&raw).expect_err("legacy unversioned snapshot must be rejected");
        assert!(err.to_string().contains("unsupported snapshot version"));
    }

    #[test]
    fn checksum_sidecar_paths_and_verification_cover_success_and_failures() {
        let dir = unique_test_dir("checksum");
        fs::create_dir_all(&dir).expect("create temp dir");
        let snapshot = dir.join("node_backup.pb");
        let data = b"snapshot bytes";
        fs::write(&snapshot, data).expect("write snapshot");

        assert_eq!(
            checksum_sidecar_path(&snapshot).file_name().unwrap(),
            "node_backup.pb.sha256"
        );
        assert_eq!(
            checksum_sidecar_path(&dir.join("snapshot"))
                .file_name()
                .unwrap(),
            "snapshot.sha256"
        );

        let missing = verify_checksum_sidecar(&snapshot, data).expect_err("missing sidecar");
        assert!(missing
            .to_string()
            .contains("missing backup checksum sidecar"));

        write_checksum_sidecar(&snapshot, data).expect("write checksum sidecar");
        verify_checksum_sidecar(&snapshot, data).expect("matching checksum should verify");

        fs::write(checksum_sidecar_path(&snapshot), "\n").expect("empty sidecar");
        let empty = verify_checksum_sidecar(&snapshot, data).expect_err("empty digest");
        assert!(empty
            .to_string()
            .contains("invalid backup checksum sidecar"));

        fs::write(checksum_sidecar_path(&snapshot), "deadbeef\n").expect("bad digest");
        let mismatch = verify_checksum_sidecar(&snapshot, data).expect_err("checksum mismatch");
        assert!(mismatch.to_string().contains("backup checksum mismatch"));

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn snapshot_decoder_rejects_malformed_protobuf() {
        let err = decode_snapshot(b"not protobuf").expect_err("malformed protobuf");
        assert!(
            err.to_string()
                .contains("failed to decode Protobuf message"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn encryption_round_trip_and_rejects_bad_inputs() {
        let key = "00".repeat(32);
        let wrong_key = "11".repeat(32);
        let plaintext = b"portable backup payload";

        let encrypted = encrypt_data(&key, plaintext).expect("encrypt");
        assert!(encrypted.starts_with(MAGIC));
        assert_eq!(decrypt_data(&key, &encrypted).expect("decrypt"), plaintext);

        let err = encrypt_data("not-hex", plaintext).expect_err("invalid hex");
        assert!(err.to_string().contains("not valid hex"));

        let err = encrypt_data("00", plaintext).expect_err("short key");
        assert!(err.to_string().contains("must be 32 bytes"));

        let err = decrypt_data(&key, b"tiny").expect_err("short encrypted data");
        assert!(err.to_string().contains("data too short"));

        let mut bad_magic = encrypted.clone();
        bad_magic[0] = b'X';
        let err = decrypt_data(&key, &bad_magic).expect_err("bad magic");
        assert!(err.to_string().contains("missing DENC magic header"));

        let mut bad_version = encrypted.clone();
        bad_version[4] = VERSION + 1;
        let err = decrypt_data(&key, &bad_version).expect_err("bad version");
        assert!(err
            .to_string()
            .contains("unsupported encrypted backup version"));

        let err = decrypt_data(&wrong_key, &encrypted).expect_err("wrong key");
        assert!(err.to_string().contains("AES-256-GCM decryption failed"));
    }

    #[tokio::test]
    async fn run_backup_rejects_disabled_persistence_gate() {
        let dir = unique_test_dir("gate");
        let mut cfg = Config::default();
        cfg.node.active = false;
        cfg.backup.path = dir.to_string_lossy().into_owned();
        let node = test_node(cfg.clone());

        let err = run_backup(node, &cfg.backup)
            .await
            .expect_err("backup gate should reject disabled persistence");
        assert!(err.to_string().contains("backup is disabled"));

        fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn run_backup_writes_plain_protobuf_snapshot_and_checksum() {
        let dir = unique_test_dir("plain-pb");
        let mut cfg = backup_enabled_config(dir.to_string_lossy().into_owned());
        cfg.backup.format = "protobuf".into();
        cfg.backup.retain_days = 1;
        let node = test_node(cfg.clone());
        node.store.set(
            "plain-key".into(),
            Bytes::from_static(b"plain-value"),
            42,
            None,
        );

        let path = run_backup(Arc::clone(&node), &cfg.backup)
            .await
            .expect("backup should write protobuf snapshot");

        assert!(path.ends_with(".pb"));
        assert!(Path::new(&path).exists());
        assert!(checksum_path_for(&path).exists());
        assert!(!node.active.load(Ordering::Relaxed));

        let data = fs::read(&path).expect("read protobuf snapshot");
        verify_checksum_sidecar(Path::new(&path), &data).expect("checksum verifies");
        let entries = decode_snapshot(&data).expect("decode protobuf snapshot");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "plain-key");
        assert_eq!(entries[0].1.value, b"plain-value");
        assert_eq!(entries[0].1.version, 42);

        fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn run_backup_writes_encrypted_json_snapshot_and_can_restore_it() {
        let dir = unique_test_dir("encrypted-json");
        let key = "7b".repeat(32);
        let mut cfg = backup_enabled_config(dir.to_string_lossy().into_owned());
        cfg.backup.format = "json".into();
        cfg.backup.encryption_key = Some(key.clone());
        let node = test_node(cfg.clone());
        node.store.set(
            "encrypted-key".into(),
            Bytes::from_static(b"encrypted-value"),
            77,
            None,
        );

        let path = run_backup(Arc::clone(&node), &cfg.backup)
            .await
            .expect("backup should write encrypted json snapshot");

        assert!(path.ends_with(".json.enc"));
        assert!(checksum_path_for(&path).exists());
        let encrypted = fs::read(&path).expect("read encrypted snapshot");
        verify_checksum_sidecar(Path::new(&path), &encrypted).expect("checksum verifies");
        let plaintext = decrypt_data(&key, &encrypted).expect("decrypt snapshot");
        let entries: Vec<(String, ExportEntry)> =
            serde_json::from_slice(&plaintext).expect("decode json snapshot");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "encrypted-key");
        assert_eq!(entries[0].1.value, b"encrypted-value");
        assert_eq!(entries[0].1.version, 77);

        let mut missing_key_cfg = cfg.backup.clone();
        missing_key_cfg.encryption_key = None;
        let err = match restore_latest_snapshot(&node, &missing_key_cfg) {
            Err(err) => err,
            Ok(_) => panic!("encrypted restore should require a key"),
        };
        assert!(err.to_string().contains("encryption_key is missing"));

        node.store.flush();
        let restored = restore_latest_snapshot(&node, &cfg.backup)
            .expect("encrypted restore should succeed")
            .expect("snapshot should exist");
        assert_eq!(restored.entries, 1);
        assert_eq!(
            node.store.get("encrypted-key").expect("restored key").value,
            Bytes::from_static(b"encrypted-value")
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn run_scheduler_returns_for_disabled_or_invalid_schedule() {
        let dir = unique_test_dir("scheduler");
        let cfg = backup_enabled_config(dir.to_string_lossy().into_owned());
        let node = test_node(cfg.clone());

        let mut disabled = cfg.backup.clone();
        disabled.enabled = false;
        run_scheduler(Arc::clone(&node), disabled).await;

        let mut invalid = cfg.backup.clone();
        invalid.enabled = true;
        invalid.schedule = "not a cron schedule".into();
        run_scheduler(node, invalid).await;

        fs::remove_dir_all(&dir).ok();
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("ditto-backup-test-{name}-{nanos}"))
    }
}
