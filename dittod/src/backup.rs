use crate::{config::BackupConfig, node::NodeHandle, store::kv_store::ExportEntry};
use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use ditto_protocol::NodeStatus;
use rand::RngCore;
use std::{
    fs,
    path::PathBuf,
    sync::{atomic::Ordering, Arc},
    time::{Duration, SystemTime},
};

/// Magic header identifying an encrypted backup file.
const MAGIC: &[u8; 4] = b"DENC";
/// Format version byte.
const VERSION: u8 = 0x01;
/// AES-256-GCM nonce length (96 bits).
const NONCE_LEN: usize = 12;

pub struct SnapshotLoadResult {
    pub path: String,
    pub entries: usize,
    pub duration_ms: u64,
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
    let result = write_snapshot(&*node, cfg).await;

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
    let data = fs::read(&path)?;
    let raw = if filename.ends_with(".enc") {
        let key = cfg.encryption_key.as_ref().ok_or_else(|| {
            anyhow::anyhow!("encrypted snapshot found but backup.encryption_key is missing")
        })?;
        decrypt_data(key, &data)?
    } else {
        data
    };

    let entries: Vec<(String, ExportEntry)> = if filename.contains(".json") {
        serde_json::from_slice(&raw)?
    } else {
        bincode::deserialize(&raw)?
    };
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
        (bincode::serialize(&snapshot)?, "bin")
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

    tracing::info!(
        "Backup written: {:?}  ({} bytes{})",
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
    rand::thread_rng().fill_bytes(&mut nonce_bytes);
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
                if modified < cutoff {
                    if fs::remove_file(entry.path()).is_ok() {
                        tracing::info!("Backup rotation: removed {:?}", entry.path());
                    }
                }
            }
        }
    }
}

/// Long-running task: fires a backup on the cron schedule.
/// Keeps running until the process exits.
pub async fn run_scheduler(node: Arc<NodeHandle>, cfg: BackupConfig) {
    use cron::Schedule;
    use std::str::FromStr;

    if !cfg.enabled {
        tracing::info!("Backup scheduler disabled (enabled = false in config).");
        return;
    }

    let schedule = match Schedule::from_str(&cfg.schedule) {
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
        let next = match schedule.upcoming(chrono::Utc).next() {
            Some(t) => t,
            None => {
                tracing::warn!("Backup schedule has no future occurrences. Stopping.");
                break;
            }
        };

        let delay = (next - chrono::Utc::now())
            .to_std()
            .unwrap_or(Duration::from_secs(60));

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
