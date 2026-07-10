use anyhow::Result;
use tracing::warn;

use crate::config::Config;

pub fn apply_replication_guardrails(config: &mut Config) {
    let interval_ms = config.replication.gossip_interval_ms.max(1);
    let min_dead_ms = interval_ms.saturating_mul(3);
    if config.replication.gossip_dead_ms < min_dead_ms {
        warn!(
            "gossip_dead_ms={}ms is lower than 3x gossip_interval_ms={}ms; clamping to {}ms",
            config.replication.gossip_dead_ms, interval_ms, min_dead_ms
        );
        config.replication.gossip_dead_ms = min_dead_ms;
    }
    if config.replication.gossip_dead_ms < 3000 {
        warn!(
            "gossip_dead_ms={}ms is aggressive and may cause false OFFLINE flapping under transient pauses",
            config.replication.gossip_dead_ms
        );
    }
    if config.replication.gossip_dead_ms > 30_000 {
        warn!(
            "gossip_dead_ms={}ms is high and may delay true failure detection",
            config.replication.gossip_dead_ms
        );
    }
}

pub fn tcp_client_auth_required(config: &Config, resolved_bind: &str, insecure: bool) -> bool {
    !insecure
        && !ditto_config::is_loopback_bind_addr(resolved_bind)
        && config.node.client_port != 0
        && config.node.client_auth_token.is_none()
}

pub fn validate_backup_encryption_policy(config: &Config, insecure: bool) -> Result<()> {
    if insecure {
        return Ok(());
    }

    let persistence_can_write_or_read_snapshots = config.backup.enabled
        || config.backup.restore_on_start
        || config.persistence.backup_allowed
        || config.persistence.import_allowed
        || config.persistence.export_allowed;
    if !persistence_can_write_or_read_snapshots {
        return Ok(());
    }

    let Some(key) = config.backup.encryption_key.as_deref() else {
        anyhow::bail!(
            "Strict security: backup.encryption_key must be configured when backup, export, or import persistence gates are enabled. Production backups/restores must be encrypted."
        );
    };

    let key_bytes = hex::decode(key.trim()).map_err(|e| {
        anyhow::anyhow!(
            "Strict security: backup.encryption_key is not valid hex: {}",
            e
        )
    })?;
    if key_bytes.len() != 32 {
        anyhow::bail!(
            "Strict security: backup.encryption_key must be 32 bytes (64 hex chars), got {} bytes.",
            key_bytes.len()
        );
    }

    Ok(())
}
