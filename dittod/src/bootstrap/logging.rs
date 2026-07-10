use anyhow::Result;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

use crate::config::LogConfig;

/// Initialise the tracing subscriber with an optional rolling-file layer.
///
/// Always installs a console (stderr) layer. When `cfg.enabled = true` a
/// second layer writes to a rolling file in `cfg.path`.
///
/// The returned `WorkerGuard` must be kept alive for the entire lifetime of
/// `main()`. Dropping it early causes the async writer thread to shut down and
/// queued records to be lost.
pub fn init_logging(cfg: &LogConfig) -> Result<tracing_appender::non_blocking::WorkerGuard> {
    let level_str = std::env::var("RUST_LOG").unwrap_or_else(|_| format!("dittod={}", cfg.level));
    let filter = EnvFilter::try_new(&level_str).unwrap_or_else(|_| EnvFilter::new("dittod=info"));

    let console = tracing_subscriber::fmt::layer()
        .with_target(false)
        .with_filter(filter.clone());

    let registry = tracing_subscriber::registry().with(console);

    if cfg.enabled {
        std::fs::create_dir_all(&cfg.path)
            .map_err(|e| anyhow::anyhow!("cannot create log directory {:?}: {}", cfg.path, e))?;

        let rotation = match cfg.rotation.as_str() {
            "hourly" => tracing_appender::rolling::Rotation::HOURLY,
            "never" => tracing_appender::rolling::Rotation::NEVER,
            _ => tracing_appender::rolling::Rotation::DAILY,
        };
        let appender =
            tracing_appender::rolling::RollingFileAppender::new(rotation, &cfg.path, "dittod.log");
        let (non_blocking, guard) = tracing_appender::non_blocking(appender);
        registry
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(non_blocking)
                    .with_ansi(false)
                    .with_filter(filter),
            )
            .init();
        Ok(guard)
    } else {
        let (_, guard) = tracing_appender::non_blocking(std::io::sink());
        registry.init();
        Ok(guard)
    }
}

/// Delete rolling log files older than `retain_days` days from `dir`.
///
/// Only files whose names start with `"dittod.log"` are examined so that
/// no other files in the directory are accidentally removed.
pub fn rotate_old_logs(dir: &str, retain_days: u64) {
    let cutoff = std::time::SystemTime::now()
        .checked_sub(std::time::Duration::from_secs(retain_days * 86_400))
        .unwrap_or(std::time::SystemTime::UNIX_EPOCH);

    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.starts_with("dittod.log"))
            .unwrap_or(false)
        {
            continue;
        }
        if let Ok(meta) = std::fs::metadata(&path) {
            if meta.modified().map(|t| t < cutoff).unwrap_or(false) {
                if let Err(e) = std::fs::remove_file(&path) {
                    tracing::warn!("log cleanup: could not remove {:?}: {}", path, e);
                }
            }
        }
    }
}
