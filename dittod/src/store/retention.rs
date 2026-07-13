use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Maximum allowed TTL for any entry, in seconds (e.g. 30 days).
pub(crate) const MAX_TTL_SECS: u64 = 30 * 24 * 60 * 60;

pub(crate) fn clamp_ttl_secs(ttl_secs: Option<u64>) -> Option<u64> {
    ttl_secs.map(|ttl| ttl.min(MAX_TTL_SECS))
}

pub(crate) fn expires_at_from_ttl_secs(
    ttl_secs: Option<u64>,
    default_ttl: Option<Duration>,
) -> Option<Instant> {
    clamp_ttl_secs(ttl_secs)
        .map(Duration::from_secs)
        .or(default_ttl)
        .map(|duration| Instant::now() + duration)
}

pub(crate) fn ttl_remaining_secs(expires_at: Option<Instant>) -> Option<u64> {
    expires_at.map(|deadline| {
        let now = Instant::now();
        if now >= deadline {
            0
        } else {
            (deadline - now).as_secs()
        }
    })
}

pub(crate) fn export_expires_at_ms(expires_at: Option<Instant>) -> Option<u64> {
    expires_at.map(|instant| {
        let now_instant = Instant::now();
        let now_system = SystemTime::now();
        let remaining = if instant > now_instant {
            instant - now_instant
        } else {
            Duration::ZERO
        };
        let expiry_sys = now_system + remaining;
        expiry_sys
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    })
}

pub(crate) fn restore_expires_at(expires_at_ms: Option<u64>) -> Option<Instant> {
    let now_sys = SystemTime::now();
    let now_inst = Instant::now();
    expires_at_ms.and_then(|ms| {
        let expiry = UNIX_EPOCH + Duration::from_millis(ms);
        expiry.duration_since(now_sys).ok().map(|duration| now_inst + duration)
    })
}

/// Minimal glob matching: supports `*` as wildcard.
pub(crate) fn glob_match(pattern: &str, input: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == input;
    }

    let mut pos = 0usize;
    for (index, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if index == 0 {
            if !input.starts_with(part) {
                return false;
            }
            pos = part.len();
        } else if index == parts.len() - 1 {
            if !input[pos..].ends_with(part) {
                return false;
            }
        } else {
            match input[pos..].find(part) {
                Some(found) => pos += found + part.len(),
                None => return false,
            }
        }
    }
    true
}
