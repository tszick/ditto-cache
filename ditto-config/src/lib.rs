//! Shared configuration utilities for the Ditto distributed cache.
//!
//! Currently provides [`resolve_bind_addr`], which translates the symbolic
//! bind-address values used in config files into concrete IP strings.

use anyhow::{anyhow, Result};
use std::net::IpAddr;

/// Resolve a bind-address string to a concrete IPv4/IPv6 string.
///
/// Supported values:
/// - `"site-local"` — first private IPv4 interface found on the host
///   (10.x.x.x, 172.16–31.x.x, 192.168.x.x).  Designed for clusters where
///   every node should communicate over its private network interface without
///   having to hard-code IP addresses.
/// - `"localhost"` — loopback (127.0.0.1).
/// - `"0.0.0.0"` — all interfaces (pass-through).
/// - Any explicit IPv4/IPv6 address — returned as-is.
///
/// # Errors
///
/// Returns an error when `"site-local"` is requested but no private IPv4
/// interface can be found on the host (e.g. a cloud VM with only a public IP).
/// The error message contains a clear remediation hint.
pub fn resolve_bind_addr(addr: &str) -> Result<String> {
    match addr.trim() {
        "site-local" => {
            let ifaces = if_addrs::get_if_addrs()
                .map_err(|e| anyhow!("failed to enumerate network interfaces: {e}"))?;

            ifaces
                .iter()
                .filter_map(|iface| match iface.addr.ip() {
                    IpAddr::V4(v4) if is_private_v4(v4) => Some(v4.to_string()),
                    _ => None,
                })
                .next()
                .ok_or_else(|| {
                    anyhow!(
                        "bind_addr = \"site-local\" but no private IPv4 interface was found.\n\
                         Set bind_addr to an explicit IP, \"0.0.0.0\", or \"localhost\"."
                    )
                })
        }
        other => Ok(other.to_string()),
    }
}

/// Returns true when the given bind address is loopback-only.
///
/// Intended for production guardrails that need to distinguish between
/// local-dev exposure (`localhost`, `127.0.0.1`, `::1`) and non-loopback
/// client exposure (`0.0.0.0`, private IPs, public IPs).
pub fn is_loopback_bind_addr(addr: &str) -> bool {
    let trimmed = addr.trim();
    if trimmed.eq_ignore_ascii_case("localhost") {
        return true;
    }
    trimmed
        .parse::<IpAddr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(false)
}

fn is_private_v4(ip: std::net::Ipv4Addr) -> bool {
    let o = ip.octets();
    matches!(o, [10, ..] | [172, 16..=31, ..] | [192, 168, ..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_ip_passthrough() {
        assert_eq!(resolve_bind_addr("192.168.1.5").unwrap(), "192.168.1.5");
        assert_eq!(resolve_bind_addr("0.0.0.0").unwrap(), "0.0.0.0");
        assert_eq!(resolve_bind_addr("127.0.0.1").unwrap(), "127.0.0.1");
        assert_eq!(resolve_bind_addr("localhost").unwrap(), "localhost");
    }

    #[test]
    fn is_private_v4_ranges() {
        use std::net::Ipv4Addr;
        assert!(is_private_v4(Ipv4Addr::new(10, 0, 0, 1)));
        assert!(is_private_v4(Ipv4Addr::new(172, 16, 0, 1)));
        assert!(is_private_v4(Ipv4Addr::new(172, 31, 255, 255)));
        assert!(is_private_v4(Ipv4Addr::new(192, 168, 1, 100)));
        assert!(!is_private_v4(Ipv4Addr::new(172, 15, 0, 1)));
        assert!(!is_private_v4(Ipv4Addr::new(8, 8, 8, 8)));
    }

    #[test]
    fn loopback_bind_detection() {
        assert!(is_loopback_bind_addr("localhost"));
        assert!(is_loopback_bind_addr("127.0.0.1"));
        assert!(is_loopback_bind_addr("::1"));
        assert!(!is_loopback_bind_addr("0.0.0.0"));
        assert!(!is_loopback_bind_addr("192.168.1.10"));
        assert!(!is_loopback_bind_addr("site-local"));
    }
}
