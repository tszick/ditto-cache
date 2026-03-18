use anyhow::{Context, Result};
use ditto_protocol::{AdminRequest, AdminResponse, ClusterMessage, encode, decode};
use rustls::pki_types::ServerName;
use std::{net::SocketAddr, time::Duration};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

/// Maximum time allowed for establishing a TCP connection to a dittod node.
/// When a Docker container stops its iptables rules are removed and new connection
/// attempts no longer receive an immediate RST — without a timeout they would block
/// for the OS-level TCP connect timeout (~20–75 s).  2 s is well below the 3-second
/// `reqwest` HTTP client timeout used elsewhere and keeps the UI responsive.
const CONNECT_TIMEOUT: Duration = Duration::from_millis(500);

// ---------------------------------------------------------------------------
// Admin RPC (cluster port 7779)
// ---------------------------------------------------------------------------

/// Send an AdminRequest to a node on its cluster port; receive AdminResponse.
pub async fn admin_rpc(
    addr: SocketAddr,
    req: AdminRequest,
    tls: Option<&TlsConnector>,
) -> Result<AdminResponse> {
    let msg   = ClusterMessage::Admin(req);
    let bytes = encode(&msg)?;

    let tcp = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .with_context(|| format!("connect timeout to {}", addr))?
        .with_context(|| format!("connecting to {}", addr))?;

    let response: ClusterMessage = if let Some(connector) = tls {
        let server_name = ServerName::try_from("ditto-cluster")
            .context("invalid TLS server name")?;
        let mut stream = connector
            .connect(server_name, tcp)
            .await
            .context("TLS handshake")?;
        stream.write_all(&bytes).await?;
        read_framed(&mut stream).await?
    } else {
        let mut stream = tcp;
        stream.write_all(&bytes).await?;
        read_framed(&mut stream).await?
    };

    match response {
        ClusterMessage::AdminResponse(resp) => Ok(resp),
        other => anyhow::bail!("unexpected cluster message: {:?}", other),
    }
}

async fn read_framed<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
) -> Result<ClusterMessage> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let max_message_size = 128 * 1024 * 1024;
    if len > max_message_size {
        anyhow::bail!("RPC response length {} exceeds max {}", len, max_message_size);
    }
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    decode(&payload, max_message_size as u64).context("decoding cluster message")
}

// ---------------------------------------------------------------------------
// Address resolution
// ---------------------------------------------------------------------------

/// Resolve a target string to cluster-port SocketAddrs.
///
/// | Target        | Resolution                                                  |
/// |---------------|-------------------------------------------------------------|
/// | `"local"`     | First configured seed (DNS-resolved); falls back to 127.0.0.1 |
/// | `"all"`       | All seeds (use [`all_cluster_addrs`] for gossip-extended list) |
/// | `"node-1"` etc. | Async DNS: `lookup_host("node-1:<cluster_port>")` → IPv4   |
/// | `"1.2.3.4:7779"` | Direct `SocketAddr` parse                               |
pub async fn resolve_target(target: &str, cluster_port: u16, seeds: &[String]) -> Vec<SocketAddr> {
    match target {
        "local" => {
            // Inside Docker, 127.0.0.1 is the mgmt container's own loopback — not a dittod
            // node. Resolve the first configured seed via DNS so that "local" means
            // "any available node" regardless of deployment topology.
            let seed = seeds.first().cloned()
                .unwrap_or_else(|| format!("127.0.0.1:{}", cluster_port));
            tokio::net::lookup_host(&seed).await
                .map(|it| it.filter(|a| a.is_ipv4()).collect())
                .unwrap_or_else(|_| {
                    vec![format!("127.0.0.1:{}", cluster_port).parse().unwrap()]
                })
        }
        "all" => seeds.iter().filter_map(|s| s.parse().ok()).collect(),
        other => {
            // 1. Direct SocketAddr parse (e.g. "127.0.0.1:7779")
            let normalised = other.replace("localhost", "127.0.0.1");
            if let Ok(addr) = normalised.parse::<SocketAddr>() {
                return vec![addr];
            }
            // 2. Async DNS lookup — handles node names ("node-1") and hostname:port
            //    ("node-1:7779"). Append cluster_port if no port present.
            let with_port = if normalised.contains(':') {
                normalised.clone()
            } else {
                format!("{}:{}", normalised, cluster_port)
            };
            tokio::net::lookup_host(&with_port).await
                .map(|it| it.filter(|a| a.is_ipv4()).collect())
                .unwrap_or_else(|_| {
                    eprintln!("Warning: cannot resolve target '{}' to a socket address", target);
                    vec![]
                })
        }
    }
}

/// For "all" targets: merges config seeds (DNS-resolved) with gossip-discovered nodes.
pub async fn all_cluster_addrs(
    seeds: &[String],
    cluster_port: u16,
    tls: Option<&TlsConnector>,
) -> Vec<SocketAddr> {
    let mut addrs: Vec<SocketAddr> = Vec::new();

    // 1. DNS-resolve config seeds (IPv4 only — Docker DNS may return both IPv4 and IPv6
    //    addresses for a service name; the IPv6 variants are phantom and would appear as
    //    "Unreachable" nodes in the UI since dittod binds on 0.0.0.0/IPv4 only).
    for s in seeds {
        if let Ok(resolved) = tokio::net::lookup_host(s).await {
            for a in resolved {
                if a.is_ipv4() && !addrs.contains(&a) {
                    addrs.push(a);
                }
            }
        }
    }

    // 2. Query ClusterStatus from first reachable seed → gossip-discovered nodes.
    //    The queried node's own entry in the snapshot always carries addr = 0.0.0.0
    //    (its bind address) because it registers itself before it knows its public IP.
    //    Skip those; their real address was already resolved in step 1 via DNS.
    if let Some(&first) = addrs.first() {
        if let Ok(AdminResponse::ClusterView(nodes)) =
            admin_rpc(first, AdminRequest::ClusterStatus, tls).await
        {
            for n in nodes {
                if n.addr.ip().is_unspecified() {
                    continue; // 0.0.0.0 / :: — bind address, not routable
                }
                let discovered = SocketAddr::new(n.addr.ip(), n.cluster_port);
                if !addrs.contains(&discovered) {
                    addrs.push(discovered);
                }
            }
        }
    }

    // Fallback to localhost if no seeds configured
    if addrs.is_empty() {
        if let Ok(addr) = format!("127.0.0.1:{}", cluster_port).parse() {
            addrs.push(addr);
        }
    }

    addrs
}

/// Derives the HTTP REST port from the cluster port.
/// Convention: http_port = cluster_port - 1  (e.g. 7779 → 7778).
pub fn http_port_for(cluster_addr: SocketAddr, cluster_port: u16) -> SocketAddr {
    let http_port = cluster_port.saturating_sub(1);
    SocketAddr::new(cluster_addr.ip(), http_port)
}
