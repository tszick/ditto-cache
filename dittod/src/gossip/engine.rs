use crate::replication::ActiveSet;
use ditto_protocol::{GossipMessage, NodeInfo};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{net::UdpSocket, sync::Mutex};
use tracing::{debug, info, warn};

pub struct GossipEngine {
    socket: Arc<UdpSocket>,
    active_set: Arc<Mutex<ActiveSet>>,
    interval: Duration,
    /// Gossip addresses of seed nodes as "host:port" strings.
    /// Kept as strings so tokio can do DNS resolution at send time
    /// (SocketAddr::parse() does not accept hostnames like "node-2").
    seed_addrs: Vec<String>,
}

impl GossipEngine {
    pub async fn new(
        bind_addr: SocketAddr,
        active_set: Arc<Mutex<ActiveSet>>,
        interval_ms: u64,
        seed_addrs: Vec<String>,
    ) -> anyhow::Result<Self> {
        let socket = UdpSocket::bind(bind_addr).await?;
        Ok(Self {
            socket: Arc::new(socket),
            active_set,
            interval: Duration::from_millis(interval_ms),
            seed_addrs,
        })
    }

    /// Spawn heartbeat sender + receiver + reaper tasks.
    pub fn start(self: Arc<Self>) {
        let sender = self.clone();
        tokio::spawn(async move { sender.run_sender().await });

        let receiver = self.clone();
        tokio::spawn(async move { receiver.run_receiver().await });

        let reaper = self.clone();
        tokio::spawn(async move { reaper.run_reaper().await });
    }

    async fn run_sender(&self) {
        let mut ticker = tokio::time::interval(self.interval);
        loop {
            ticker.tick().await;

            let (msg, known_peers) = {
                let set = self.active_set.lock().await;
                let local = set
                    .all_nodes()
                    .into_iter()
                    .find(|n| n.id == set.local_id())
                    .cloned();
                // Known peers: use the gossip address stored in addr
                // (set correctly via run_receiver using actual UDP src).
                let peers: Vec<SocketAddr> = set
                    .all_nodes()
                    .into_iter()
                    .filter(|n| n.id != set.local_id())
                    .map(|n| SocketAddr::new(n.addr.ip(), n.addr.port()))
                    .collect();
                match local {
                    None => continue,
                    Some(me) => {
                        let msg = GossipMessage::Heartbeat {
                            node_id: me.id,
                            addr: me.addr,
                            cluster_port: me.cluster_port,
                            status: me.status,
                            last_applied: me.last_applied,
                        };
                        (msg, peers)
                    }
                }
            };

            // NOTE: use raw bincode (no length prefix) for UDP.
            // The ditto_protocol::encode() helper prepends a 4-byte TCP framing
            // header that decode() does NOT strip – it would silently corrupt UDP.
            // Use fixint encoding explicitly so sender and receiver agree on wire format.
            use bincode::Options;
            let codec = bincode::DefaultOptions::new().with_fixint_encoding();
            if let Ok(bytes) = codec.serialize(&msg) {
                // Send to already-discovered peers.
                for peer in &known_peers {
                    let _ = self.socket.send_to(&bytes, peer).await;
                }
                // Bootstrap: also send to seed addresses so we discover peers
                // that aren't in our active set yet.
                for seed in &self.seed_addrs {
                    let _ = self.socket.send_to(&bytes, seed.as_str()).await;
                    debug!("Gossip: bootstrap ping → {}", seed);
                }
            }
        }
    }

    async fn run_receiver(&self) {
        let mut buf = vec![0u8; 65535];
        loop {
            match self.socket.recv_from(&mut buf).await {
                Err(e) => {
                    warn!("Gossip recv error: {}", e);
                    continue;
                }
                Ok((len, src)) => {
                    let slice = &buf[..len];
                    // Raw bincode – no length prefix (unlike TCP frames).
                    // Must use fixint encoding to match the sender.
                    use bincode::Options;
                    let options = bincode::DefaultOptions::new()
                        .with_fixint_encoding()
                        .with_limit(65535)
                        .allow_trailing_bytes();
                    match options.deserialize::<GossipMessage>(slice) {
                        Err(e) => {
                            warn!("Gossip decode error from {}: {}", src, e);
                            continue;
                        }
                        // Use actual UDP src: real routable IP + gossip port.
                        Ok(msg) => self.handle(msg, src).await,
                    }
                }
            }
        }
    }

    async fn handle(&self, msg: GossipMessage, src: SocketAddr) {
        let mut set = self.active_set.lock().await;
        match msg {
            GossipMessage::Heartbeat {
                node_id,
                addr: _, // ignore – may be 0.0.0.0 (bind addr)
                cluster_port,
                status,
                last_applied,
            } => {
                let is_new = !set.all_nodes().iter().any(|n| n.id == node_id);
                // Use the actual UDP source address: real routable IP + gossip port.
                set.upsert(NodeInfo {
                    id: node_id,
                    addr: src,
                    cluster_port,
                    status,
                    last_applied,
                });
                if is_new {
                    info!(
                        "Gossip: discovered new peer {} at {} (cluster :{})",
                        node_id, src, cluster_port
                    );
                } else {
                    debug!("Gossip: heartbeat from {} (src={})", node_id, src);
                }
            }
            GossipMessage::ActiveSetUpdate { active_nodes } => {
                for node in active_nodes {
                    set.upsert(node);
                }
            }
        }
    }

    /// Periodically reap nodes that stopped sending heartbeats.
    async fn run_reaper(&self) {
        let mut ticker = tokio::time::interval(self.interval * 3);
        loop {
            ticker.tick().await;
            let mut set = self.active_set.lock().await;
            set.reap_dead();
        }
    }
}
