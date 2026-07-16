use crate::replication::ActiveSet;
use dashmap::DashMap;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::{
    net::SocketAddr,
    sync::{atomic::{AtomicU64, Ordering}, Arc},
    time::Duration,
};
use ditto_protocol::{decode_gossip, encode_gossip, GossipMessage, NodeInfo};
use tokio::{net::UdpSocket, sync::Mutex};
use tracing::{debug, info, warn};

use uuid::Uuid;
pub struct GossipEngine {
    socket: Arc<UdpSocket>,
    active_set: Arc<Mutex<ActiveSet>>,
    interval: Duration,
    /// Gossip addresses of seed nodes as "host:port" strings.
    /// Kept as strings so tokio can do DNS resolution at send time
    /// (SocketAddr::parse() does not accept hostnames like "node-2").
    seed_addrs: Vec<String>,
    auth_secret: Option<Arc<[u8]>>,
    session_id: Uuid,
    next_sequence: AtomicU64,
    replay_sequences: DashMap<(Uuid, Uuid), u64>,
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
            auth_secret: None,
            session_id: Uuid::new_v4(),
            next_sequence: AtomicU64::new(1),
            replay_sequences: DashMap::new(),
        })
    }

    /// Spawn heartbeat sender + receiver + reaper tasks.

    pub async fn new_with_auth(
        bind_addr: SocketAddr,
        active_set: Arc<Mutex<ActiveSet>>,
        interval_ms: u64,
        seed_addrs: Vec<String>,
        auth_secret: String,
    ) -> anyhow::Result<Self> {
        let socket = UdpSocket::bind(bind_addr).await?;
        Ok(Self {
            socket: Arc::new(socket),
            active_set,
            interval: Duration::from_millis(interval_ms),
            seed_addrs,
            auth_secret: Some(Arc::from(auth_secret.into_bytes())),
            session_id: Uuid::new_v4(),
            next_sequence: AtomicU64::new(1),
            replay_sequences: DashMap::new(),
        })
    }
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

            // NOTE: use raw prost envelope bytes (no length prefix) for UDP.
            // The ditto_protocol::encode() helper prepends a 4-byte TCP framing
            // header that decode() does NOT strip – it would silently corrupt UDP.
            if let Ok(bytes) = encode_gossip(&msg) {
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
                    // Raw prost envelope – no length prefix (unlike TCP frames).
                    match decode_gossip(slice, 65535) {
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

#[cfg(test)]
mod tests {
    use super::GossipEngine;
    use crate::replication::ActiveSet;
    use ditto_protocol::{decode_gossip, encode_gossip, GossipMessage, NodeInfo, NodeStatus};
    use std::{net::SocketAddr, sync::Arc, time::Duration};
    use tokio::{net::UdpSocket, sync::Mutex};
    use uuid::Uuid;

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    fn active_set(local_id: Uuid) -> Arc<Mutex<ActiveSet>> {
        Arc::new(Mutex::new(ActiveSet::new(
            local_id,
            addr(7780),
            7779,
            50,
            8,
        )))
    }

    #[tokio::test]
    async fn handle_heartbeat_uses_udp_source_address_and_updates_existing_peer() {
        let local_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();
        let active_set = active_set(local_id);
        let engine = GossipEngine::new(addr(0), active_set.clone(), 10, vec![])
            .await
            .unwrap();
        let first_src = addr(9001);
        let second_src = addr(9002);

        engine
            .handle(
                GossipMessage::Heartbeat {
                    node_id: peer_id,
                    addr: addr(1),
                    cluster_port: 7779,
                    status: NodeStatus::Syncing,
                    last_applied: 41,
                },
                first_src,
            )
            .await;
        engine
            .handle(
                GossipMessage::Heartbeat {
                    node_id: peer_id,
                    addr: addr(2),
                    cluster_port: 7788,
                    status: NodeStatus::Active,
                    last_applied: 42,
                },
                second_src,
            )
            .await;

        let set = active_set.lock().await;
        let peer = set
            .all_nodes()
            .into_iter()
            .find(|node| node.id == peer_id)
            .expect("peer should be known");
        assert_eq!(peer.addr, second_src);
        assert_eq!(peer.cluster_port, 7788);
        assert_eq!(peer.status, NodeStatus::Active);
        assert_eq!(peer.last_applied, 42);
    }

    #[tokio::test]
    async fn handle_active_set_update_upserts_all_nodes() {
        let local_id = Uuid::new_v4();
        let peer_a = Uuid::new_v4();
        let peer_b = Uuid::new_v4();
        let active_set = active_set(local_id);
        let engine = GossipEngine::new(addr(0), active_set.clone(), 10, vec![])
            .await
            .unwrap();

        engine
            .handle(
                GossipMessage::ActiveSetUpdate {
                    active_nodes: vec![
                        NodeInfo {
                            id: peer_a,
                            addr: addr(9101),
                            cluster_port: 7779,
                            status: NodeStatus::Active,
                            last_applied: 7,
                        },
                        NodeInfo {
                            id: peer_b,
                            addr: addr(9102),
                            cluster_port: 7780,
                            status: NodeStatus::Inactive,
                            last_applied: 8,
                        },
                    ],
                },
                addr(9999),
            )
            .await;

        let nodes = active_set.lock().await.snapshot();
        assert!(nodes.iter().any(|node| {
            node.id == peer_a && node.status == NodeStatus::Active && node.last_applied == 7
        }));
        assert!(nodes.iter().any(|node| {
            node.id == peer_b && node.status == NodeStatus::Inactive && node.cluster_port == 7780
        }));
    }

    #[tokio::test]
    async fn run_sender_sends_raw_gossip_heartbeat_to_seed_addresses() {
        let seed = UdpSocket::bind(addr(0)).await.unwrap();
        let seed_addr = seed.local_addr().unwrap();
        let local_id = Uuid::new_v4();
        let active_set = active_set(local_id);
        active_set.lock().await.set_local_applied(123);
        let engine = GossipEngine::new(addr(0), active_set, 10, vec![seed_addr.to_string()])
            .await
            .unwrap();

        let sender = tokio::spawn({
            let engine = Arc::new(engine);
            async move { engine.run_sender().await }
        });

        let mut buf = [0u8; 2048];
        let (len, _) = tokio::time::timeout(Duration::from_secs(1), seed.recv_from(&mut buf))
            .await
            .expect("seed should receive heartbeat")
            .unwrap();
        sender.abort();

        match decode_gossip(&buf[..len], 2048).unwrap() {
            GossipMessage::Heartbeat {
                node_id,
                cluster_port,
                status,
                last_applied,
                ..
            } => {
                assert_eq!(node_id, local_id);
                assert_eq!(cluster_port, 7779);
                assert_eq!(status, NodeStatus::Active);
                assert_eq!(last_applied, 123);
            }
            other => panic!("unexpected gossip message: {other:?}"),
        }
    }

    #[tokio::test]
    async fn run_receiver_decodes_udp_gossip_and_ignores_bad_packets() {
        let local_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();
        let active_set = active_set(local_id);
        let engine = Arc::new(
            GossipEngine::new(addr(0), active_set.clone(), 10, vec![])
                .await
                .unwrap(),
        );
        let engine_addr = engine.socket.local_addr().unwrap();
        let receiver = tokio::spawn({
            let engine = engine.clone();
            async move { engine.run_receiver().await }
        });
        let client = UdpSocket::bind(addr(0)).await.unwrap();
        let client_addr = client.local_addr().unwrap();

        client.send_to(b"not gossip", engine_addr).await.unwrap();
        let message = GossipMessage::Heartbeat {
            node_id: peer_id,
            addr: addr(1),
            cluster_port: 7779,
            status: NodeStatus::Active,
            last_applied: 9,
        };
        client
            .send_to(&encode_gossip(&message).unwrap(), engine_addr)
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let seen = active_set
                    .lock()
                    .await
                    .snapshot()
                    .into_iter()
                    .any(|node| node.id == peer_id && node.addr == client_addr);
                if seen {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("receiver should apply valid heartbeat");
        receiver.abort();
    }

    #[tokio::test]
    async fn run_reaper_marks_stale_remote_nodes_offline() {
        let local_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();
        let active_set = active_set(local_id);
        active_set.lock().await.upsert(NodeInfo {
            id: peer_id,
            addr: addr(9200),
            cluster_port: 7779,
            status: NodeStatus::Active,
            last_applied: 1,
        });
        tokio::time::sleep(Duration::from_millis(60)).await;
        let engine = Arc::new(
            GossipEngine::new(addr(0), active_set.clone(), 1, vec![])
                .await
                .unwrap(),
        );

        let reaper = tokio::spawn({
            let engine = engine.clone();
            async move { engine.run_reaper().await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let offline = active_set
                    .lock()
                    .await
                    .snapshot()
                    .into_iter()
                    .any(|node| node.id == peer_id && node.status == NodeStatus::Offline);
                if offline {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("reaper should mark stale peer offline");
        reaper.abort();
    }
}
