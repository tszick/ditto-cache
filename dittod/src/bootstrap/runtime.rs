use crate::{
    backup,
    config::Config,
    gossip::GossipEngine,
    network,
    node::NodeHandle,
    store::{self, KvStore},
};
use anyhow::Result;
use std::{net::SocketAddr, sync::Arc};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::{info, warn};

pub struct RuntimeComponents {
    pub store: Arc<KvStore>,
    pub client_tls_acceptor: Option<TlsAcceptor>,
    pub tls_acceptor: Option<TlsAcceptor>,
    pub node: Arc<NodeHandle>,
}

pub struct ServerBinds {
    pub tcp_bind: String,
    pub http_bind: String,
    pub cluster_bind: String,
}

pub fn build_runtime_components(
    config: &Config,
    config_path: &str,
    resolved_cluster_bind: &str,
) -> Result<RuntimeComponents> {
    let store = build_store(config);
    let (client_tls_acceptor, tls_acceptor, tls_connector) = build_tls_components(config)?;
    let node = NodeHandle::new(
        config.clone(),
        config_path.to_string(),
        store.clone(),
        tls_connector,
        resolved_cluster_bind.to_string(),
    );
    restore_snapshot_if_enabled(config, &node);

    Ok(RuntimeComponents {
        store,
        client_tls_acceptor,
        tls_acceptor,
        node,
    })
}

fn build_store(config: &Config) -> Arc<KvStore> {
    let store = Arc::new(KvStore::new(
        config.cache.max_memory_mb,
        config.cache.default_ttl_secs,
        config.cache.value_size_limit_bytes,
        config.cache.max_keys,
        config.compression.enabled,
        config.compression.threshold_bytes,
    ));

    store::ttl::start_ttl_sweep(store.clone(), 5);
    store
}

fn build_tls_components(
    config: &Config,
) -> Result<(Option<TlsAcceptor>, Option<TlsAcceptor>, Option<TlsConnector>)> {
    if config.tls.enabled {
        let client_acceptor = network::tls::build_server_only_acceptor(&config.tls)?;
        let acceptor = network::tls::build_acceptor(&config.tls)?;
        let connector = network::tls::build_connector(&config.tls)?;
        info!("TLS enabled on client TCP port; mTLS enabled on cluster/admin port");
        Ok((Some(client_acceptor), Some(acceptor), Some(connector)))
    } else {
        Ok((None, None, None))
    }
}

fn restore_snapshot_if_enabled(config: &Config, node: &Arc<NodeHandle>) {
    if !config.backup.restore_on_start {
        return;
    }

    match backup::restore_latest_snapshot(node, &config.backup) {
        Ok(Some(loaded)) => {
            node.record_snapshot_restore(
                loaded.path.clone(),
                loaded.entries as u64,
                loaded.duration_ms,
            );
            info!(
                "Startup snapshot restore completed: path={} entries={} duration_ms={}",
                loaded.path, loaded.entries, loaded.duration_ms
            );
        }
        Ok(None) => {
            info!("Startup snapshot restore enabled, but no snapshot file found.");
        }
        Err(e) => {
            warn!(
                "Startup snapshot restore failed: {} (continuing without restore)",
                e
            );
        }
    }
}

pub async fn start_gossip_engine(
    config: &Config,
    resolved_cluster_bind: &str,
    node: &Arc<NodeHandle>,
) -> Result<Arc<GossipEngine>> {
    let gossip_addr: SocketAddr =
        format!("{}:{}", resolved_cluster_bind, config.node.gossip_port).parse()?;

    let seed_gossip_addrs: Vec<String> = config
        .cluster
        .seeds
        .iter()
        .filter_map(|seed| {
            let host = seed.split(':').next()?;
            Some(format!("{}:{}", host, config.node.gossip_port))
        })
        .collect();
    info!("Gossip bootstrap seeds: {:?}", seed_gossip_addrs);

    let gossip = Arc::new(
        GossipEngine::new(
            gossip_addr,
            node.active_set.clone(),
            config.replication.gossip_interval_ms,
            seed_gossip_addrs,
        )
        .await?,
    );
    gossip.clone().start();
    info!("Gossip engine started on UDP {}", gossip_addr);
    Ok(gossip)
}

pub fn start_background_tasks(node: &Arc<NodeHandle>, config: &Config) {
    {
        let n = node.clone();
        tokio::spawn(async move { n.run_recovery().await });
    }

    {
        let n = node.clone();
        let cfg = config.backup.clone();
        tokio::spawn(async move { backup::run_scheduler(n, cfg).await });
    }

    node.clone().start_log_compaction();
    node.clone().start_version_check();
    node.clone().start_anti_entropy();
    node.clone().start_mixed_version_probe();
}

pub fn build_server_binds(
    config: &Config,
    resolved_bind: &str,
    resolved_cluster_bind: &str,
) -> ServerBinds {
    ServerBinds {
        tcp_bind: format!("{}:{}", resolved_bind, config.node.client_port),
        http_bind: format!("{}:{}", resolved_bind, config.node.http_port),
        cluster_bind: format!("{}:{}", resolved_cluster_bind, config.node.cluster_port),
    }
}

pub async fn run_servers(
    binds: ServerBinds,
    node: Arc<NodeHandle>,
    client_tls_acceptor: Option<TlsAcceptor>,
    tls_acceptor: Option<TlsAcceptor>,
    http_tls: Option<(String, String)>,
) -> Result<()> {
    let n1 = node.clone();
    let n2 = node.clone();
    let n3 = node;

    tokio::try_join!(
        network::tcp_server::start(binds.tcp_bind, n1, client_tls_acceptor),
        network::http_server::start(binds.http_bind, n2, http_tls),
        network::cluster_server::start(binds.cluster_bind, n3, tls_acceptor),
    )?;

    Ok(())
}
