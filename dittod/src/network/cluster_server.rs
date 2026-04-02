use crate::node::NodeHandle;
use ditto_protocol::{decode, encode, ClusterMessage};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::{debug, error, info};

const FRAME_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Start the cluster + admin TCP server (port 7779).
/// Pass `Some(acceptor)` to require mTLS on every incoming connection.
pub async fn start(
    bind: String,
    node: Arc<NodeHandle>,
    acceptor: Option<TlsAcceptor>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&bind).await?;
    info!("Cluster/Admin TCP listening on {}", bind);
    loop {
        let (stream, addr) = listener.accept().await?;
        debug!("Cluster/Admin connection from {}", addr);
        let node = node.clone();
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            let result = if let Some(acc) = acceptor {
                match acc.accept(stream).await {
                    Ok(tls_stream) => handle_cluster(tls_stream, node).await,
                    Err(e) => {
                        error!("TLS accept error from {}: {}", addr, e);
                        return;
                    }
                }
            } else {
                handle_cluster(stream, node).await
            };
            if let Err(e) = result {
                error!("Cluster handler error: {}", e);
            }
        });
    }
}

async fn handle_cluster<S>(mut stream: S, node: Arc<NodeHandle>) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let max_message_size = node.config.lock().unwrap().node.max_message_size_bytes as usize;

    loop {
        let mut len_buf = [0u8; 4];
        match tokio::time::timeout(FRAME_READ_TIMEOUT, stream.read_exact(&mut len_buf)).await {
            Ok(Ok(_)) => {}
            Ok(Err(_)) => break,
            Err(_) => anyhow::bail!("timed out waiting for cluster frame header"),
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > max_message_size {
            anyhow::bail!("message length {} exceeds max {}", len, max_message_size);
        }
        let alloc_len = len.min(max_message_size);
        let mut payload = vec![0u8; alloc_len];
        match tokio::time::timeout(FRAME_READ_TIMEOUT, stream.read_exact(&mut payload)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => anyhow::bail!("timed out waiting for cluster frame payload"),
        }

        let msg: ClusterMessage = decode(&payload, max_message_size as u64)?;
        if let Some(response) = Arc::clone(&node).handle_cluster(msg).await {
            let bytes = encode(&response)?;
            stream.write_all(&bytes).await?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Helper: send a ClusterMessage to a remote node and receive a response.
// ---------------------------------------------------------------------------

/// Send a ClusterMessage to a remote node.
/// Pass `Some(connector)` to use mTLS for the connection.
pub async fn send_cluster(
    addr: std::net::SocketAddr,
    msg: &ClusterMessage,
    tls: Option<&TlsConnector>,
) -> anyhow::Result<Option<ClusterMessage>> {
    let tcp = TcpStream::connect(addr).await?;

    if let Some(connector) = tls {
        let server_name = crate::network::tls::cluster_server_name();
        let tls_stream = connector.connect(server_name, tcp).await?;
        do_cluster_rpc(tls_stream, msg).await
    } else {
        do_cluster_rpc(tcp, msg).await
    }
}

async fn do_cluster_rpc<S>(
    mut stream: S,
    msg: &ClusterMessage,
) -> anyhow::Result<Option<ClusterMessage>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let bytes = encode(msg)?;
    stream.write_all(&bytes).await?;

    let mut len_buf = [0u8; 4];
    match tokio::time::timeout(
        std::time::Duration::from_millis(500),
        stream.read_exact(&mut len_buf),
    )
    .await
    {
        Ok(Ok(_)) => {
            let len = u32::from_be_bytes(len_buf) as usize;
            const MAX_RPC_RESPONSE_BYTES: usize = 128 * 1024 * 1024;
            if len > MAX_RPC_RESPONSE_BYTES {
                anyhow::bail!(
                    "RPC response length {} exceeds max {}",
                    len,
                    MAX_RPC_RESPONSE_BYTES
                );
            }
            let alloc_len = len.min(MAX_RPC_RESPONSE_BYTES);
            let mut payload = vec![0u8; alloc_len];
            match tokio::time::timeout(FRAME_READ_TIMEOUT, stream.read_exact(&mut payload)).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Err(e.into()),
                Err(_) => anyhow::bail!("timed out waiting for RPC payload"),
            }
            Ok(Some(decode(&payload, MAX_RPC_RESPONSE_BYTES as u64)?))
        }
        _ => Ok(None),
    }
}
