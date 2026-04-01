use crate::node::NodeHandle;
use ditto_protocol::{ClientRequest, ClientResponse, encode, decode};
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::broadcast,
};
use tracing::{debug, error, info, warn};

const FRAME_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Start the client-facing TCP server (port 7777).
pub async fn start(bind: String, node: Arc<NodeHandle>) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&bind).await?;
    info!("Client TCP listening on {}", bind);
    loop {
        let (stream, addr) = listener.accept().await?;
        debug!("Client connected from {}", addr);
        let node = node.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, node).await {
                error!("Client handler error: {}", e);
            }
        });
    }
}

async fn handle_client(mut stream: TcpStream, node: Arc<NodeHandle>) -> anyhow::Result<()> {
    let (max_message_size, expected_token) = {
        let cfg = node.config.lock().unwrap();
        (cfg.node.max_message_size_bytes as usize, cfg.node.client_auth_token.clone())
    };
    let mut authenticated = expected_token.is_none();

    // DITTO-02: subscribe to the node-wide watch broadcast channel.
    let mut watch_rx = node.watch_tx.subscribe();
    // Keys this connection is watching.
    let mut watched_keys: HashSet<String> = HashSet::new();

    loop {
        tokio::select! {
            // ── Incoming client request ──────────────────────────────────────
            read_result = read_frame(
                &mut stream,
                max_message_size,
                expected_token.is_some() && !authenticated,
            ) => {
                let payload = match read_result {
                    Ok(Some(p)) => p,
                    Ok(None)    => break, // connection closed cleanly
                    Err(e)      => return Err(e),
                };

                let request: ClientRequest = decode(&payload, max_message_size as u64)?;

                if !authenticated {
                    match request {
                        ClientRequest::Auth { token } if Some(&token) == expected_token.as_ref() => {
                            authenticated = true;
                            let bytes = encode(&ClientResponse::AuthOk)?;
                            stream.write_all(&bytes).await?;
                            continue;
                        }
                        _ => {
                            let response = ClientResponse::Error {
                                code: ditto_protocol::ErrorCode::AuthFailed,
                                message: "Invalid or missing auth token".into(),
                            };
                            let bytes = encode(&response)?;
                            stream.write_all(&bytes).await?;
                            anyhow::bail!("Client auth failed");
                        }
                    }
                }

                // DITTO-02: Watch / Unwatch are handled at the connection level
                // (they mutate per-connection state, not node-wide state).
                match request {
                    ClientRequest::Watch { key } => {
                        watched_keys.insert(key);
                        let bytes = encode(&ClientResponse::Watching)?;
                        stream.write_all(&bytes).await?;
                    }
                    ClientRequest::Unwatch { key } => {
                        watched_keys.remove(&key);
                        let bytes = encode(&ClientResponse::Unwatched)?;
                        stream.write_all(&bytes).await?;
                    }
                    other => {
                        let response = node.handle_client(other).await;
                        let bytes = encode(&response)?;
                        stream.write_all(&bytes).await?;
                    }
                }
            }

            // ── DITTO-02: outbound watch event push ──────────────────────────
            event = watch_rx.recv() => {
                match event {
                    Ok((key, value, version)) => {
                        if watched_keys.contains(&key) {
                            let response = ClientResponse::WatchEvent { key, value, version };
                            let bytes = encode(&response)?;
                            stream.write_all(&bytes).await?;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        // Subscriber was slow; some events were dropped.
                        // For cache invalidation this is acceptable — next poll will refresh.
                        warn!("Watch subscriber lagged, missed {} events", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
    Ok(())
}

/// Read one length-prefixed frame from the stream.
/// Returns `Ok(None)` on clean EOF, `Ok(Some(payload))` on success, `Err` on I/O errors.
async fn read_frame(
    stream: &mut TcpStream,
    max_size: usize,
    use_timeout: bool,
) -> anyhow::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    if use_timeout {
        match tokio::time::timeout(FRAME_READ_TIMEOUT, stream.read_exact(&mut len_buf)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => anyhow::bail!("timed out waiting for frame header"),
        }
    } else if let Err(e) = stream.read_exact(&mut len_buf).await {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(e.into());
    }

    let len = u32::from_be_bytes(len_buf) as usize;
    if len > max_size {
        anyhow::bail!("message length {} exceeds max {}", len, max_size);
    }
    let mut payload = vec![0u8; len];
    if use_timeout {
        match tokio::time::timeout(FRAME_READ_TIMEOUT, stream.read_exact(&mut payload)).await {
            Ok(Ok(_)) => Ok(Some(payload)),
            Ok(Err(e)) => Err(e.into()),
            Err(_) => anyhow::bail!("timed out waiting for frame payload"),
        }
    } else {
        stream.read_exact(&mut payload).await?;
        Ok(Some(payload))
    }
}
