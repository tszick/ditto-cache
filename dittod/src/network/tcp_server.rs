use crate::node::NodeHandle;
use ditto_protocol::{decode, encode, ClientRequest, ClientResponse};
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::broadcast,
};
use tracing::{debug, error, info, warn};

fn namespaced_watch_key(node: &Arc<NodeHandle>, namespace: Option<String>, key: String) -> String {
    let cfg = node.config.lock().unwrap();
    if !cfg.tenancy.enabled {
        return key;
    }
    let ns = namespace
        .unwrap_or_else(|| cfg.tenancy.default_namespace.clone())
        .trim()
        .to_string();
    if ns.is_empty() {
        key
    } else {
        format!("{}::{}", ns, key)
    }
}

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
    let (max_message_size, expected_token, frame_read_timeout) = {
        let cfg = node.config.lock().unwrap();
        (
            cfg.node.max_message_size_bytes as usize,
            cfg.node.client_auth_token.clone(),
            Duration::from_millis(cfg.node.frame_read_timeout_ms.max(1)),
        )
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
                frame_read_timeout,
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
                    ClientRequest::Watch { key, namespace } => {
                        watched_keys.insert(namespaced_watch_key(&node, namespace, key));
                        let bytes = encode(&ClientResponse::Watching)?;
                        stream.write_all(&bytes).await?;
                    }
                    ClientRequest::Unwatch { key, namespace } => {
                        watched_keys.remove(&namespaced_watch_key(&node, namespace, key));
                        let bytes = encode(&ClientResponse::Unwatched)?;
                        stream.write_all(&bytes).await?;
                    }
                    other => {
                        let response = node.handle_client_tcp(other).await;
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
    frame_read_timeout: Duration,
) -> anyhow::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match tokio::time::timeout(frame_read_timeout, stream.read_exact(&mut len_buf)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Ok(Err(e)) => return Err(e.into()),
        Err(_) => anyhow::bail!("timed out waiting for frame header"),
    }

    let len = u32::from_be_bytes(len_buf) as usize;
    if len > max_size {
        anyhow::bail!("message length {} exceeds max {}", len, max_size);
    }
    let mut payload = vec![0u8; len];
    match tokio::time::timeout(frame_read_timeout, stream.read_exact(&mut payload)).await {
        Ok(Ok(_)) => Ok(Some(payload)),
        Ok(Err(e)) => Err(e.into()),
        Err(_) => anyhow::bail!("timed out waiting for frame payload"),
    }
}

#[cfg(test)]
mod tests {
    use super::{handle_client, namespaced_watch_key, read_frame};
    use crate::{config::Config, node::NodeHandle, store::KvStore};
    use bytes::Bytes;
    use ditto_protocol::{decode, encode, ClientRequest, ClientResponse, ErrorCode};
    use std::sync::Arc;
    use tokio::{
        io::AsyncWriteExt,
        net::{TcpListener, TcpStream},
    };

    async fn stream_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
        let (server, _) = listener.accept().await.unwrap();
        (client.await.unwrap(), server)
    }

    async fn read_client_response(stream: &mut TcpStream) -> ClientResponse {
        let payload = read_frame(stream, 4096, std::time::Duration::from_secs(1))
            .await
            .unwrap()
            .unwrap();
        decode(&payload, 4096).unwrap()
    }

    fn test_node(mut cfg: Config) -> Arc<NodeHandle> {
        cfg.node.cluster_port = 17779;
        let store = Arc::new(KvStore::new(
            cfg.cache.max_memory_mb,
            cfg.cache.default_ttl_secs,
            cfg.cache.value_size_limit_bytes,
            cfg.cache.max_keys,
            cfg.compression.enabled,
            cfg.compression.threshold_bytes,
        ));
        NodeHandle::new(
            cfg,
            "test-node.toml".into(),
            store,
            None,
            "127.0.0.1".into(),
        )
    }

    #[tokio::test]
    async fn handle_client_rejects_missing_auth_before_dispatch() {
        let mut cfg = Config::default();
        cfg.node.client_auth_token = Some("secret".into());
        let node = test_node(cfg);
        let (mut client, server) = stream_pair().await;
        let task = tokio::spawn(handle_client(server, node));

        client
            .write_all(&encode(&ClientRequest::Ping).unwrap())
            .await
            .unwrap();

        let response = read_client_response(&mut client).await;
        assert!(matches!(
            response,
            ClientResponse::Error {
                code: ErrorCode::AuthFailed,
                ..
            }
        ));
        drop(client);

        let err = task.await.unwrap().unwrap_err();
        assert!(err.to_string().contains("Client auth failed"));
    }

    #[tokio::test]
    async fn handle_client_authenticates_then_dispatches_ping() {
        let mut cfg = Config::default();
        cfg.node.client_auth_token = Some("secret".into());
        let node = test_node(cfg);
        let (mut client, server) = stream_pair().await;
        let task = tokio::spawn(handle_client(server, node));

        client
            .write_all(
                &encode(&ClientRequest::Auth {
                    token: "secret".into(),
                })
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            read_client_response(&mut client).await,
            ClientResponse::AuthOk
        ));

        client
            .write_all(&encode(&ClientRequest::Ping).unwrap())
            .await
            .unwrap();
        assert!(matches!(
            read_client_response(&mut client).await,
            ClientResponse::Pong
        ));
        drop(client);

        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn handle_client_pushes_watch_events_and_accepts_unwatch() {
        let node = test_node(Config::default());
        let (mut client, server) = stream_pair().await;
        let task = tokio::spawn(handle_client(server, Arc::clone(&node)));

        client
            .write_all(
                &encode(&ClientRequest::Watch {
                    key: "watched".into(),
                    namespace: None,
                })
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            read_client_response(&mut client).await,
            ClientResponse::Watching
        ));

        node.watch_tx
            .send(("watched".into(), Some(Bytes::from_static(b"value")), 5))
            .unwrap();
        match read_client_response(&mut client).await {
            ClientResponse::WatchEvent {
                key,
                value,
                version,
            } => {
                assert_eq!(key, "watched");
                assert_eq!(value, Some(Bytes::from_static(b"value")));
                assert_eq!(version, 5);
            }
            other => panic!("expected watch event, got {other:?}"),
        }

        client
            .write_all(
                &encode(&ClientRequest::Unwatch {
                    key: "watched".into(),
                    namespace: None,
                })
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            read_client_response(&mut client).await,
            ClientResponse::Unwatched
        ));
        drop(client);

        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn read_frame_decodes_payload_and_reports_clean_eof() {
        let (mut client, mut server) = stream_pair().await;
        let frame = encode(&ClientRequest::Ping).unwrap();
        client.write_all(&frame).await.unwrap();

        let payload = read_frame(&mut server, 1024, std::time::Duration::from_secs(1))
            .await
            .unwrap()
            .unwrap();
        let decoded: ClientRequest = decode(&payload, 1024).unwrap();
        assert!(matches!(decoded, ClientRequest::Ping));

        drop(client);
        assert!(
            read_frame(&mut server, 1024, std::time::Duration::from_secs(1))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn read_frame_rejects_oversized_payload_length() {
        let (mut client, mut server) = stream_pair().await;
        client.write_all(&9u32.to_be_bytes()).await.unwrap();

        let err = read_frame(&mut server, 8, std::time::Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("exceeds max"));
    }

    #[tokio::test]
    async fn read_frame_times_out_waiting_for_header() {
        let (_client, mut server) = stream_pair().await;

        let err = read_frame(&mut server, 8, std::time::Duration::from_millis(5))
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("timed out waiting for frame header"));
    }

    #[test]
    fn namespaced_watch_key_honors_tenancy_settings() {
        let disabled = test_node(Config::default());
        assert_eq!(
            namespaced_watch_key(&disabled, Some("tenant-a".into()), "alpha".into()),
            "alpha"
        );

        let mut cfg = Config::default();
        cfg.tenancy.enabled = true;
        cfg.tenancy.default_namespace = "default-ns".into();
        let enabled = test_node(cfg);
        assert_eq!(
            namespaced_watch_key(&enabled, Some(" tenant-a ".into()), "alpha".into()),
            "tenant-a::alpha"
        );
        assert_eq!(
            namespaced_watch_key(&enabled, None, "beta".into()),
            "default-ns::beta"
        );
    }
}
