use crate::{
    network::client_auth::ClientAccessController,
    node::NodeHandle,
};
use ditto_protocol::{decode, encode, ClientRequest, ClientResponse, ErrorCode};
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::broadcast,
};
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error, info, warn};

trait ClientStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T> ClientStream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

type BoxedClientStream = Box<dyn ClientStream>;

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
pub async fn start(
    bind: String,
    node: Arc<NodeHandle>,
    tls_acceptor: Option<TlsAcceptor>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&bind).await?;
    if tls_acceptor.is_some() {
        info!("Client TCP listening with TLS on {}", bind);
    } else {
        info!("Client TCP listening on {}", bind);
    }
    loop {
        let (stream, addr) = listener.accept().await?;
        debug!("Client connected from {}", addr);
        let node = node.clone();
        let tls_acceptor = tls_acceptor.clone();
        tokio::spawn(async move {
            let result = if let Some(acceptor) = tls_acceptor {
                match acceptor.accept(stream).await {
                    Ok(stream) => handle_client(Box::new(stream), node).await,
                    Err(err) => Err(anyhow::anyhow!("client TLS handshake failed: {}", err)),
                }
            } else {
                handle_client(Box::new(stream), node).await
            };
            if let Err(e) = result {
                error!("Client handler error: {}", e);
            }
        });
    }
}

async fn handle_client(
    mut stream: BoxedClientStream,
    node: Arc<NodeHandle>,
) -> anyhow::Result<()> {
    let (max_message_size, access_controller, legacy_token, frame_read_timeout) = {
        let cfg = node.config.lock().unwrap();
        (
            cfg.node.max_message_size_bytes as usize,
            ClientAccessController::from_config(&cfg),
            cfg.node.client_auth_token.clone(),
            Duration::from_millis(cfg.node.frame_read_timeout_ms.max(1)),
        )
    };
    let mut authenticated = access_controller.default_principal();

    let mut watch_rx = node.watch_tx.subscribe();
    let mut watched_keys: HashSet<String> = HashSet::new();

    loop {
        tokio::select! {
            read_result = read_frame(
                &mut stream,
                max_message_size,
                frame_read_timeout,
            ) => {
                let payload = match read_result {
                    Ok(Some(p)) => p,
                    Ok(None) => break,
                    Err(e) => return Err(e),
                };

                let request: ClientRequest = decode(&payload, max_message_size as u64)?;

                if authenticated.is_none() {
                    match request {
                        ClientRequest::Auth { token } => {
                            if let Some(principal) = access_controller.authenticate(&token, legacy_token.as_deref()) {
                                authenticated = Some(principal);
                                let bytes = encode(&ClientResponse::AuthOk)?;
                                stream.write_all(&bytes).await?;
                                continue;
                            }
                            let response = ClientResponse::Error {
                                code: ErrorCode::AuthFailed,
                                message: "Invalid or missing auth token".into(),
                            };
                            let bytes = encode(&response)?;
                            stream.write_all(&bytes).await?;
                            anyhow::bail!("Client auth failed");
                        }
                        _ => {
                            let response = ClientResponse::Error {
                                code: ErrorCode::AuthFailed,
                                message: "Invalid or missing auth token".into(),
                            };
                            let bytes = encode(&response)?;
                            stream.write_all(&bytes).await?;
                            anyhow::bail!("Client auth failed");
                        }
                    }
                }

                let principal = authenticated
                    .as_ref()
                    .expect("authenticated principal must exist after auth gate");

                if let Err(response) = principal.authorize(&request, &access_controller.tenancy) {
                    let bytes = encode(&response)?;
                    stream.write_all(&bytes).await?;
                    continue;
                }

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
                        warn!("Watch subscriber lagged, missed {} events", n);
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
    Ok(())
}

async fn read_frame<S>(
    stream: &mut S,
    max_size: usize,
    frame_read_timeout: Duration,
) -> anyhow::Result<Option<Vec<u8>>>
where
    S: AsyncRead + Unpin + ?Sized,
{
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
        let task = tokio::spawn(handle_client(Box::new(server), node));

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
        let task = tokio::spawn(handle_client(Box::new(server), node));

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
    async fn handle_client_applies_scoped_acl_after_authentication() {
        let mut cfg = Config::default();
        cfg.tenancy.enabled = true;
        cfg.tenancy.default_namespace = "default".into();
        cfg.client_auth.tokens.push(crate::config::ClientAuthTokenConfig {
            name: "reader".into(),
            token: Some("reader-secret".into()),
            namespaces: vec!["tenant-a".into()],
            read_prefixes: vec!["cache:".into()],
            ..Default::default()
        });
        let node = test_node(cfg);
        let (mut client, server) = stream_pair().await;
        let task = tokio::spawn(handle_client(Box::new(server), node));

        client
            .write_all(
                &encode(&ClientRequest::Auth {
                    token: "reader-secret".into(),
                })
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(read_client_response(&mut client).await, ClientResponse::AuthOk));

        client
            .write_all(
                &encode(&ClientRequest::Get {
                    key: "cache:item".into(),
                    namespace: Some("tenant-b".into()),
                })
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            read_client_response(&mut client).await,
            ClientResponse::Error {
                code: ErrorCode::AccessDenied,
                ..
            }
        ));

        drop(client);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn handle_client_pushes_watch_events_and_accepts_unwatch() {
        let node = test_node(Config::default());
        let (mut client, server) = stream_pair().await;
        let task = tokio::spawn(handle_client(Box::new(server), Arc::clone(&node)));

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
