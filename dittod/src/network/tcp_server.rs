use crate::node::NodeHandle;
use ditto_protocol::{ClientRequest, encode, decode};
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tracing::{debug, error, info};

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

    loop {
        // Read 4-byte length prefix.
        let mut len_buf = [0u8; 4];
        if stream.read_exact(&mut len_buf).await.is_err() {
            break; // connection closed
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > max_message_size {
            anyhow::bail!("message length {} exceeds max {}", len, max_message_size);
        }

        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).await?;

        let request: ClientRequest = decode(&payload, max_message_size as u64)?;

        if !authenticated {
            match request {
                ClientRequest::Auth { token } if Some(&token) == expected_token.as_ref() => {
                    authenticated = true;
                    let response = ditto_protocol::ClientResponse::AuthOk;
                    let bytes = encode(&response)?;
                    stream.write_all(&bytes).await?;
                    continue;
                }
                _ => {
                    let response = ditto_protocol::ClientResponse::Error {
                        code: ditto_protocol::ErrorCode::AuthFailed,
                        message: "Invalid or missing auth token".into(),
                    };
                    let bytes = encode(&response)?;
                    stream.write_all(&bytes).await?;
                    anyhow::bail!("Client auth failed");
                }
            }
        }

        let response = node.handle_client(request).await;

        let bytes = encode(&response)?;
        stream.write_all(&bytes).await?;
    }
    Ok(())
}
