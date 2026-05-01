//! HTTP helper functions for communicating with `ditto-mgmt`.
//!
//! All four verbs (`mgmt_get`, `mgmt_post`, `mgmt_put`, `mgmt_delete`) follow
//! the same pattern: send a JSON request, assert a 2xx status, parse the JSON
//! response body.  Any non-2xx response is returned as an `Err`.
//!
//! [`enc`] percent-encodes target strings for safe embedding in URL path segments.

use anyhow::{Context, Result};
use serde_json::Value;

// ---------------------------------------------------------------------------
// HTTP helpers for communicating with ditto-mgmt
// ---------------------------------------------------------------------------

/// GET {url} → parsed JSON Value.
pub async fn mgmt_get(client: &reqwest::Client, url: &str) -> Result<Value> {
    let resp = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("GET {}", url))?;
    let status = resp.status();
    let body = resp.text().await?;
    if !status.is_success() {
        anyhow::bail!("GET {} → {}: {}", url, status, body);
    }
    serde_json::from_str(&body).context("parsing JSON response")
}

/// POST {url} with JSON body → parsed JSON Value.
pub async fn mgmt_post(client: &reqwest::Client, url: &str, body: Value) -> Result<Value> {
    let resp = client
        .post(url)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("POST {}", url))?;
    let status = resp.status();
    let text = resp.text().await?;
    if !status.is_success() {
        anyhow::bail!("POST {} → {}: {}", url, status, text);
    }
    serde_json::from_str(&text).context("parsing JSON response")
}

/// PUT {url} with JSON body → parsed JSON Value.
pub async fn mgmt_put(client: &reqwest::Client, url: &str, body: Value) -> Result<Value> {
    let resp = client
        .put(url)
        .json(&body)
        .send()
        .await
        .with_context(|| format!("PUT {}", url))?;
    let status = resp.status();
    let text = resp.text().await?;
    if !status.is_success() {
        anyhow::bail!("PUT {} → {}: {}", url, status, text);
    }
    serde_json::from_str(&text).context("parsing JSON response")
}

/// DELETE {url} → parsed JSON Value.
pub async fn mgmt_delete(client: &reqwest::Client, url: &str) -> Result<Value> {
    let resp = client
        .delete(url)
        .send()
        .await
        .with_context(|| format!("DELETE {}", url))?;
    let status = resp.status();
    let text = resp.text().await?;
    if !status.is_success() {
        anyhow::bail!("DELETE {} → {}: {}", url, status, text);
    }
    serde_json::from_str(&text).context("parsing JSON response")
}

/// URL-encode a target string for use in path segments.
/// e.g. "127.0.0.1:7779" → "127.0.0.1%3A7779"
pub fn enc(target: &str) -> String {
    percent_encode(target)
}

fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    async fn http_response(
        status: &'static str,
        body: &'static str,
    ) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = stream.read(&mut buf).await.unwrap();
            let request = String::from_utf8_lossy(&buf[..n]).to_string();
            let response = format!(
                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            request
        });

        (format!("http://{addr}/api"), handle)
    }

    #[test]
    fn enc_percent_encodes_path_reserved_bytes() {
        assert_eq!(enc("127.0.0.1:7779"), "127.0.0.1%3A7779");
        assert_eq!(enc("tenant a/key?#"), "tenant%20a%2Fkey%3F%23");
        assert_eq!(enc("safe-._~"), "safe-._~");
    }

    #[tokio::test]
    async fn mgmt_get_parses_success_json() {
        let client = reqwest::Client::new();
        let (url, request) = http_response("200 OK", r#"{"ok":true}"#).await;

        let value = mgmt_get(&client, &url).await.unwrap();
        assert_eq!(value["ok"], true);
        assert!(request.await.unwrap().starts_with("GET /api HTTP/1.1"));
    }

    #[tokio::test]
    async fn mgmt_get_reports_non_success_status_and_body() {
        let client = reqwest::Client::new();
        let (url, request) = http_response("502 Bad Gateway", r#"{"error":"down"}"#).await;

        let err = mgmt_get(&client, &url).await.unwrap_err();
        assert!(err.to_string().contains("502 Bad Gateway"));
        assert!(err.to_string().contains("down"));
        assert!(request.await.unwrap().starts_with("GET /api HTTP/1.1"));
    }

    #[tokio::test]
    async fn mgmt_post_put_and_delete_send_expected_methods() {
        let client = reqwest::Client::new();

        let (post_url, post_request) = http_response("200 OK", r#"{"posted":true}"#).await;
        assert_eq!(
            mgmt_post(&client, &post_url, serde_json::json!({"x": 1}))
                .await
                .unwrap()["posted"],
            true
        );
        assert!(post_request
            .await
            .unwrap()
            .starts_with("POST /api HTTP/1.1"));

        let (put_url, put_request) = http_response("200 OK", r#"{"put":true}"#).await;
        assert_eq!(
            mgmt_put(&client, &put_url, serde_json::json!({"x": 2}))
                .await
                .unwrap()["put"],
            true
        );
        assert!(put_request.await.unwrap().starts_with("PUT /api HTTP/1.1"));

        let (delete_url, delete_request) = http_response("200 OK", r#"{"deleted":true}"#).await;
        assert_eq!(
            mgmt_delete(&client, &delete_url).await.unwrap()["deleted"],
            true
        );
        assert!(delete_request
            .await
            .unwrap()
            .starts_with("DELETE /api HTTP/1.1"));
    }
}
