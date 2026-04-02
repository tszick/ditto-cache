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
