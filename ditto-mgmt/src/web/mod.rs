use axum::{
    extract::Path,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};

const INDEX_HTML: &str = include_str!("index.html");
const DITTO_LOGO: &[u8] = include_bytes!("ditto.png");
const BOOTSTRAP_CSS: &[u8] = include_bytes!("assets/bootstrap.min.css");
const BOOTSTRAP_ICONS_CSS: &[u8] = include_bytes!("assets/bootstrap-icons.min.css");
const BOOTSTRAP_JS: &[u8] = include_bytes!("assets/bootstrap.bundle.min.js");
const BOOTSTRAP_ICONS_WOFF: &[u8] = include_bytes!("assets/fonts/bootstrap-icons.woff");
const BOOTSTRAP_ICONS_WOFF2: &[u8] = include_bytes!("assets/fonts/bootstrap-icons.woff2");

/// Serve the embedded management UI.
pub async fn serve_index() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        INDEX_HTML,
    )
        .into_response()
}

/// Serve the embedded Ditto logo used by the management UI.
pub async fn serve_logo() -> Response {
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "image/png"),
            (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
        ],
        DITTO_LOGO,
    )
        .into_response()
}

/// Serve vendored UI assets so the management console does not depend on third-party CDNs.
pub async fn serve_asset(Path(path): Path<String>) -> Response {
    match path.as_str() {
        "bootstrap.min.css" => serve_bytes("text/css; charset=utf-8", BOOTSTRAP_CSS),
        "bootstrap-icons.min.css" => {
            serve_bytes("text/css; charset=utf-8", BOOTSTRAP_ICONS_CSS)
        }
        "bootstrap.bundle.min.js" => {
            serve_bytes("application/javascript; charset=utf-8", BOOTSTRAP_JS)
        }
        "fonts/bootstrap-icons.woff" => serve_bytes("font/woff", BOOTSTRAP_ICONS_WOFF),
        "fonts/bootstrap-icons.woff2" => serve_bytes("font/woff2", BOOTSTRAP_ICONS_WOFF2),
        _ => (StatusCode::NOT_FOUND, "asset not found").into_response(),
    }
}

fn serve_bytes(content_type: &'static str, body: &'static [u8]) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, content_type)],
        body,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    #[tokio::test]
    async fn serve_index_returns_embedded_html_with_content_type() {
        let response = serve_index().await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/html; charset=utf-8"
        );

        let body = to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("read body bytes");
        let html = String::from_utf8(body.to_vec()).expect("html should be utf8");
        assert!(html.contains("<!doctype html>") || html.contains("<!DOCTYPE html>"));
    }

    #[tokio::test]
    async fn serve_logo_returns_embedded_png() {
        let response = serve_logo().await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "image/png"
        );

        let body = to_bytes(response.into_body(), 256 * 1024)
            .await
            .expect("read logo bytes");
        assert!(body.starts_with(b"\x89PNG\r\n\x1a\n"));
    }

    #[tokio::test]
    async fn serve_asset_returns_vendored_bootstrap_css() {
        let response = serve_asset(Path("bootstrap.min.css".to_string())).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/css; charset=utf-8"
        );

        let body = to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("read asset bytes");
        assert!(body.starts_with(b"@charset \"UTF-8\";"));
    }

    #[tokio::test]
    async fn serve_asset_returns_not_found_for_unknown_file() {
        let response = serve_asset(Path("missing.js".to_string())).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
