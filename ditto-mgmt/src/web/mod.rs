use axum::{
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};

const INDEX_HTML: &str = include_str!("index.html");

/// Serve the embedded management UI.
pub async fn serve_index() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        INDEX_HTML,
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
}
