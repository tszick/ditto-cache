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
