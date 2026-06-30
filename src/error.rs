use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RouterError {
    #[error("upstream not found")]
    UpstreamNotFound,

    #[error("no healthy upstream available for provider {0}")]
    NoHealthyUpstream(String),

    #[error("upstream error: {0}")]
    Upstream(String),

    #[error("upstream HTTP error: {status} {body}")]
    UpstreamHttp { status: u16, body: String },

    #[error("invalid request: {0}")]
    BadRequest(String),

    #[error("unauthorized: {0}")]
    Unauthorized(String),

    #[error("too many requests: {0}")]
    TooManyRequests(String),

    #[error("budget exceeded: {0}")]
    PaymentRequired(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

impl From<axum::http::header::InvalidHeaderValue> for RouterError {
    fn from(e: axum::http::header::InvalidHeaderValue) -> Self {
        RouterError::Internal(format!("invalid header value: {e}"))
    }
}

impl IntoResponse for RouterError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            RouterError::UpstreamNotFound => (StatusCode::NOT_FOUND, self.to_string()),
            RouterError::NoHealthyUpstream(_) => (StatusCode::SERVICE_UNAVAILABLE, self.to_string()),
            RouterError::BadRequest(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            RouterError::Unauthorized(_) => (StatusCode::UNAUTHORIZED, self.to_string()),
            RouterError::TooManyRequests(_) => (StatusCode::TOO_MANY_REQUESTS, self.to_string()),
            RouterError::PaymentRequired(_) => (StatusCode::PAYMENT_REQUIRED, self.to_string()),
            RouterError::NotFound(_) => (StatusCode::NOT_FOUND, self.to_string()),
            RouterError::UpstreamHttp { status, .. } => {
                let s = StatusCode::from_u16(*status).unwrap_or(StatusCode::BAD_GATEWAY);
                (s, self.to_string())
            }
            _ => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
        };

        let body = match &self {
            RouterError::UpstreamHttp { body, .. } => match serde_json::from_str::<serde_json::Value>(body) {
                Ok(v) => v,
                Err(_) => json!({ "error": { "message": message, "type": "upstream_error" } }),
            },
            _ => json!({ "error": { "message": message, "type": "router_error" } }),
        };

        (status, Json(body)).into_response()
    }
}

pub type RouterResult<T> = std::result::Result<T, RouterError>;
