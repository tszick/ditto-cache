use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientRequest {
    Get {
        key: String,
        namespace: Option<String>,
    },
    Set {
        key: String,
        value: Bytes,
        ttl_secs: Option<u64>,
        namespace: Option<String>,
    },
    Delete {
        key: String,
        namespace: Option<String>,
    },
    Ping,
    Auth {
        token: String,
    },
    Watch {
        key: String,
        namespace: Option<String>,
    },
    Unwatch {
        key: String,
        namespace: Option<String>,
    },
    DeleteByPattern {
        pattern: String,
        namespace: Option<String>,
    },
    SetNx {
        key: String,
        value: Bytes,
        ttl_secs: Option<u64>,
        namespace: Option<String>,
    },
    Incr {
        key: String,
        delta: i64,
        ttl_secs_on_create: Option<u64>,
        namespace: Option<String>,
    },
    SetTtlByPattern {
        pattern: String,
        ttl_secs: Option<u64>,
        namespace: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientResponse {
    Value {
        key: String,
        value: Bytes,
        version: u64,
    },
    Ok {
        version: u64,
    },
    Deleted,
    NotFound,
    Pong,
    AuthOk,
    Error {
        code: ErrorCode,
        message: String,
    },
    Watching,
    Unwatched,
    WatchEvent {
        key: String,
        value: Option<Bytes>,
        version: u64,
    },
    PatternDeleted {
        deleted: usize,
    },
    PatternTtlUpdated {
        updated: usize,
    },
    SetNx {
        created: bool,
        version: u64,
    },
    Counter {
        value: i64,
        version: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ErrorCode {
    NodeInactive,
    NoQuorum,
    KeyNotFound,
    InternalError,
    WriteTimeout,
    ValueTooLarge,
    KeyLimitReached,
    RateLimited,
    CircuitOpen,
    NamespaceQuotaExceeded,
    AuthFailed,
    AccessDenied,
    UnsupportedRequest,
    TypeMismatch,
    Overflow,
}

impl From<ErrorCode> for crate::pb::ErrorCode {
    fn from(value: ErrorCode) -> Self {
        match value {
            ErrorCode::NodeInactive => Self::NodeInactive,
            ErrorCode::NoQuorum => Self::NoQuorum,
            ErrorCode::KeyNotFound => Self::KeyNotFound,
            ErrorCode::InternalError => Self::InternalError,
            ErrorCode::WriteTimeout => Self::WriteTimeout,
            ErrorCode::ValueTooLarge => Self::ValueTooLarge,
            ErrorCode::KeyLimitReached => Self::KeyLimitReached,
            ErrorCode::RateLimited => Self::RateLimited,
            ErrorCode::CircuitOpen => Self::CircuitOpen,
            ErrorCode::NamespaceQuotaExceeded => Self::NamespaceQuotaExceeded,
            ErrorCode::AuthFailed => Self::AuthFailed,
            ErrorCode::AccessDenied => Self::AccessDenied,
            ErrorCode::UnsupportedRequest => Self::UnsupportedRequest,
            ErrorCode::TypeMismatch => Self::TypeMismatch,
            ErrorCode::Overflow => Self::Overflow,
        }
    }
}

impl TryFrom<i32> for ErrorCode {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> anyhow::Result<Self> {
        match crate::pb::ErrorCode::try_from(value)? {
            crate::pb::ErrorCode::NodeInactive => Ok(Self::NodeInactive),
            crate::pb::ErrorCode::NoQuorum => Ok(Self::NoQuorum),
            crate::pb::ErrorCode::KeyNotFound => Ok(Self::KeyNotFound),
            crate::pb::ErrorCode::InternalError => Ok(Self::InternalError),
            crate::pb::ErrorCode::WriteTimeout => Ok(Self::WriteTimeout),
            crate::pb::ErrorCode::ValueTooLarge => Ok(Self::ValueTooLarge),
            crate::pb::ErrorCode::KeyLimitReached => Ok(Self::KeyLimitReached),
            crate::pb::ErrorCode::RateLimited => Ok(Self::RateLimited),
            crate::pb::ErrorCode::CircuitOpen => Ok(Self::CircuitOpen),
            crate::pb::ErrorCode::NamespaceQuotaExceeded => Ok(Self::NamespaceQuotaExceeded),
            crate::pb::ErrorCode::AuthFailed => Ok(Self::AuthFailed),
            crate::pb::ErrorCode::AccessDenied => Ok(Self::AccessDenied),
            crate::pb::ErrorCode::UnsupportedRequest => Ok(Self::UnsupportedRequest),
            crate::pb::ErrorCode::TypeMismatch => Ok(Self::TypeMismatch),
            crate::pb::ErrorCode::Overflow => Ok(Self::Overflow),
        }
    }
}

impl From<ClientRequest> for crate::pb::ClientRequest {
    fn from(value: ClientRequest) -> Self {
        use crate::pb::client_request::Request;

        let request = match value {
            ClientRequest::Get { key, namespace } => Request::Get(crate::pb::KeyNamespace {
                key,
                namespace: crate::opt_string(namespace),
            }),
            ClientRequest::Set {
                key,
                value,
                ttl_secs,
                namespace,
            } => Request::Set(crate::pb::SetRequest {
                key,
                value: value.to_vec(),
                ttl_secs: crate::opt_u64(ttl_secs),
                namespace: crate::opt_string(namespace),
            }),
            ClientRequest::Delete { key, namespace } => Request::Delete(crate::pb::KeyNamespace {
                key,
                namespace: crate::opt_string(namespace),
            }),
            ClientRequest::Ping => Request::Ping(crate::pb::Empty {}),
            ClientRequest::Auth { token } => Request::Auth(crate::pb::AuthRequest { token }),
            ClientRequest::Watch { key, namespace } => Request::Watch(crate::pb::KeyNamespace {
                key,
                namespace: crate::opt_string(namespace),
            }),
            ClientRequest::Unwatch { key, namespace } => {
                Request::Unwatch(crate::pb::KeyNamespace {
                    key,
                    namespace: crate::opt_string(namespace),
                })
            }
            ClientRequest::DeleteByPattern { pattern, namespace } => {
                Request::DeleteByPattern(crate::pb::PatternNamespace {
                    pattern,
                    namespace: crate::opt_string(namespace),
                })
            }
            ClientRequest::SetNx {
                key,
                value,
                ttl_secs,
                namespace,
            } => Request::SetNx(crate::pb::SetRequest {
                key,
                value: value.to_vec(),
                ttl_secs: crate::opt_u64(ttl_secs),
                namespace: crate::opt_string(namespace),
            }),
            ClientRequest::Incr {
                key,
                delta,
                ttl_secs_on_create,
                namespace,
            } => Request::Incr(crate::pb::IncrRequest {
                key,
                delta: Some(delta),
                ttl_secs_on_create: crate::opt_u64(ttl_secs_on_create),
                namespace: crate::opt_string(namespace),
            }),
            ClientRequest::SetTtlByPattern {
                pattern,
                ttl_secs,
                namespace,
            } => Request::SetTtlByPattern(crate::pb::SetTtlByPatternRequest {
                pattern,
                ttl_secs: crate::opt_u64(ttl_secs),
                namespace: crate::opt_string(namespace),
            }),
        };

        Self {
            request: Some(request),
        }
    }
}

impl TryFrom<crate::pb::ClientRequest> for ClientRequest {
    type Error = anyhow::Error;

    fn try_from(value: crate::pb::ClientRequest) -> anyhow::Result<Self> {
        use crate::pb::client_request::Request;

        let request = value
            .request
            .ok_or_else(|| anyhow::anyhow!("missing client request payload"))?;

        Ok(match request {
            Request::Get(v) => Self::Get {
                key: v.key,
                namespace: crate::from_opt_string(v.namespace),
            },
            Request::Set(v) => Self::Set {
                key: v.key,
                value: Bytes::from(v.value),
                ttl_secs: crate::from_opt_u64(v.ttl_secs),
                namespace: crate::from_opt_string(v.namespace),
            },
            Request::Delete(v) => Self::Delete {
                key: v.key,
                namespace: crate::from_opt_string(v.namespace),
            },
            Request::Ping(_) => Self::Ping,
            Request::Auth(v) => Self::Auth { token: v.token },
            Request::Watch(v) => Self::Watch {
                key: v.key,
                namespace: crate::from_opt_string(v.namespace),
            },
            Request::Unwatch(v) => Self::Unwatch {
                key: v.key,
                namespace: crate::from_opt_string(v.namespace),
            },
            Request::DeleteByPattern(v) => Self::DeleteByPattern {
                pattern: v.pattern,
                namespace: crate::from_opt_string(v.namespace),
            },
            Request::SetNx(v) => Self::SetNx {
                key: v.key,
                value: Bytes::from(v.value),
                ttl_secs: crate::from_opt_u64(v.ttl_secs),
                namespace: crate::from_opt_string(v.namespace),
            },
            Request::Incr(v) => Self::Incr {
                key: v.key,
                delta: v.delta.unwrap_or(1),
                ttl_secs_on_create: crate::from_opt_u64(v.ttl_secs_on_create),
                namespace: crate::from_opt_string(v.namespace),
            },
            Request::SetTtlByPattern(v) => Self::SetTtlByPattern {
                pattern: v.pattern,
                ttl_secs: crate::from_opt_u64(v.ttl_secs),
                namespace: crate::from_opt_string(v.namespace),
            },
        })
    }
}

impl From<ClientResponse> for crate::pb::ClientResponse {
    fn from(value: ClientResponse) -> Self {
        use crate::pb::client_response::Response;

        let response = match value {
            ClientResponse::Value {
                key,
                value,
                version,
            } => Response::Value(crate::pb::ValueResponse {
                key,
                value: value.to_vec(),
                version,
            }),
            ClientResponse::Ok { version } => {
                Response::Ok(crate::pb::VersionResponse { version })
            }
            ClientResponse::Deleted => Response::Deleted(crate::pb::Empty {}),
            ClientResponse::NotFound => Response::NotFound(crate::pb::Empty {}),
            ClientResponse::Pong => Response::Pong(crate::pb::Empty {}),
            ClientResponse::AuthOk => Response::AuthOk(crate::pb::Empty {}),
            ClientResponse::Error { code, message } => Response::Error(crate::pb::ErrorResponse {
                code: crate::pb::ErrorCode::from(code) as i32,
                message,
            }),
            ClientResponse::Watching => Response::Watching(crate::pb::Empty {}),
            ClientResponse::Unwatched => Response::Unwatched(crate::pb::Empty {}),
            ClientResponse::WatchEvent {
                key,
                value,
                version,
            } => Response::WatchEvent(crate::pb::WatchEvent {
                key,
                value: crate::opt_bytes(value),
                version,
            }),
            ClientResponse::PatternDeleted { deleted } => {
                Response::PatternDeleted(crate::pb::CountResponse {
                    count: deleted as u64,
                })
            }
            ClientResponse::PatternTtlUpdated { updated } => {
                Response::PatternTtlUpdated(crate::pb::CountResponse {
                    count: updated as u64,
                })
            }
            ClientResponse::SetNx { created, version } => {
                Response::SetNx(crate::pb::SetNxResponse { created, version })
            }
            ClientResponse::Counter { value, version } => {
                Response::Counter(crate::pb::CounterResponse { value, version })
            }
        };

        Self {
            response: Some(response),
        }
    }
}

impl TryFrom<crate::pb::ClientResponse> for ClientResponse {
    type Error = anyhow::Error;

    fn try_from(value: crate::pb::ClientResponse) -> anyhow::Result<Self> {
        use crate::pb::client_response::Response;

        let response = value
            .response
            .ok_or_else(|| anyhow::anyhow!("missing client response payload"))?;

        Ok(match response {
            Response::Value(v) => Self::Value {
                key: v.key,
                value: Bytes::from(v.value),
                version: v.version,
            },
            Response::Ok(v) => Self::Ok { version: v.version },
            Response::Deleted(_) => Self::Deleted,
            Response::NotFound(_) => Self::NotFound,
            Response::Pong(_) => Self::Pong,
            Response::AuthOk(_) => Self::AuthOk,
            Response::Error(v) => Self::Error {
                code: v.code.try_into()?,
                message: v.message,
            },
            Response::Watching(_) => Self::Watching,
            Response::Unwatched(_) => Self::Unwatched,
            Response::WatchEvent(v) => Self::WatchEvent {
                key: v.key,
                value: crate::from_opt_bytes(v.value),
                version: v.version,
            },
            Response::PatternDeleted(v) => Self::PatternDeleted {
                deleted: usize::try_from(v.count)?,
            },
            Response::PatternTtlUpdated(v) => Self::PatternTtlUpdated {
                updated: usize::try_from(v.count)?,
            },
            Response::SetNx(v) => Self::SetNx {
                created: v.created,
                version: v.version,
            },
            Response::Counter(v) => Self::Counter {
                value: v.value,
                version: v.version,
            },
        })
    }
}
