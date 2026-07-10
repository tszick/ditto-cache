use super::{ClientRequestSource, CoordinatedWriteRequest, NodeHandle};
use ditto_protocol::{ClientRequest, ClientResponse, ErrorCode};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Instant,
};

impl NodeHandle {
    #[allow(dead_code)]
    pub async fn handle_client(self: &Arc<Self>, req: ClientRequest) -> ClientResponse {
        self.handle_client_with_source(req, ClientRequestSource::Internal)
            .await
    }

    pub async fn handle_client_tcp(self: &Arc<Self>, req: ClientRequest) -> ClientResponse {
        self.handle_client_with_source(req, ClientRequestSource::Tcp)
            .await
    }

    pub async fn handle_client_http(self: &Arc<Self>, req: ClientRequest) -> ClientResponse {
        self.handle_client_with_source(req, ClientRequestSource::Http)
            .await
    }

    pub(super) async fn handle_client_with_source(
        self: &Arc<Self>,
        req: ClientRequest,
        source: ClientRequestSource,
    ) -> ClientResponse {
        let started = Instant::now();
        let namespace_label = self.request_namespace_label(&req);
        let hot_key_label = self.request_hot_key_label(&req, namespace_label.as_deref());
        let mut should_record_circuit_result = false;
        let response = if !self.active.load(Ordering::Relaxed) {
            ClientResponse::Error {
                code: ErrorCode::NodeInactive,
                message: "Node is inactive (maintenance mode)".into(),
            }
        } else if !self.allow_by_rate_limit() {
            ClientResponse::Error {
                code: ErrorCode::RateLimited,
                message: "Request throttled by rate limiter".into(),
            }
        } else if !self.allow_by_circuit_breaker() {
            ClientResponse::Error {
                code: ErrorCode::CircuitOpen,
                message: "Request rejected by circuit breaker".into(),
            }
        } else {
            should_record_circuit_result = true;
            self.dispatch_client_request(req).await
        };
        if should_record_circuit_result {
            self.record_circuit_result(&response);
        }
        self.observe_client_response_metrics(
            source,
            started.elapsed(),
            &response,
            namespace_label.as_deref(),
            hot_key_label.as_deref(),
        );
        response
    }

    async fn dispatch_client_request(self: &Arc<Self>, req: ClientRequest) -> ClientResponse {
        match req {
            ClientRequest::Ping => ClientResponse::Pong,
            ClientRequest::Auth { .. } => ClientResponse::Error {
                code: ErrorCode::InternalError,
                message: "Authentication is already complete or invalid in this context".into(),
            },

            ClientRequest::Get { key, namespace } => match self.namespace_context(namespace) {
                Ok(ns) => {
                    let namespaced_key = self.namespaced_key(&ns, &key);
                    let local = self
                        .handle_get_with_single_flight(namespaced_key, key.clone())
                        .await;
                    if matches!(local, ClientResponse::NotFound) {
                        self.maybe_read_repair_on_miss(ns, key)
                            .await
                            .unwrap_or(local)
                    } else {
                        local
                    }
                }
                Err(err) => err,
            },

            ClientRequest::Set {
                key,
                value,
                ttl_secs,
                namespace,
            } => match self.namespace_context(namespace) {
                Ok(ns) => {
                    let namespaced_key = self.namespaced_key(&ns, &key);
                    self.coordinate_special_write(
                        namespaced_key,
                        ns,
                        CoordinatedWriteRequest::Set { value, ttl_secs },
                    )
                    .await
                }
                Err(err) => err,
            },

            ClientRequest::SetNx {
                key,
                value,
                ttl_secs,
                namespace,
            } => match self.namespace_context(namespace) {
                Ok(ns) => {
                    let namespaced_key = self.namespaced_key(&ns, &key);
                    self.coordinate_special_write(
                        namespaced_key,
                        ns,
                        CoordinatedWriteRequest::SetNx { value, ttl_secs },
                    )
                    .await
                }
                Err(err) => err,
            },

            ClientRequest::Incr {
                key,
                delta,
                ttl_secs_on_create,
                namespace,
            } => match self.namespace_context(namespace) {
                Ok(ns) => {
                    let namespaced_key = self.namespaced_key(&ns, &key);
                    self.coordinate_special_write(
                        namespaced_key,
                        ns,
                        CoordinatedWriteRequest::Incr {
                            delta,
                            ttl_secs_on_create,
                        },
                    )
                    .await
                }
                Err(err) => err,
            },

            ClientRequest::Delete { key, namespace } => match self.namespace_context(namespace) {
                Ok(ns) => {
                    self.coordinate_special_write(
                        self.namespaced_key(&ns, &key),
                        ns,
                        CoordinatedWriteRequest::Delete,
                    )
                    .await
                }
                Err(err) => err,
            },

            ClientRequest::DeleteByPattern { pattern, namespace } => {
                match self.namespace_context(namespace) {
                    Ok(ns) => {
                        self.delete_by_pattern(self.namespaced_pattern(&ns, &pattern))
                            .await
                    }
                    Err(err) => err,
                }
            }

            ClientRequest::SetTtlByPattern {
                pattern,
                ttl_secs,
                namespace,
            } => match self.namespace_context(namespace) {
                Ok(ns) => {
                    self.set_ttl_by_pattern(self.namespaced_pattern(&ns, &pattern), ttl_secs)
                        .await
                }
                Err(err) => err,
            },

            ClientRequest::Watch { .. } | ClientRequest::Unwatch { .. } => ClientResponse::Error {
                code: ErrorCode::InternalError,
                message: "Watch/Unwatch must be handled at the connection level".into(),
            },
        }
    }
}
