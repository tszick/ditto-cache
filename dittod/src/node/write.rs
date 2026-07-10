use super::{CoordinatedWriteRequest, NodeHandle};
use crate::{
    config::WriteQuorumMode, network::cluster_server::send_cluster, store::kv_store::LimitError,
};
use bytes::Bytes;
use ditto_protocol::{ClientRequest, ClientResponse, ClusterMessage, ErrorCode, NodeStatus};
use std::{net::SocketAddr, time::Duration};
use tracing::{info, warn};

impl NodeHandle {
    pub(super) async fn delete_by_pattern(&self, pattern: String) -> ClientResponse {
        let keys = self.store.keys(Some(&pattern));
        let mut deleted = 0usize;
        for key in keys {
            let response = self.coordinate_write(key, None, None).await;
            match response {
                ClientResponse::Deleted => deleted += 1,
                ClientResponse::NotFound => {}
                ClientResponse::Error { .. } => return response,
                _ => {
                    return ClientResponse::Error {
                        code: ErrorCode::InternalError,
                        message: "Unexpected response while deleting by pattern".into(),
                    };
                }
            }
        }
        ClientResponse::PatternDeleted { deleted }
    }

    pub(super) async fn set_ttl_by_pattern(
        &self,
        pattern: String,
        ttl_secs: Option<u64>,
    ) -> ClientResponse {
        let keys = self.store.keys(Some(&pattern));
        let mut updated = 0usize;
        for key in keys {
            let Some(entry) = self.store.get(&key) else {
                continue;
            };
            let response = self
                .coordinate_write(key, Some(entry.value), ttl_secs)
                .await;
            match response {
                ClientResponse::Ok { .. } => updated += 1,
                ClientResponse::NotFound => {}
                ClientResponse::Error { .. } => return response,
                _ => {
                    return ClientResponse::Error {
                        code: ErrorCode::InternalError,
                        message: "Unexpected response while updating ttl by pattern".into(),
                    };
                }
            }
        }
        ClientResponse::PatternTtlUpdated { updated }
    }

    fn request_from_coordinated_write(
        key: String,
        request: &CoordinatedWriteRequest,
    ) -> ClientRequest {
        match request {
            CoordinatedWriteRequest::Set { value, ttl_secs } => ClientRequest::Set {
                key,
                value: value.clone(),
                ttl_secs: *ttl_secs,
                namespace: None,
            },
            CoordinatedWriteRequest::Delete => ClientRequest::Delete {
                key,
                namespace: None,
            },
            CoordinatedWriteRequest::SetNx { value, ttl_secs } => ClientRequest::SetNx {
                key,
                value: value.clone(),
                ttl_secs: *ttl_secs,
                namespace: None,
            },
            CoordinatedWriteRequest::Incr {
                delta,
                ttl_secs_on_create,
            } => ClientRequest::Incr {
                key,
                delta: *delta,
                ttl_secs_on_create: *ttl_secs_on_create,
                namespace: None,
            },
        }
    }

    fn namespace_quota_response(&self, namespace_name: &str, max: usize) -> ClientResponse {
        self.namespace_quota_reject_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        ClientResponse::Error {
            code: ErrorCode::NamespaceQuotaExceeded,
            message: format!("Namespace '{}' reached key limit ({})", namespace_name, max),
        }
    }

    fn limit_error_response(&self, value_len: usize, error: LimitError) -> ClientResponse {
        match error {
            LimitError::ValueTooLarge => ClientResponse::Error {
                code: ErrorCode::ValueTooLarge,
                message: format!(
                    "Value size {} bytes exceeds the configured limit",
                    value_len
                ),
            },
            LimitError::KeyLimitReached => ClientResponse::Error {
                code: ErrorCode::KeyLimitReached,
                message: "Cache is at the maximum key count limit".into(),
            },
        }
    }

    fn parse_counter_value(value: &Bytes) -> Result<i64, ()> {
        let text = std::str::from_utf8(value).map_err(|_| ())?;
        if text.is_empty() {
            return Err(());
        }

        let digits = if let Some(rest) = text.strip_prefix('-') {
            if rest.is_empty() {
                return Err(());
            }
            rest
        } else {
            text
        };

        if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(());
        }

        text.parse::<i64>().map_err(|_| ())
    }

    fn should_reject_namespace_create(&self, namespace: Option<&str>) -> Option<ClientResponse> {
        let namespace_name = namespace?;
        let max = self.config.lock().unwrap().tenancy.max_keys_per_namespace;
        if max > 0 && self.namespace_key_count(namespace_name) >= max {
            Some(self.namespace_quota_response(namespace_name, max))
        } else {
            None
        }
    }

    fn should_reject_store_limits(&self, key: &str, value: &Bytes) -> Option<ClientResponse> {
        self.store
            .check_limits(key, value)
            .err()
            .map(|err| self.limit_error_response(value.len(), err))
    }

    async fn forward_if_not_primary(&self, request: ClientRequest) -> Option<ClientResponse> {
        if !self.active_set.lock().await.is_primary() {
            let resp = self.forward_request_to_primary(request).await;
            if !matches!(
                &resp,
                ClientResponse::Error {
                    code: ErrorCode::NodeInactive,
                    ..
                }
            ) || !self.active_set.lock().await.is_primary()
            {
                return Some(resp);
            }
            info!("Took over as primary after stale primary refused write; retrying write locally");
        }
        None
    }

    pub(super) async fn coordinate_special_write(
        &self,
        key: String,
        namespace: Option<String>,
        request: CoordinatedWriteRequest,
    ) -> ClientResponse {
        let forward_request = Self::request_from_coordinated_write(key.clone(), &request);
        if let Some(resp) = self.forward_if_not_primary(forward_request.clone()).await {
            return resp;
        }

        let _guard = self.write_path_lock.lock().await;

        if let Some(resp) = self.forward_if_not_primary(forward_request).await {
            return resp;
        }

        let current = self.store.get(&key);
        match request {
            CoordinatedWriteRequest::Set { value, ttl_secs } => {
                let creates_key = current.is_none();
                if creates_key {
                    if let Some(resp) = self.should_reject_namespace_create(namespace.as_deref()) {
                        return resp;
                    }
                }
                if let Some(resp) = self.should_reject_store_limits(&key, &value) {
                    return resp;
                }

                match self
                    .replicate_write_unlocked(key, Some(value), ttl_secs)
                    .await
                {
                    Ok(version) => ClientResponse::Ok { version },
                    Err(resp) => resp,
                }
            }
            CoordinatedWriteRequest::Delete => {
                match self.replicate_write_unlocked(key, None, None).await {
                    Ok(_) => ClientResponse::Deleted,
                    Err(resp) => resp,
                }
            }
            CoordinatedWriteRequest::SetNx { value, ttl_secs } => {
                if let Some(existing) = current {
                    return ClientResponse::SetNx {
                        created: false,
                        version: existing.version,
                    };
                }
                if let Some(resp) = self.should_reject_namespace_create(namespace.as_deref()) {
                    return resp;
                }
                if let Some(resp) = self.should_reject_store_limits(&key, &value) {
                    return resp;
                }

                match self
                    .replicate_write_unlocked(key, Some(value), ttl_secs)
                    .await
                {
                    Ok(version) => ClientResponse::SetNx {
                        created: true,
                        version,
                    },
                    Err(resp) => resp,
                }
            }
            CoordinatedWriteRequest::Incr {
                delta,
                ttl_secs_on_create,
            } => {
                let (next_value, ttl_secs, creates_key) = match current {
                    Some(existing) => {
                        let base = match Self::parse_counter_value(&existing.value) {
                            Ok(value) => value,
                            Err(_) => {
                                return ClientResponse::Error {
                                    code: ErrorCode::TypeMismatch,
                                    message: "Stored value is not a valid int64 counter".into(),
                                };
                            }
                        };
                        let next_value = match base.checked_add(delta) {
                            Some(value) => value,
                            None => {
                                return ClientResponse::Error {
                                    code: ErrorCode::Overflow,
                                    message: "Counter result exceeds int64 range".into(),
                                };
                            }
                        };
                        (next_value, existing.ttl_remaining_secs(), false)
                    }
                    None => (delta, ttl_secs_on_create, true),
                };

                if creates_key {
                    if let Some(resp) = self.should_reject_namespace_create(namespace.as_deref()) {
                        return resp;
                    }
                }

                let encoded = Bytes::from(next_value.to_string().into_bytes());
                if let Some(resp) = self.should_reject_store_limits(&key, &encoded) {
                    return resp;
                }

                match self
                    .replicate_write_unlocked(key, Some(encoded), ttl_secs)
                    .await
                {
                    Ok(version) => ClientResponse::Counter {
                        value: next_value,
                        version,
                    },
                    Err(resp) => resp,
                }
            }
        }
    }

    pub(super) async fn coordinate_write(
        &self,
        key: String,
        value: Option<Bytes>,
        ttl_secs: Option<u64>,
    ) -> ClientResponse {
        let request = match value {
            Some(value) => CoordinatedWriteRequest::Set { value, ttl_secs },
            None => CoordinatedWriteRequest::Delete,
        };
        self.coordinate_special_write(key, None, request).await
    }

    async fn replicate_write_unlocked(
        &self,
        key: String,
        value: Option<Bytes>,
        ttl_secs: Option<u64>,
    ) -> Result<u64, ClientResponse> {
        let log_index = {
            let mut log = self.write_log.lock().await;
            log.append(key.clone(), value.clone(), ttl_secs)
        };

        let peers: Vec<SocketAddr> = {
            let set = self.active_set.lock().await;
            set.active_peers()
                .into_iter()
                .map(|n| SocketAddr::new(n.addr.ip(), n.cluster_port))
                .collect()
        };

        let prepare_msg = ClusterMessage::Prepare {
            log_index,
            key: key.clone(),
            value: value.clone(),
            ttl_secs,
        };

        let (timeout, quorum_mode) = {
            let cfg = self.config.lock().unwrap();
            (
                Duration::from_millis(cfg.replication.write_timeout_ms),
                cfg.replication.write_quorum_mode,
            )
        };
        let required_peer_acks = Self::required_prepare_peer_acks(quorum_mode, peers.len());
        let prepare_acks = self
            .broadcast_and_count_acks(&peers, &prepare_msg, timeout)
            .await;

        if prepare_acks < required_peer_acks {
            warn!("PREPARE timed out for log_index={}", log_index);
            return Err(ClientResponse::Error {
                code: ErrorCode::WriteTimeout,
                message: format!("Write timed out at PREPARE (index {})", log_index),
            });
        }

        let version = self.apply_locally(&key, value.clone(), ttl_secs, log_index);

        let commit_msg = ClusterMessage::Commit { log_index };
        let _ = self
            .broadcast_and_count_acks(&peers, &commit_msg, timeout)
            .await;

        self.write_log.lock().await.commit(log_index);
        self.active_set.lock().await.set_local_applied(log_index);

        Ok(version)
    }

    pub(super) async fn forward_request_to_primary(
        &self,
        request: ClientRequest,
    ) -> ClientResponse {
        let (primary_id, primary_addr) = {
            let set = self.active_set.lock().await;
            let primary_id = match set.primary_id() {
                Some(id) => id,
                None => {
                    return ClientResponse::Error {
                        code: ErrorCode::NoQuorum,
                        message: "No primary elected".into(),
                    };
                }
            };
            let addr = set
                .all_nodes()
                .into_iter()
                .find(|n| n.id == primary_id)
                .map(|n| SocketAddr::new(n.addr.ip(), n.cluster_port));
            (primary_id, addr)
        };

        let addr = match primary_addr {
            Some(a) => a,
            None => {
                return ClientResponse::Error {
                    code: ErrorCode::NoQuorum,
                    message: "Primary address unknown".into(),
                };
            }
        };

        let forward = ClusterMessage::Forward {
            request,
            origin_node: self.id,
        };

        let resp = match send_cluster(addr, &forward, self.tls_connector.as_ref()).await {
            Ok(Some(ClusterMessage::ForwardResponse(r))) => r,
            Err(e) => ClientResponse::Error {
                code: ErrorCode::InternalError,
                message: e.to_string(),
            },
            _ => ClientResponse::Error {
                code: ErrorCode::InternalError,
                message: "No ForwardResponse from primary".into(),
            },
        };

        if matches!(
            &resp,
            ClientResponse::Error {
                code: ErrorCode::NodeInactive,
                ..
            }
        ) {
            let mut set = self.active_set.lock().await;
            if let Some(mut info) = set.snapshot().into_iter().find(|n| n.id == primary_id) {
                info.status = NodeStatus::Inactive;
                set.upsert(info);
                info!(
                    "Primary {} reported NodeInactive; updated local view, triggering re-election",
                    primary_id
                );
            }
        }

        resp
    }

    pub(super) fn required_prepare_peer_acks(
        quorum_mode: WriteQuorumMode,
        peer_count: usize,
    ) -> usize {
        match quorum_mode {
            WriteQuorumMode::AllActive => peer_count,
            WriteQuorumMode::Majority => {
                let total_active = peer_count + 1;
                let required_total = (total_active / 2) + 1;
                required_total.saturating_sub(1)
            }
        }
    }

    async fn broadcast_and_count_acks(
        &self,
        peers: &[SocketAddr],
        msg: &ClusterMessage,
        timeout: Duration,
    ) -> usize {
        if peers.is_empty() {
            return 0;
        }

        let handles: Vec<_> = peers
            .iter()
            .map(|&addr| {
                let msg = msg.clone();
                let tls = self.tls_connector.clone();
                tokio::spawn(async move {
                    tokio::time::timeout(timeout, send_cluster(addr, &msg, tls.as_ref())).await
                })
            })
            .collect();

        let mut acked = 0usize;
        for h in handles {
            if matches!(h.await, Ok(Ok(Ok(_)))) {
                acked += 1;
            }
        }
        acked
    }
}
