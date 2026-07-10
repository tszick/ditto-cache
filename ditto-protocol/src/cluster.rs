use crate::{AdminRequest, AdminResponse, ClientRequest, ClientResponse};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterMessage {
    // --- Write path (two-phase) ---
    /// Primary -> all active nodes: "prepare this entry".
    Prepare {
        log_index: u64,
        key: String,
        value: Option<Bytes>,
        ttl_secs: Option<u64>,
    },
    /// Follower -> primary: prepare acknowledged.
    PrepareAck {
        log_index: u64,
        node_id: Uuid,
    },

    /// Primary -> all active nodes: "commit, make visible".
    Commit {
        log_index: u64,
    },
    /// Follower -> primary: commit acknowledged.
    CommitAck {
        log_index: u64,
        node_id: Uuid,
    },

    // --- Forwarding (non-primary receives client write) ---
    /// Non-primary -> primary: forward a client write.
    Forward {
        request: ClientRequest,
        origin_node: Uuid,
    },

    // --- Recovery / anti-entropy ---
    /// Recovering node -> any active node: "give me log entries".
    RequestLog {
        from_index: u64,
    },
    /// Active node -> recovering node: log entries response.
    LogEntries {
        entries: Vec<LogEntry>,
    },
    /// Recovering node -> gossip: "I'm fully synced".
    Synced {
        node_id: Uuid,
        last_applied: u64,
    },

    // --- Forward response (primary -> follower after handling forwarded write) ---
    /// Primary -> non-primary: result of a forwarded client write.
    ForwardResponse(ClientResponse),

    // --- Primary election override ---
    /// Broadcast by force-elected primary to all peers.
    ForcePrimary {
        node_id: Uuid,
    },

    // --- Admin (also on port 7779) ---
    Admin(AdminRequest),
    /// Node -> admin client: response to an Admin request.
    AdminResponse(Box<AdminResponse>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub index: u64,
    pub key: String,
    pub value: Option<Bytes>,
    pub ttl_secs: Option<u64>,
    pub ts_ms: u64,
}

impl From<LogEntry> for crate::pb::LogEntry {
    fn from(value: LogEntry) -> Self {
        Self {
            index: value.index,
            key: value.key,
            value: crate::opt_bytes(value.value),
            ttl_secs: crate::opt_u64(value.ttl_secs),
            ts_ms: value.ts_ms,
        }
    }
}

impl TryFrom<crate::pb::LogEntry> for LogEntry {
    type Error = anyhow::Error;

    fn try_from(value: crate::pb::LogEntry) -> anyhow::Result<Self> {
        Ok(Self {
            index: value.index,
            key: value.key,
            value: crate::from_opt_bytes(value.value),
            ttl_secs: crate::from_opt_u64(value.ttl_secs),
            ts_ms: value.ts_ms,
        })
    }
}

impl From<ClusterMessage> for crate::pb::ClusterMessage {
    fn from(value: ClusterMessage) -> Self {
        use crate::pb::cluster_message::Message;

        let message = match value {
            ClusterMessage::Prepare {
                log_index,
                key,
                value,
                ttl_secs,
            } => Message::Prepare(crate::pb::Prepare {
                log_index,
                key,
                value: crate::opt_bytes(value),
                ttl_secs: crate::opt_u64(ttl_secs),
            }),
            ClusterMessage::PrepareAck { log_index, node_id } => {
                Message::PrepareAck(crate::pb::LogAck {
                    log_index,
                    node_id: crate::uuid_to_string(node_id),
                })
            }
            ClusterMessage::Commit { log_index } => {
                Message::Commit(crate::pb::Commit { log_index })
            }
            ClusterMessage::CommitAck { log_index, node_id } => {
                Message::CommitAck(crate::pb::LogAck {
                    log_index,
                    node_id: crate::uuid_to_string(node_id),
                })
            }
            ClusterMessage::Forward {
                request,
                origin_node,
            } => Message::Forward(crate::pb::Forward {
                request: Some(request.into()),
                origin_node: crate::uuid_to_string(origin_node),
            }),
            ClusterMessage::RequestLog { from_index } => {
                Message::RequestLog(crate::pb::RequestLog { from_index })
            }
            ClusterMessage::LogEntries { entries } => Message::LogEntries(crate::pb::LogEntries {
                entries: entries.into_iter().map(Into::into).collect(),
            }),
            ClusterMessage::Synced {
                node_id,
                last_applied,
            } => Message::Synced(crate::pb::Synced {
                node_id: crate::uuid_to_string(node_id),
                last_applied,
            }),
            ClusterMessage::ForwardResponse(resp) => Message::ForwardResponse(resp.into()),
            ClusterMessage::ForcePrimary { node_id } => {
                Message::ForcePrimary(crate::pb::ForcePrimary {
                    node_id: crate::uuid_to_string(node_id),
                })
            }
            ClusterMessage::Admin(req) => Message::Admin(req.into()),
            ClusterMessage::AdminResponse(resp) => Message::AdminResponse((*resp).into()),
        };

        Self {
            message: Some(message),
        }
    }
}

impl TryFrom<crate::pb::ClusterMessage> for ClusterMessage {
    type Error = anyhow::Error;

    fn try_from(value: crate::pb::ClusterMessage) -> anyhow::Result<Self> {
        use crate::pb::cluster_message::Message;

        let message = value
            .message
            .ok_or_else(|| anyhow::anyhow!("missing cluster message payload"))?;

        Ok(match message {
            Message::Prepare(v) => Self::Prepare {
                log_index: v.log_index,
                key: v.key,
                value: crate::from_opt_bytes(v.value),
                ttl_secs: crate::from_opt_u64(v.ttl_secs),
            },
            Message::PrepareAck(v) => Self::PrepareAck {
                log_index: v.log_index,
                node_id: crate::uuid_from_string(v.node_id)?,
            },
            Message::Commit(v) => Self::Commit {
                log_index: v.log_index,
            },
            Message::CommitAck(v) => Self::CommitAck {
                log_index: v.log_index,
                node_id: crate::uuid_from_string(v.node_id)?,
            },
            Message::Forward(v) => Self::Forward {
                request: v
                    .request
                    .ok_or_else(|| anyhow::anyhow!("missing forwarded request"))?
                    .try_into()?,
                origin_node: crate::uuid_from_string(v.origin_node)?,
            },
            Message::RequestLog(v) => Self::RequestLog {
                from_index: v.from_index,
            },
            Message::LogEntries(v) => Self::LogEntries {
                entries: v
                    .entries
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<anyhow::Result<Vec<_>>>()?,
            },
            Message::Synced(v) => Self::Synced {
                node_id: crate::uuid_from_string(v.node_id)?,
                last_applied: v.last_applied,
            },
            Message::ForwardResponse(v) => Self::ForwardResponse(v.try_into()?),
            Message::ForcePrimary(v) => Self::ForcePrimary {
                node_id: crate::uuid_from_string(v.node_id)?,
            },
            Message::Admin(v) => Self::Admin(v.try_into()?),
            Message::AdminResponse(v) => Self::AdminResponse(Box::new(v.try_into()?)),
        })
    }
}
