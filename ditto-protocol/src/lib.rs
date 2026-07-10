//! Ditto wire protocol — shared types and framing helpers.
//!
//! All four protocol layers are defined here:
//! - **Client protocol** (port 7777 TCP / 7778 HTTP): `ClientRequest`, `ClientResponse`
//! - **Cluster protocol** (port 7779 TCP): `ClusterMessage`, `LogEntry`
//! - **Gossip protocol** (port 7780 UDP): `GossipMessage`
//! - **Admin protocol** (embedded in `ClusterMessage::Admin`): `AdminRequest`, `AdminResponse`
//!
//! Messages are serialised into a prost envelope and framed with a 4-byte big-endian
//! length prefix using the [`encode`] / [`decode`] helpers.

use bytes::Bytes;
use std::net::SocketAddr;
use uuid::Uuid;

mod admin;
mod client;
mod cluster;
mod common;
mod frame;
mod gossip;

pub use admin::{
    AdminRequest, AdminResponse, HotKeyUsage, NamespaceLatencySummary, NamespaceQuotaUsage,
    NodeStats,
};
pub use client::{ClientRequest, ClientResponse, ErrorCode};
pub use cluster::{ClusterMessage, LogEntry};
pub use common::{NodeInfo, NodeStatus};
pub use frame::{decode, decode_gossip, encode, encode_gossip, WireMessage};
pub use gossip::GossipMessage;

#[allow(clippy::large_enum_variant)]
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/ditto.protocol.v1.rs"));
}

impl WireMessage for ClientRequest {
    fn into_payload(self) -> pb::envelope::Payload {
        pb::envelope::Payload::ClientRequest(self.into())
    }

    fn from_payload(payload: pb::envelope::Payload) -> anyhow::Result<Self> {
        match payload {
            pb::envelope::Payload::ClientRequest(msg) => msg.try_into(),
            _ => anyhow::bail!("unexpected protobuf payload for ClientRequest"),
        }
    }
}

impl WireMessage for ClientResponse {
    fn into_payload(self) -> pb::envelope::Payload {
        pb::envelope::Payload::ClientResponse(self.into())
    }

    fn from_payload(payload: pb::envelope::Payload) -> anyhow::Result<Self> {
        match payload {
            pb::envelope::Payload::ClientResponse(msg) => msg.try_into(),
            _ => anyhow::bail!("unexpected protobuf payload for ClientResponse"),
        }
    }
}

impl WireMessage for ClusterMessage {
    fn into_payload(self) -> pb::envelope::Payload {
        pb::envelope::Payload::ClusterMessage(self.into())
    }

    fn from_payload(payload: pb::envelope::Payload) -> anyhow::Result<Self> {
        match payload {
            pb::envelope::Payload::ClusterMessage(msg) => msg.try_into(),
            _ => anyhow::bail!("unexpected protobuf payload for ClusterMessage"),
        }
    }
}

impl WireMessage for GossipMessage {
    fn into_payload(self) -> pb::envelope::Payload {
        pb::envelope::Payload::GossipMessage(self.into())
    }

    fn from_payload(payload: pb::envelope::Payload) -> anyhow::Result<Self> {
        match payload {
            pb::envelope::Payload::GossipMessage(msg) => msg.try_into(),
            _ => anyhow::bail!("unexpected protobuf payload for GossipMessage"),
        }
    }
}

pub(crate) fn opt_string(value: Option<String>) -> Option<pb::OptionalString> {
    value.map(|value| pb::OptionalString { value })
}

pub(crate) fn from_opt_string(value: Option<pb::OptionalString>) -> Option<String> {
    value.map(|value| value.value)
}

pub(crate) fn opt_u64(value: Option<u64>) -> Option<pb::OptionalUint64> {
    value.map(|value| pb::OptionalUint64 { value })
}

pub(crate) fn from_opt_u64(value: Option<pb::OptionalUint64>) -> Option<u64> {
    value.map(|value| value.value)
}

pub(crate) fn opt_bytes(value: Option<Bytes>) -> Option<pb::OptionalBytes> {
    value.map(|value| pb::OptionalBytes {
        value: value.to_vec(),
    })
}

pub(crate) fn from_opt_bytes(value: Option<pb::OptionalBytes>) -> Option<Bytes> {
    value.map(|value| Bytes::from(value.value))
}

pub(crate) fn uuid_to_string(value: Uuid) -> String {
    value.to_string()
}

pub(crate) fn uuid_from_string(value: String) -> anyhow::Result<Uuid> {
    Ok(Uuid::parse_str(&value)?)
}

pub(crate) fn socket_to_string(value: SocketAddr) -> String {
    value.to_string()
}

pub(crate) fn socket_from_string(value: String) -> anyhow::Result<SocketAddr> {
    Ok(value.parse()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use prost::Message;
    use std::net::SocketAddr;
    use crate::frame::PROTOCOL_VERSION;

    fn payload(frame: &[u8]) -> &[u8] {
        let len = u32::from_be_bytes(frame[0..4].try_into().unwrap()) as usize;
        assert_eq!(len, frame.len() - 4);
        &frame[4..]
    }

    #[test]
    fn client_requests_round_trip_through_envelope() {
        let cases = vec![
            ClientRequest::Get {
                key: "k".into(),
                namespace: Some("tenant-a".into()),
            },
            ClientRequest::Set {
                key: "k".into(),
                value: Bytes::from_static(b"value"),
                ttl_secs: Some(60),
                namespace: Some("tenant-a".into()),
            },
            ClientRequest::Delete {
                key: "k".into(),
                namespace: None,
            },
            ClientRequest::Ping,
            ClientRequest::Auth {
                token: "secret".into(),
            },
            ClientRequest::Watch {
                key: "k".into(),
                namespace: Some("tenant-a".into()),
            },
            ClientRequest::Unwatch {
                key: "k".into(),
                namespace: None,
            },
            ClientRequest::DeleteByPattern {
                pattern: "tenant:*".into(),
                namespace: Some("tenant-a".into()),
            },
            ClientRequest::SetNx {
                key: "lock".into(),
                value: Bytes::from_static(b"holder-a"),
                ttl_secs: Some(30),
                namespace: Some("tenant-a".into()),
            },
            ClientRequest::Incr {
                key: "counter".into(),
                delta: -2,
                ttl_secs_on_create: Some(15),
                namespace: None,
            },
            ClientRequest::SetTtlByPattern {
                pattern: "tenant:*".into(),
                ttl_secs: None,
                namespace: Some("tenant-a".into()),
            },
        ];

        for request in cases {
            let encoded = encode(&request).unwrap();
            let decoded: ClientRequest = decode(payload(&encoded), 1024).unwrap();
            match (request, decoded) {
                (
                    ClientRequest::Get {
                        key: a,
                        namespace: an,
                    },
                    ClientRequest::Get {
                        key: b,
                        namespace: bn,
                    },
                ) => {
                    assert_eq!((a, an), (b, bn));
                }
                (
                    ClientRequest::Set {
                        key: a,
                        value: av,
                        ttl_secs: at,
                        namespace: an,
                    },
                    ClientRequest::Set {
                        key: b,
                        value: bv,
                        ttl_secs: bt,
                        namespace: bn,
                    },
                ) => assert_eq!((a, av, at, an), (b, bv, bt, bn)),
                (
                    ClientRequest::Delete {
                        key: a,
                        namespace: an,
                    },
                    ClientRequest::Delete {
                        key: b,
                        namespace: bn,
                    },
                ) => {
                    assert_eq!((a, an), (b, bn));
                }
                (ClientRequest::Ping, ClientRequest::Ping) => {}
                (ClientRequest::Auth { token: a }, ClientRequest::Auth { token: b }) => {
                    assert_eq!(a, b)
                }
                (
                    ClientRequest::Watch {
                        key: a,
                        namespace: an,
                    },
                    ClientRequest::Watch {
                        key: b,
                        namespace: bn,
                    },
                ) => {
                    assert_eq!((a, an), (b, bn));
                }
                (
                    ClientRequest::Unwatch {
                        key: a,
                        namespace: an,
                    },
                    ClientRequest::Unwatch {
                        key: b,
                        namespace: bn,
                    },
                ) => {
                    assert_eq!((a, an), (b, bn));
                }
                (
                    ClientRequest::DeleteByPattern {
                        pattern: a,
                        namespace: an,
                    },
                    ClientRequest::DeleteByPattern {
                        pattern: b,
                        namespace: bn,
                    },
                ) => assert_eq!((a, an), (b, bn)),
                (
                    ClientRequest::SetNx {
                        key: a,
                        value: av,
                        ttl_secs: at,
                        namespace: an,
                    },
                    ClientRequest::SetNx {
                        key: b,
                        value: bv,
                        ttl_secs: bt,
                        namespace: bn,
                    },
                ) => assert_eq!((a, av, at, an), (b, bv, bt, bn)),
                (
                    ClientRequest::Incr {
                        key: a,
                        delta: ad,
                        ttl_secs_on_create: at,
                        namespace: an,
                    },
                    ClientRequest::Incr {
                        key: b,
                        delta: bd,
                        ttl_secs_on_create: bt,
                        namespace: bn,
                    },
                ) => assert_eq!((a, ad, at, an), (b, bd, bt, bn)),
                (
                    ClientRequest::SetTtlByPattern {
                        pattern: a,
                        ttl_secs: at,
                        namespace: an,
                    },
                    ClientRequest::SetTtlByPattern {
                        pattern: b,
                        ttl_secs: bt,
                        namespace: bn,
                    },
                ) => assert_eq!((a, at, an), (b, bt, bn)),
                (a, b) => panic!("roundtrip changed request: {a:?} -> {b:?}"),
            }
        }
    }

    #[test]
    fn client_responses_round_trip_through_envelope() {
        let cases = vec![
            ClientResponse::Value {
                key: "k".into(),
                value: Bytes::from_static(b"value"),
                version: 2,
            },
            ClientResponse::Ok { version: 3 },
            ClientResponse::Deleted,
            ClientResponse::NotFound,
            ClientResponse::Pong,
            ClientResponse::AuthOk,
            ClientResponse::Error {
                code: ErrorCode::NamespaceQuotaExceeded,
                message: "quota".into(),
            },
            ClientResponse::Watching,
            ClientResponse::Unwatched,
            ClientResponse::WatchEvent {
                key: "k".into(),
                value: Some(Bytes::from_static(b"value")),
                version: 4,
            },
            ClientResponse::WatchEvent {
                key: "k".into(),
                value: None,
                version: 5,
            },
            ClientResponse::PatternDeleted { deleted: 6 },
            ClientResponse::PatternTtlUpdated { updated: 7 },
            ClientResponse::SetNx {
                created: true,
                version: 8,
            },
            ClientResponse::Counter {
                value: -9,
                version: 10,
            },
        ];

        for response in cases {
            let encoded = encode(&response).unwrap();
            let decoded: ClientResponse = decode(payload(&encoded), 1024).unwrap();
            match (response, decoded) {
                (
                    ClientResponse::Value {
                        key: a,
                        value: av,
                        version: aver,
                    },
                    ClientResponse::Value {
                        key: b,
                        value: bv,
                        version: bver,
                    },
                ) => assert_eq!((a, av, aver), (b, bv, bver)),
                (ClientResponse::Ok { version: a }, ClientResponse::Ok { version: b }) => {
                    assert_eq!(a, b)
                }
                (ClientResponse::Deleted, ClientResponse::Deleted)
                | (ClientResponse::NotFound, ClientResponse::NotFound)
                | (ClientResponse::Pong, ClientResponse::Pong)
                | (ClientResponse::AuthOk, ClientResponse::AuthOk)
                | (ClientResponse::Watching, ClientResponse::Watching)
                | (ClientResponse::Unwatched, ClientResponse::Unwatched) => {}
                (
                    ClientResponse::Error {
                        code: ErrorCode::NamespaceQuotaExceeded,
                        message: a,
                    },
                    ClientResponse::Error {
                        code: ErrorCode::NamespaceQuotaExceeded,
                        message: b,
                    },
                ) => assert_eq!(a, b),
                (
                    ClientResponse::WatchEvent {
                        key: a,
                        value: av,
                        version: aver,
                    },
                    ClientResponse::WatchEvent {
                        key: b,
                        value: bv,
                        version: bver,
                    },
                ) => assert_eq!((a, av, aver), (b, bv, bver)),
                (
                    ClientResponse::PatternDeleted { deleted: a },
                    ClientResponse::PatternDeleted { deleted: b },
                ) => assert_eq!(a, b),
                (
                    ClientResponse::PatternTtlUpdated { updated: a },
                    ClientResponse::PatternTtlUpdated { updated: b },
                ) => assert_eq!(a, b),
                (
                    ClientResponse::SetNx {
                        created: ac,
                        version: av,
                    },
                    ClientResponse::SetNx {
                        created: bc,
                        version: bv,
                    },
                ) => assert_eq!((ac, av), (bc, bv)),
                (
                    ClientResponse::Counter {
                        value: av,
                        version: aver,
                    },
                    ClientResponse::Counter {
                        value: bv,
                        version: bver,
                    },
                ) => assert_eq!((av, aver), (bv, bver)),
                (a, b) => panic!("roundtrip changed response: {a:?} -> {b:?}"),
            }
        }
    }

    #[test]
    fn cluster_and_gossip_messages_round_trip() {
        let node_id = Uuid::from_u128(42);
        let peer_id = Uuid::from_u128(43);
        let addr: SocketAddr = "127.0.0.1:7779".parse().unwrap();
        let node = NodeInfo {
            id: node_id,
            addr,
            cluster_port: 7779,
            status: NodeStatus::Active,
            last_applied: 12,
        };

        let cluster = ClusterMessage::Forward {
            request: ClientRequest::Ping,
            origin_node: peer_id,
        };
        let decoded: ClusterMessage = decode(payload(&encode(&cluster).unwrap()), 2048).unwrap();
        match decoded {
            ClusterMessage::Forward {
                request: ClientRequest::Ping,
                origin_node,
            } => assert_eq!(origin_node, peer_id),
            other => panic!("unexpected cluster roundtrip: {other:?}"),
        }

        let log_entries = ClusterMessage::LogEntries {
            entries: vec![LogEntry {
                index: 9,
                key: "k".into(),
                value: Some(Bytes::from_static(b"value")),
                ttl_secs: Some(30),
                ts_ms: 123,
            }],
        };
        let decoded: ClusterMessage =
            decode(payload(&encode(&log_entries).unwrap()), 2048).unwrap();
        match decoded {
            ClusterMessage::LogEntries { entries } => {
                assert_eq!(entries[0].key, "k");
                assert_eq!(entries[0].value.as_deref(), Some(&b"value"[..]));
                assert_eq!(entries[0].ttl_secs, Some(30));
            }
            other => panic!("unexpected log entries roundtrip: {other:?}"),
        }

        let gossip = GossipMessage::ActiveSetUpdate {
            active_nodes: vec![node.clone()],
        };
        let decoded = decode_gossip(&encode_gossip(&gossip).unwrap(), 2048).unwrap();
        match decoded {
            GossipMessage::ActiveSetUpdate { active_nodes } => {
                assert_eq!(active_nodes[0].id, node_id);
                assert_eq!(active_nodes[0].addr, addr);
            }
            other => panic!("unexpected gossip roundtrip: {other:?}"),
        }

        let heartbeat = GossipMessage::Heartbeat {
            node_id,
            addr,
            cluster_port: 7779,
            status: NodeStatus::Syncing,
            last_applied: 99,
        };
        match decode_gossip(&encode_gossip(&heartbeat).unwrap(), 2048).unwrap() {
            GossipMessage::Heartbeat {
                status: NodeStatus::Syncing,
                last_applied: 99,
                ..
            } => {}
            other => panic!("unexpected heartbeat roundtrip: {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_bad_envelopes_and_limits() {
        let encoded = encode(&ClientRequest::Ping).unwrap();
        assert!(decode::<ClientRequest>(payload(&encoded), 1)
            .unwrap_err()
            .to_string()
            .contains("exceeds max"));

        let wrong_payload = pb::Envelope {
            version: 99,
            payload: Some(ClientRequest::Ping.into_payload()),
        }
        .encode_to_vec();
        assert!(decode::<ClientRequest>(&wrong_payload, 1024)
            .unwrap_err()
            .to_string()
            .contains("unsupported protocol version"));

        let missing_payload = pb::Envelope {
            version: PROTOCOL_VERSION,
            payload: None,
        }
        .encode_to_vec();
        assert!(decode::<ClientRequest>(&missing_payload, 1024)
            .unwrap_err()
            .to_string()
            .contains("missing protobuf envelope payload"));
    }
}

