use crate::{NodeInfo, NodeStatus};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipMessage {
    Heartbeat {
        node_id: Uuid,
        addr: SocketAddr,
        cluster_port: u16,
        status: NodeStatus,
        last_applied: u64,
    },
    ActiveSetUpdate { active_nodes: Vec<NodeInfo> },
}

impl From<GossipMessage> for crate::pb::GossipMessage {
    fn from(value: GossipMessage) -> Self {
        use crate::pb::gossip_message::Message;

        let message = match value {
            GossipMessage::Heartbeat {
                node_id,
                addr,
                cluster_port,
                status,
                last_applied,
            } => Message::Heartbeat(crate::pb::Heartbeat {
                node_id: crate::uuid_to_string(node_id),
                addr: crate::socket_to_string(addr),
                cluster_port: cluster_port as u32,
                status: crate::pb::NodeStatus::from(status) as i32,
                last_applied,
            }),
            GossipMessage::ActiveSetUpdate { active_nodes } => {
                Message::ActiveSetUpdate(crate::pb::ActiveSetUpdate {
                    active_nodes: active_nodes.into_iter().map(Into::into).collect(),
                })
            }
        };

        Self {
            message: Some(message),
        }
    }
}

impl TryFrom<crate::pb::GossipMessage> for GossipMessage {
    type Error = anyhow::Error;

    fn try_from(value: crate::pb::GossipMessage) -> anyhow::Result<Self> {
        use crate::pb::gossip_message::Message;

        let message = value
            .message
            .ok_or_else(|| anyhow::anyhow!("missing gossip message payload"))?;

        Ok(match message {
            Message::Heartbeat(v) => Self::Heartbeat {
                node_id: crate::uuid_from_string(v.node_id)?,
                addr: crate::socket_from_string(v.addr)?,
                cluster_port: u16::try_from(v.cluster_port)?,
                status: v.status.try_into()?,
                last_applied: v.last_applied,
            },
            Message::ActiveSetUpdate(v) => Self::ActiveSetUpdate {
                active_nodes: v
                    .active_nodes
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<anyhow::Result<Vec<_>>>()?,
            },
        })
    }
}
