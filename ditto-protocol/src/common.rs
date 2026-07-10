use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: Uuid,
    pub addr: SocketAddr,
    pub cluster_port: u16,
    pub status: NodeStatus,
    pub last_applied: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Participating in writes and serving reads.
    Active,
    /// Online but catching up on missed log entries; no reads/writes.
    Syncing,
    /// Unreachable (gossip timeout).
    Offline,
    /// Manually deactivated via admin CLI; rejects client requests.
    Inactive,
}

impl From<NodeStatus> for crate::pb::NodeStatus {
    fn from(value: NodeStatus) -> Self {
        match value {
            NodeStatus::Active => Self::Active,
            NodeStatus::Syncing => Self::Syncing,
            NodeStatus::Offline => Self::Offline,
            NodeStatus::Inactive => Self::Inactive,
        }
    }
}

impl TryFrom<i32> for NodeStatus {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> anyhow::Result<Self> {
        match crate::pb::NodeStatus::try_from(value)? {
            crate::pb::NodeStatus::Active => Ok(Self::Active),
            crate::pb::NodeStatus::Syncing => Ok(Self::Syncing),
            crate::pb::NodeStatus::Offline => Ok(Self::Offline),
            crate::pb::NodeStatus::Inactive => Ok(Self::Inactive),
        }
    }
}

impl From<NodeInfo> for crate::pb::NodeInfo {
    fn from(value: NodeInfo) -> Self {
        Self {
            id: crate::uuid_to_string(value.id),
            addr: crate::socket_to_string(value.addr),
            cluster_port: value.cluster_port as u32,
            status: crate::pb::NodeStatus::from(value.status) as i32,
            last_applied: value.last_applied,
        }
    }
}

impl TryFrom<crate::pb::NodeInfo> for NodeInfo {
    type Error = anyhow::Error;

    fn try_from(value: crate::pb::NodeInfo) -> anyhow::Result<Self> {
        Ok(Self {
            id: crate::uuid_from_string(value.id)?,
            addr: crate::socket_from_string(value.addr)?,
            cluster_port: u16::try_from(value.cluster_port)?,
            status: value.status.try_into()?,
            last_applied: value.last_applied,
        })
    }
}
