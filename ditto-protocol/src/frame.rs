use crate::{pb, GossipMessage};
use prost::Message;

pub(crate) const PROTOCOL_VERSION: u32 = 1;

pub trait WireMessage: Sized {
    fn into_payload(self) -> pb::envelope::Payload;
    fn from_payload(payload: pb::envelope::Payload) -> anyhow::Result<Self>;
}

fn encode_payload<T: WireMessage>(msg: T) -> Vec<u8> {
    let envelope = pb::Envelope {
        version: PROTOCOL_VERSION,
        payload: Some(msg.into_payload()),
    };
    envelope.encode_to_vec()
}

fn decode_payload<T: WireMessage>(buf: &[u8], max_size: u64) -> anyhow::Result<T> {
    if buf.len() as u64 > max_size {
        anyhow::bail!("message length {} exceeds max {}", buf.len(), max_size);
    }

    let envelope = pb::Envelope::decode(buf)?;
    if envelope.version != PROTOCOL_VERSION {
        anyhow::bail!(
            "unsupported protocol version {} (expected {})",
            envelope.version,
            PROTOCOL_VERSION
        );
    }
    let payload = envelope
        .payload
        .ok_or_else(|| anyhow::anyhow!("missing protobuf envelope payload"))?;
    T::from_payload(payload)
}

/// Encode any protocol message to length-prefixed bytes (4-byte BE length).
pub fn encode<T: WireMessage + Clone>(msg: &T) -> anyhow::Result<Vec<u8>> {
    let payload = encode_payload(msg.clone());
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Decode a length-prefixed message from a byte slice with limits.
pub fn decode<T: WireMessage>(buf: &[u8], max_size: u64) -> anyhow::Result<T> {
    decode_payload(buf, max_size)
}

/// Encode a gossip datagram payload without TCP length framing.
pub fn encode_gossip(msg: &GossipMessage) -> anyhow::Result<Vec<u8>> {
    Ok(encode_payload(msg.clone()))
}

/// Decode a gossip datagram payload without TCP length framing.
pub fn decode_gossip(buf: &[u8], max_size: u64) -> anyhow::Result<GossipMessage> {
    decode_payload(buf, max_size)
}
