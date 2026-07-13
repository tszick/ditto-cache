use bytes::Bytes;

pub(crate) fn entry_size(key: &str, value: &Bytes) -> u64 {
    (key.len() + value.len() + 64) as u64
}

pub(crate) fn replace_entry_size(used_bytes: u64, key: &str, old_value: &Bytes, new_value: &Bytes) -> u64 {
    used_bytes
        .saturating_sub(entry_size(key, old_value))
        .saturating_add(entry_size(key, new_value))
}

pub(crate) fn remove_entry_size(used_bytes: u64, key: &str, value: &Bytes) -> u64 {
    used_bytes.saturating_sub(entry_size(key, value))
}
