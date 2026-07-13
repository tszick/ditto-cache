use bytes::Bytes;
use lz4_flex::{compress_prepend_size, decompress_size_prepended};

pub(crate) fn sanitize_for_log(s: &str) -> String {
    s.replace(['\n', '\r'], "")
}

pub(crate) const MAX_DECOMPRESSED_FALLBACK_BYTES: u64 = 512 * 1024 * 1024;
pub(crate) const MAX_COMPRESS_INPUT_BYTES: usize = 512 * 1024 * 1024;

pub(crate) enum ReadDecompression {
    SkippedDeclaredTooLarge { declared_size: u64, limit: u64 },
    Decompressed(Bytes),
    Failed(String),
}

fn decompression_limit(max_value_bytes: u64) -> u64 {
    if max_value_bytes > 0 {
        max_value_bytes
    } else {
        MAX_DECOMPRESSED_FALLBACK_BYTES
    }
}

fn declared_decompressed_size(value: &Bytes) -> Option<u64> {
    value
        .get(..4)
        .map(|header| u32::from_le_bytes(header.try_into().unwrap()) as u64)
}

pub(crate) fn export_portable_value(value: &Bytes, compressed: bool) -> Vec<u8> {
    if !compressed {
        return value.to_vec();
    }

    let oversized = declared_decompressed_size(value)
        .is_some_and(|declared| declared > MAX_DECOMPRESSED_FALLBACK_BYTES);
    if oversized {
        return value.to_vec();
    }

    decompress_size_prepended(value).unwrap_or_else(|_| value.to_vec())
}

pub(crate) fn read_decompressed_value(value: &Bytes, max_value_bytes: u64) -> ReadDecompression {
    let limit = decompression_limit(max_value_bytes);
    if let Some(declared_size) = declared_decompressed_size(value) {
        if declared_size > limit {
            return ReadDecompression::SkippedDeclaredTooLarge {
                declared_size,
                limit,
            };
        }
    }

    match decompress_size_prepended(value) {
        Ok(decompressed) => ReadDecompression::Decompressed(Bytes::from(decompressed)),
        Err(error) => ReadDecompression::Failed(error.to_string()),
    }
}

pub(crate) fn validate_manual_decompression(
    value: &Bytes,
    max_value_bytes: u64,
) -> Result<(), &'static str> {
    let limit = decompression_limit(max_value_bytes);
    if let Some(declared_size) = declared_decompressed_size(value) {
        if declared_size > limit {
            return Err("decompressed size exceeds limit");
        }
    }
    Ok(())
}

pub(crate) fn compress_value(value: &Bytes) -> Bytes {
    Bytes::from(compress_prepend_size(value))
}

pub(crate) fn decompress_value(value: &Bytes) -> Result<Bytes, &'static str> {
    decompress_size_prepended(value)
        .map(Bytes::from)
        .map_err(|_| "decompression failed")
}
