use crate::store::compression::sanitize_for_log;
use ditto_protocol::AdminRequest;

pub fn labels(req: &AdminRequest) -> Option<(&'static str, &'static str)> {
    match req {
        AdminRequest::SetProperty { .. } => Some(("set_property", "node")),
        AdminRequest::GetKeyInfo { .. } => Some(("reveal_key_info", "cache_key")),
        AdminRequest::SetKeyProperty { .. } => Some(("set_key_property", "cache_key")),
        AdminRequest::FlushCache => Some(("flush", "cache")),
        AdminRequest::BackupNow => Some(("backup", "node")),
        AdminRequest::RestoreLatestSnapshot => Some(("restore_snapshot", "node")),
        AdminRequest::SetKeysTtl { .. } => Some(("set_ttl", "cache_keys")),
        _ => None,
    }
}

pub fn target(req: &AdminRequest) -> Option<String> {
    match req {
        AdminRequest::SetProperty { name, .. } => Some(sanitize_for_log(name)),
        AdminRequest::GetKeyInfo { key } => Some(sanitize_for_log(key)),
        AdminRequest::SetKeyProperty { key, name, .. } => Some(format!(
            "{}:{}",
            sanitize_for_log(key),
            sanitize_for_log(name)
        )),
        AdminRequest::SetKeysTtl { pattern, .. } => Some(sanitize_for_log(pattern)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_cover_sensitive_admin_operations() {
        assert_eq!(labels(&AdminRequest::FlushCache), Some(("flush", "cache")));
        assert_eq!(
            labels(&AdminRequest::GetKeyInfo {
                key: "secret".into()
            }),
            Some(("reveal_key_info", "cache_key"))
        );
        assert_eq!(
            target(&AdminRequest::SetKeyProperty {
                key: "tenant\nkey".into(),
                name: "compressed".into(),
                value: "true".into()
            })
            .as_deref(),
            Some("tenantkey:compressed")
        );
        assert_eq!(labels(&AdminRequest::GetStats), None);
    }
}
