use super::NodeHandle;
use ditto_protocol::{ClientResponse, ErrorCode};

impl NodeHandle {
    pub(super) fn namespace_context(
        &self,
        namespace: Option<String>,
    ) -> Result<Option<String>, ClientResponse> {
        let cfg = self.config.lock().unwrap();
        if !cfg.tenancy.enabled {
            return Ok(None);
        }
        let namespace = namespace
            .unwrap_or_else(|| cfg.tenancy.default_namespace.clone())
            .trim()
            .to_string();
        if namespace.is_empty() || namespace.contains("::") {
            return Err(ClientResponse::Error {
                code: ErrorCode::InternalError,
                message: "Invalid namespace".into(),
            });
        }
        Ok(Some(namespace))
    }

    pub(super) fn namespaced_key(&self, namespace: &Option<String>, key: &str) -> String {
        match namespace {
            Some(namespace) => format!("{}::{}", namespace, key),
            None => key.to_string(),
        }
    }

    pub(super) fn namespaced_pattern(&self, namespace: &Option<String>, pattern: &str) -> String {
        match namespace {
            Some(namespace) => format!("{}::{}", namespace, pattern),
            None => pattern.to_string(),
        }
    }

    pub(super) fn namespace_key_count(&self, namespace: &str) -> usize {
        let pattern = format!("{}::*", namespace);
        self.store
            .keys(Some(&pattern))
            .into_iter()
            .filter(|key| self.store.get(key).is_some())
            .count()
    }
}
