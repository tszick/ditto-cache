use crate::config::{ClientAuthTokenConfig, Config, TenancyConfig};
use ditto_protocol::{ClientRequest, ClientResponse, ErrorCode};

#[derive(Debug, Clone)]
pub struct ClientAccessController {
    default_principal: Option<AuthenticatedClient>,
    scoped_tokens: Vec<ScopedToken>,
    pub(crate) tenancy: TenancyConfig,
}

#[derive(Debug, Clone)]
pub struct AuthenticatedClient {
    principal_name: String,
    policy: AccessPolicy,
}

#[derive(Debug, Clone)]
struct ScopedToken {
    token: String,
    principal: AuthenticatedClient,
}

#[derive(Debug, Clone)]
struct AccessPolicy {
    allow_all: bool,
    namespaces: Vec<String>,
    read_prefixes: Vec<String>,
    write_prefixes: Vec<String>,
    watch_prefixes: Vec<String>,
    pattern_prefixes: Vec<String>,
}

impl ClientAccessController {
    pub fn from_config(config: &Config) -> Self {
        let legacy_auth_enabled = config
            .node
            .client_auth_token
            .as_deref()
            .is_some_and(|token| !token.is_empty());
        let default_principal = if legacy_auth_enabled || !config.client_auth.tokens.is_empty() {
            None
        } else {
            Some(AuthenticatedClient {
                principal_name: "anonymous".to_string(),
                policy: AccessPolicy::allow_all(),
            })
        };

        Self {
            default_principal,
            scoped_tokens: config
                .client_auth
                .tokens
                .iter()
                .filter_map(ScopedToken::from_config)
                .collect(),
            tenancy: config.tenancy.clone(),
        }
    }

    pub fn default_principal(&self) -> Option<AuthenticatedClient> {
        self.default_principal.clone()
    }

    pub fn authenticate(&self, token: &str, legacy_token: Option<&str>) -> Option<AuthenticatedClient> {
        if legacy_token.is_some_and(|expected| expected == token) {
            return Some(AuthenticatedClient {
                principal_name: "legacy-client-auth-token".to_string(),
                policy: AccessPolicy::allow_all(),
            });
        }

        self.scoped_tokens
            .iter()
            .find(|candidate| candidate.token == token)
            .map(|candidate| candidate.principal.clone())
    }
}

impl AuthenticatedClient {
    pub fn authorize(&self, request: &ClientRequest, tenancy: &TenancyConfig) -> Result<(), ClientResponse> {
        let decision = self.policy.authorize(request, tenancy);
        if decision.allowed {
            return Ok(());
        }

        let scope = decision.scope.unwrap_or("request");
        Err(ClientResponse::Error {
            code: ErrorCode::AccessDenied,
            message: format!(
                "Client principal '{}' is not allowed to access {} '{}'",
                self.principal_name, scope, decision.target
            ),
        })
    }
}

impl ScopedToken {
    fn from_config(config: &ClientAuthTokenConfig) -> Option<Self> {
        let token = config.token.as_ref()?.trim();
        if token.is_empty() {
            return None;
        }
        Some(Self {
            token: token.to_string(),
            principal: AuthenticatedClient {
                principal_name: config.name.clone(),
                policy: AccessPolicy {
                    allow_all: config.allow_all,
                    namespaces: config.namespaces.clone(),
                    read_prefixes: config.read_prefixes.clone(),
                    write_prefixes: config.write_prefixes.clone(),
                    watch_prefixes: config.watch_prefixes.clone(),
                    pattern_prefixes: config.pattern_prefixes.clone(),
                },
            },
        })
    }
}

impl AccessPolicy {
    fn allow_all() -> Self {
        Self {
            allow_all: true,
            namespaces: Vec::new(),
            read_prefixes: Vec::new(),
            write_prefixes: Vec::new(),
            watch_prefixes: Vec::new(),
            pattern_prefixes: Vec::new(),
        }
    }

    fn authorize(&self, request: &ClientRequest, tenancy: &TenancyConfig) -> AccessDecision {
        if self.allow_all || matches!(request, ClientRequest::Ping) {
            return AccessDecision::allowed();
        }

        match request {
            ClientRequest::Ping | ClientRequest::Auth { .. } => AccessDecision::allowed(),
            ClientRequest::Get { key, namespace } => self.check_key("key", key, namespace.as_deref(), tenancy, &self.read_prefixes),
            ClientRequest::Set { key, namespace, .. }
            | ClientRequest::SetNx { key, namespace, .. }
            | ClientRequest::Incr { key, namespace, .. }
            | ClientRequest::Delete { key, namespace } => {
                self.check_key("key", key, namespace.as_deref(), tenancy, &self.write_prefixes)
            }
            ClientRequest::Watch { key, namespace }
            | ClientRequest::Unwatch { key, namespace } => {
                self.check_key("watch key", key, namespace.as_deref(), tenancy, &self.watch_prefixes)
            }
            ClientRequest::DeleteByPattern { pattern, namespace }
            | ClientRequest::SetTtlByPattern {
                pattern,
                namespace,
                ..
            } => self.check_key(
                "pattern",
                pattern,
                namespace.as_deref(),
                tenancy,
                &self.pattern_prefixes,
            ),
        }
    }

    fn check_key(
        &self,
        scope: &'static str,
        target: &str,
        namespace: Option<&str>,
        tenancy: &TenancyConfig,
        prefixes: &[String],
    ) -> AccessDecision {
        if let Some(namespace) = resolved_namespace(tenancy, namespace) {
            if !self.namespaces.is_empty() && !self.namespaces.iter().any(|allowed| allowed == namespace) {
                return AccessDecision::denied("namespace", namespace);
            }
        }

        if prefixes.iter().any(|prefix| target.starts_with(prefix)) {
            AccessDecision::allowed()
        } else {
            AccessDecision::denied(scope, target)
        }
    }
}

fn resolved_namespace<'a>(tenancy: &'a TenancyConfig, namespace: Option<&'a str>) -> Option<&'a str> {
    if !tenancy.enabled {
        return None;
    }
    namespace
        .map(str::trim)
        .filter(|namespace| !namespace.is_empty())
        .or(Some(tenancy.default_namespace.as_str()))
}

struct AccessDecision {
    allowed: bool,
    scope: Option<&'static str>,
    target: String,
}

impl AccessDecision {
    fn allowed() -> Self {
        Self {
            allowed: true,
            scope: None,
            target: String::new(),
        }
    }

    fn denied(scope: &'static str, target: &str) -> Self {
        Self {
            allowed: false,
            scope: Some(scope),
            target: target.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClientAuthConfig;

    fn config_with_scoped_token() -> Config {
        let mut config = Config::default();
        config.tenancy.enabled = true;
        config.tenancy.default_namespace = "default".into();
        config.client_auth = ClientAuthConfig {
            tokens: vec![ClientAuthTokenConfig {
                name: "reader".into(),
                token: Some("reader-secret".into()),
                namespaces: vec!["tenant-a".into()],
                read_prefixes: vec!["cache:".into()],
                watch_prefixes: vec!["cache:".into()],
                ..Default::default()
            }],
        };
        config
    }

    #[test]
    fn legacy_token_remains_allow_all() {
        let mut config = Config::default();
        config.node.client_auth_token = Some("legacy".into());
        let controller = ClientAccessController::from_config(&config);
        let principal = controller.authenticate("legacy", config.node.client_auth_token.as_deref());
        assert!(principal.is_some());
        principal
            .unwrap()
            .authorize(
                &ClientRequest::Delete {
                    key: "any".into(),
                    namespace: None,
                },
                &config.tenancy,
            )
            .unwrap();
    }

    #[test]
    fn scoped_token_enforces_namespace_and_prefixes() {
        let config = config_with_scoped_token();
        let controller = ClientAccessController::from_config(&config);
        let principal = controller
            .authenticate("reader-secret", config.node.client_auth_token.as_deref())
            .unwrap();

        principal
            .authorize(
                &ClientRequest::Get {
                    key: "cache:item".into(),
                    namespace: Some("tenant-a".into()),
                },
                &config.tenancy,
            )
            .unwrap();

        let denied = principal
            .authorize(
                &ClientRequest::Get {
                    key: "other:item".into(),
                    namespace: Some("tenant-a".into()),
                },
                &config.tenancy,
            )
            .unwrap_err();
        assert!(matches!(
            denied,
            ClientResponse::Error {
                code: ErrorCode::AccessDenied,
                ..
            }
        ));

        let denied = principal
            .authorize(
                &ClientRequest::Get {
                    key: "cache:item".into(),
                    namespace: Some("tenant-b".into()),
                },
                &config.tenancy,
            )
            .unwrap_err();
        assert!(matches!(
            denied,
            ClientResponse::Error {
                code: ErrorCode::AccessDenied,
                ..
            }
        ));
    }
}
