use crate::proxy::ProxyError;

#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    #[error("Claude Codex bridge is not available for this request")]
    OutOfScope,
    #[error(transparent)]
    Codec(#[from] ProxyError),
}

impl BridgeError {
    pub fn into_proxy_error(self) -> ProxyError {
        match self {
            Self::OutOfScope => ProxyError::InvalidRequest(
                "Claude Codex bridge is not available for this request".to_string(),
            ),
            Self::Codec(error) => error,
        }
    }
}
