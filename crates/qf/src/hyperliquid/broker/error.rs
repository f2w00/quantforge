use crate::hyperliquid::types::{HlClientOrderId, HlCoin};
use crate::risk::RiskViolation;

#[derive(Debug, thiserror::Error)]
pub enum HlBrokerError {
    #[error("risk rejected: {violations:?}")]
    RiskRejected { violations: Vec<RiskViolation> },

    #[error("invalid request: {message}")]
    InvalidRequest { message: String },

    #[error("exchange rejected: {message}")]
    ExchangeRejected {
        message: String,
        raw: serde_json::Value,
    },

    #[error("transport failed: {message}")]
    Transport { message: String },

    #[error("request outcome is unknown for client order id {client_order_id:?}")]
    OutcomeUnknown { client_order_id: HlClientOrderId },

    #[error("local broker state is unavailable or stale")]
    StateUnavailable,

    #[error("position is unavailable for {coin:?}")]
    PositionUnavailable { coin: HlCoin },
}
