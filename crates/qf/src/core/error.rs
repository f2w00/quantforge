use thiserror::Error;

pub type QfResult<T> = Result<T, QfError>;

#[derive(Debug, Error)]
pub enum QfError {
    #[error("invalid input: {0}")]
    InvalidInput(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("operation rejected: {0}")]
    Rejected(String),
}
