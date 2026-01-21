use thiserror::Error;

#[derive(Error, Debug)]
pub enum CtlError {
    #[error("target '{0}' not found")]
    TargetNotFound(String),

    #[error("target '{0}' already exists")]
    TargetExists(String),

    #[error("ctld command failed: {0}")]
    CommandFailed(String),

    #[allow(dead_code)]
    #[error("failed to parse ctld output: {0}")]
    ParseError(String),

    #[error("configuration error: {0}")]
    ConfigError(String),

    #[error("invalid name: {0}")]
    InvalidName(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, CtlError>;
