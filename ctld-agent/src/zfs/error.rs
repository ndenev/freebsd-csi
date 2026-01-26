use thiserror::Error;

#[derive(Error, Debug)]
pub enum ZfsError {
    #[error("dataset '{0}' not found")]
    DatasetNotFound(String),

    #[error("dataset '{0}' already exists")]
    DatasetExists(String),

    #[error("dataset '{0}' is busy")]
    DatasetBusy(String),

    #[error("invalid dataset name: {0}")]
    InvalidName(String),

    #[error("zfs command failed: {0}")]
    CommandFailed(String),

    #[error("failed to parse zfs output: {0}")]
    ParseError(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, ZfsError>;
