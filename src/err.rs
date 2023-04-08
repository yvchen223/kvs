//! err

use std::io;
use thiserror::Error;

/// KvError
#[derive(Error, Debug)]
pub enum KvError {
    /// Open file error
    #[error("io error: {0:?}")]
    IoError(#[from] io::Error),

    /// json serialize error
    #[error("serialize json error: {0:?}")]
    JSONSerializeError(#[from] serde_json::Error),

    /// unknown
    #[error("unknown error")]
    Unknown,

    /// not found
    #[error("Key not found")]
    RecordNotFound,

    /// Find file error
    #[error("find file error {0}")]
    FindFileError(String),
}

/// Alias for a Result with the error type KvError.
pub type Result<T> = std::result::Result<T, KvError>;
