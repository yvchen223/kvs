//! err

use std::io;
use thiserror::Error;

/// KvError
#[derive(Error, Debug)]
pub enum KvError {
    /// Open file error
    #[error("can't open file")]
    OpenFileError(io::Error),

    /// json serialize error
    #[error("serialize json error")]
    JSONSerializeError(#[from] serde_json::Error),

    /// unknown
    #[error("unknown error")]
    Unknown,

    /// write file
    #[error("write file")]
    WriteFileError(#[from] io::Error),

    /// not found
    #[error("Key not found")]
    RecordNotFound,

    /// Read file error
    #[error("read file error")]
    ReadFileError(String),
}

/// Alias for a Result with the error type KvError.
pub type Result<T> = std::result::Result<T, KvError>;
