//! err

use std::io;
use thiserror::Error;

/// Error
#[derive(Error, Debug)]
pub enum Error {
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

    /// Client get value error
    #[error("get value error: {0}")]
    ClientGetError(String),

    /// Client set key-value error
    #[error("set key-value error: {0}")]
    ClientSetError(String),

    /// Client remove value error
    #[error("remove value error: {0}")]
    ClientRemoveError(String),
}

/// Alias for a Result with the error type Error.
pub type Result<T> = std::result::Result<T, Error>;
