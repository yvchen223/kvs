#![deny(missing_docs)]
//! A simple key-value store
pub use kv::KvStore;
pub use err::Result;

mod kv;

pub mod err;

