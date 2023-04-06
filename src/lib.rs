#![deny(missing_docs)]
//! A simple key-value store
pub use err::Result;
pub use kv::KvStore;

mod kv;

pub mod err;
