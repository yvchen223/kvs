#![deny(missing_docs)]
//! A simple key-value store
pub use client::KvsClient;
pub use engines::{KvStore, KvsEngine, SledKvsEngine};
pub use err::Result;
pub use server::KvsServer;

mod client;
mod common;
mod engines;
pub mod err;
mod server;
pub mod thread_pool;
