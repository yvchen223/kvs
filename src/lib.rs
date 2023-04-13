#![deny(missing_docs)]
//! A simple key-value store
pub use err::Result;
pub use engines::{KvsEngine, KvStore};
pub use server::KvsServer;
pub use client::KvsClient;

pub mod err;
mod engines;
mod server;
mod client;
mod common;
