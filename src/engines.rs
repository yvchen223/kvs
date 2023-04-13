//! KvsEngine

pub use kvs::KvStore;

mod kvs;

use crate::err::Result;

/// KvsEngine
pub trait KvsEngine {
    /// set
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// get
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// remove
    fn remove(&mut self, key: String) -> Result<()>;
}