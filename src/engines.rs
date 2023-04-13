//! KvsEngine

pub use self::kvs::KvStore;
pub use self::sled::SledKvsEngine;

mod kvs;
mod sled;

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
