//! KvsEngine

pub use self::kvs::KvStore;
pub use self::sled::SledKvsEngine;

mod kvs;
mod sled;

use crate::err::Result;

/// KvsEngine
pub trait KvsEngine: Clone + Send + 'static {
    /// set
    fn set(&self, key: String, value: String) -> Result<()>;

    /// get
    fn get(&self, key: String) -> Result<Option<String>>;

    /// remove
    fn remove(&self, key: String) -> Result<()>;
}
