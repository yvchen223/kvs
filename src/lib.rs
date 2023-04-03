#![deny(missing_docs)]
//! A simple key-value store

use std::collections::HashMap;

/// `KvStore` stores key-value pairs in memory.
///
/// The pairs are stored in an internal HashMap.
///
/// Example
///
/// ```rust
/// use kvs::KvStore;
///
/// let mut kv = KvStore::new();
/// kv.set("key".to_owned(), "value".to_owned());
/// let val = kv.get("key".to_owned());
/// assert_eq!(val, Some("value".to_owned()));
///
/// ```
#[derive(Default)]
pub struct KvStore {
    store: HashMap<String, String>,
}

impl KvStore {
    /// Constructs a new instance of `KvStore`
    pub fn new() -> Self {
        KvStore {
            store: HashMap::new(),
        }
    }

    /// Sets a pair of key-value.
    ///
    /// The value will be overwritten if the key has existed.
    pub fn set(&mut self, key: String, value: String) {
        self.store.insert(key, value);
    }

    /// Gets the string value of the given string key.
    ///
    /// Returns `None` if the key does not exist.
    pub fn get(&self, key: String) -> Option<String> {
        self.store.get(&key).map(|v| v.to_owned())
    }

    /// Removes a given key.
    ///
    /// Does nothing if the key does not exist.
    pub fn remove(&mut self, key: String) {
        self.store.remove(&key);
    }
}
