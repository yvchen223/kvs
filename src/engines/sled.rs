use crate::err::Error;
use crate::KvsEngine;
use crate::Result;
use sled::Tree;
use std::path::PathBuf;

/// SledKvsEngine contains sled db
pub struct SledKvsEngine {
    sled: sled::Db,
}

impl SledKvsEngine {
    /// New SledKvsEngine
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let sled = match sled::open(path) {
            Ok(db) => db,
            Err(e) => return Err(Error::ServerError(e.to_string())),
        };
        Ok(SledKvsEngine { sled })
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let tree: &Tree = &self.sled;
        tree.insert(key, value.as_bytes())?;
        tree.flush()?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.sled.get(key.as_str()) {
            Ok(val) => match val {
                Some(v) => {
                    let str = match std::str::from_utf8(&v) {
                        Ok(s) => s,
                        Err(e) => return Err(Error::ServerError(e.to_string())),
                    };
                    Ok(Some(str.to_string()))
                }
                None => Ok(None),
            },
            Err(e) => Err(Error::ServerError(e.to_string())),
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        self.sled.remove(key)?.ok_or(Error::RecordNotFound)?;
        self.sled.flush()?;
        Ok(())
    }
}
