use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use crate::err::KvError;
use crate::err::Result;
use serde::{Serialize,Deserialize};

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
/// kv.set("key".to_owned(), "value".to_owned())?;
/// let val = kv.get("key".to_owned());
/// assert_eq!(val, Ok(Some("value".to_owned())));
///
/// ```
//#[derive(Clone)]
pub struct KvStore {
    store: HashMap<String, String>,
    writer: BufWriter<File>
}

#[derive(Debug, Serialize, Deserialize)]
enum CommandType {
    Set = 0,
    Remove = 1,
}

#[derive(Serialize, Deserialize, Debug)]
struct Command {
    command_type: CommandType,
    key: String,
    value: String,
}

impl KvStore {

    /// Open file to store log
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();

        fs::create_dir_all(&path)?;
        let log_file = gen_log_file(&path);
        let f = match fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(log_file.as_path()) {
            Ok(f) => f,
            Err(e) => return Err(KvError::OpenFileError(e)),
        };

        let writer = BufWriter::new(f.try_clone()?);
        let reader = BufReader::new(f.try_clone()?);

        let deserializer = serde_json::Deserializer::from_reader(reader);
        let iter = deserializer.into_iter::<Command>();

        let mut store = HashMap::new();
        for item in iter {
            if let Ok(command) = item {
                match command.command_type {
                    CommandType::Set => store.insert(command.key, command.value),
                    CommandType::Remove => store.remove(&command.key),
                };
            };
        }

        Ok(KvStore {
            store,
            writer,
        })
    }


    /// Sets a pair of key-value.
    ///
    /// The value will be overwritten if the key has existed.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command {
            command_type: CommandType::Set,
            key: key.clone(),
            value: value.clone(),
        };
        let command_serialize = serde_json::to_vec(&command)?;
        self.writer.write(&command_serialize)?;
        self.writer.flush()?;

        self.store.insert(key, value);
        Ok(())
    }

    /// Gets the string value of the given string key.
    ///
    /// Returns `None` if the key does not exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {

        return match self.store.get(&key) {
            Some(v) => {
                Ok(Some(v.to_owned()))
            },
            None => {
                Ok(None)
            },
        };
    }

    /// Removes a given key.
    ///
    /// Does nothing if the key does not exist.
    pub fn remove(&mut self, key: String) -> Result<()> {

        match self.store.remove(&key) {
            Some(_) => {
                let command = Command {
                    command_type: CommandType::Remove,
                    key: key.clone(),
                    value: String::new(),
                };
                let command_serialize = serde_json::to_vec(&command)?;
                self.writer.write(&command_serialize)?;
                self.writer.flush()?;
            },
            None => return Err(KvError::RecordNotFound),
        }
        Ok(())
    }


}

fn gen_log_file(path: &Path) -> PathBuf {
    path.join("1.log")
}


