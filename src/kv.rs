use crate::err::KvError;
use crate::err::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::{fs, io};

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
pub struct KvStore {
    writer: BufWriterWithPos,
    reader: BufReaderWithPos,
    index: HashMap<String, CommandPos>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
enum CommandType {
    Set = 0,
    Remove = 1,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Command {
    command_type: CommandType,
    key: String,
    value: String,
}

struct CommandPos {
    pos: u64,
    len: usize,
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
            .open(log_file.as_path())
        {
            Ok(f) => f,
            Err(e) => return Err(KvError::OpenFileError(e)),
        };

        let reader = BufReader::new(f.try_clone()?);
        let mut reader = BufReaderWithPos::new(reader)?;
        reader.seek(SeekFrom::Start(0))?;
        let deserializer = serde_json::Deserializer::from_reader(&mut reader);
        let mut iter = deserializer.into_iter::<Command>();

        let mut index = HashMap::new();
        let mut offset: usize = 0;
        while let Some(item) = iter.next() {
            if let Ok(command) = item {
                let offset_end = iter.byte_offset();
                match command.command_type {
                    CommandType::Set => {
                        let command_pos = CommandPos {
                            pos: offset as u64,
                            len: offset_end - offset,
                        };
                        index.insert(command.key, command_pos);
                    }
                    CommandType::Remove => {
                        index.remove(&command.key);
                    }
                };
                offset = offset_end;
            };
        }

        let writer = BufWriter::new(f.try_clone()?);
        let writer = BufWriterWithPos::new(writer)?;

        Ok(KvStore {
            writer,
            reader,
            index,
        })
    }

    /// Sets a pair of key-value.
    ///
    /// The value will be overwritten if the key has existed.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command {
            command_type: CommandType::Set,
            key: key.clone(),
            value,
        };
        let command_serialize = serde_json::to_vec(&command)?;

        let start = self.writer.pos;
        let len = self.writer.write(&command_serialize)?;
        let command_pos = CommandPos { pos: start, len };

        self.index.insert(key, command_pos);
        Ok(())
    }

    /// Gets the string value of the given string key.
    ///
    /// Returns `None` if the key does not exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        return match self.index.get(&key) {
            Some(command_pos) => {
                self.reader.seek(SeekFrom::Start(command_pos.pos))?;
                let mut buf = vec![0; command_pos.len];
                self.reader.read_exact(&mut buf)?;
                let command: Command = serde_json::from_slice(&buf)?;
                Ok(Some(command.value))
            }
            None => Ok(None),
        };
    }

    /// Removes a given key.
    ///
    /// Does nothing if the key does not exist.
    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.index.remove(&key) {
            Some(_) => {
                let command = Command {
                    command_type: CommandType::Remove,
                    key: key.clone(),
                    value: String::new(),
                };
                let command_serialize = serde_json::to_vec(&command)?;
                self.writer.write_all(&command_serialize)?;
            }
            None => return Err(KvError::RecordNotFound),
        }
        Ok(())
    }
}

fn gen_log_file(path: &Path) -> PathBuf {
    path.join("1.log")
}

struct BufReaderWithPos {
    reader: BufReader<File>,
    pos: u64,
}
impl Read for BufReaderWithPos {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}
impl Seek for BufReaderWithPos {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

impl BufReaderWithPos {
    fn new(mut reader: BufReader<File>) -> Result<Self> {
        let pos = reader.stream_position()?;
        Ok(BufReaderWithPos { reader, pos })
    }
}

struct BufWriterWithPos {
    writer: BufWriter<File>,
    pos: u64,
}

impl Write for BufWriterWithPos {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.flush()?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

impl BufWriterWithPos {
    fn new(mut writer: BufWriter<File>) -> Result<Self> {
        let pos = writer.stream_position()?;
        Ok(BufWriterWithPos { writer, pos })
    }
}
