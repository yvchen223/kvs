use crate::err::KvError;
use crate::err::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::OsStr;
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
/// use std::env;
/// use kvs::KvStore;
///
/// let mut kv = KvStore::open(env::current_dir().expect("current dir error")).expect("open error");
/// kv.set("key".to_owned(), "value".to_owned()).expect("set error");
/// let val = kv.get("key".to_owned()).expect("get error");
/// assert_eq!(val, Some("value".to_owned()));
///
/// ```
pub struct KvStore {
    writer: BufWriterWithPos,
    index: HashMap<String, CommandPos>,
    file_id: u64,
    readers: HashMap<u64, BufReaderWithPos>,
    uncompacted: u64,
    path: PathBuf,
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
    file_id: u64,
    pos: u64,
    len: usize,
}

impl KvStore {
    /// Open file to store log
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();

        fs::create_dir_all(&path)?;
        let ids = gen_log_file_id(&path)?;
        let cur_file_id = ids.last().unwrap_or(&0) + 1;

        let mut readers = HashMap::new();
        let mut index = HashMap::new();
        let mut uncompacted = 0;
        for id in ids {
            let file = path.join(format!("{}.log", id));
            let f = fs::File::open(file)?;
            let reader = BufReader::new(f);
            let mut reader = BufReaderWithPos::new(reader)?;

            uncompacted += load_data_from_file(id, &mut reader, &mut index)?;

            readers.insert(id, reader);
        }

        let writer = new_log_file(cur_file_id, &path, &mut readers)?;

        Ok(KvStore {
            writer,
            index,
            file_id: cur_file_id,
            readers,
            uncompacted,
            path,
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
        let command_pos = CommandPos {
            file_id: self.file_id,
            pos: start,
            len,
        };

        if let Some(command_pos) = self.index.insert(key, command_pos) {
            self.uncompacted += command_pos.len as u64;
        }
        if self.uncompacted > 1024 {
            self.compact()?;
        }
        Ok(())
    }

    /// Gets the string value of the given string key.
    ///
    /// Returns `None` if the key does not exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        return match self.index.get(&key) {
            Some(command_pos) => {
                let reader = match self.readers.get_mut(&command_pos.file_id) {
                    Some(r) => r,
                    None => return Err(KvError::FindFileError(command_pos.file_id.to_string())),
                };
                reader.seek(SeekFrom::Start(command_pos.pos))?;
                let mut buf = vec![0; command_pos.len];
                reader.read_exact(&mut buf)?;
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
            Some(command_pos) => {
                self.uncompacted += command_pos.len as u64;
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

    fn compact(&mut self) -> Result<()> {
        let compact_file_id = self.file_id + 1;
        self.file_id = compact_file_id + 1;
        self.writer = self.new_log_file(self.file_id)?;
        let mut writer = self.new_log_file(compact_file_id)?;
        let mut new_pos = 0;
        for command_pos in self.index.values_mut() {
            let reader = match self.readers.get_mut(&command_pos.file_id) {
                Some(r) => r,
                None => return Err(KvError::FindFileError(command_pos.file_id.to_string())),
            };
            reader.seek(SeekFrom::Start(command_pos.pos))?;
            let mut buf = reader.take(command_pos.len as u64);
            let len = io::copy(&mut buf, &mut writer)?;
            *command_pos = CommandPos {
                file_id: compact_file_id,
                pos: new_pos,
                len: len as usize,
            };
            new_pos += len;
        }
        writer.flush()?;

        let mut rm_ids = vec![];
        for id in self.readers.keys() {
            if *id < compact_file_id {
                rm_ids.push(*id);
            }
        }
        for id in rm_ids {
            self.readers.remove(&id);
            fs::remove_file(self.path.join(format!("{}.log", id)))?;
        }

        self.uncompacted = 0;
        Ok(())
    }

    fn new_log_file(&mut self, file_id: u64) -> Result<BufWriterWithPos> {
        new_log_file(file_id, &self.path, &mut self.readers)
    }
}

fn new_log_file(
    file_id: u64,
    path: &Path,
    readers: &mut HashMap<u64, BufReaderWithPos>,
) -> Result<BufWriterWithPos> {
    let log_file = path.join(format!("{}.log", file_id));
    let f = match fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .append(true)
        .open(log_file.as_path())
    {
        Ok(f) => f,
        Err(e) => return Err(KvError::IoError(e)),
    };

    let reader = BufReader::new(f.try_clone()?);
    let mut reader = BufReaderWithPos::new(reader)?;
    reader.seek(SeekFrom::Start(0))?;
    readers.insert(file_id, reader);

    let writer = BufWriter::new(f.try_clone()?);
    let writer = BufWriterWithPos::new(writer)?;
    Ok(writer)
}

fn gen_log_file_id(path: &Path) -> Result<Vec<u64>> {
    let mut ids: Vec<u64> = fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap())
        .map(|p| p.path())
        .filter(|p| p.extension() == Some("log".as_ref()))
        .map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .unwrap()
                .trim_end_matches(".log")
                .parse::<u64>()
                .unwrap()
        })
        .collect();
    ids.sort();

    Ok(ids)
}

fn load_data_from_file(
    file_id: u64,
    mut reader: &mut BufReaderWithPos,
    index: &mut HashMap<String, CommandPos>,
) -> Result<u64> {
    reader.seek(SeekFrom::Start(0))?;
    let deserializer = serde_json::Deserializer::from_reader(&mut reader);
    let mut iter = deserializer.into_iter::<Command>();

    let mut offset: usize = 0;
    let mut uncompacted: u64 = 0;
    while let Some(item) = iter.next() {
        if let Ok(command) = item {
            let offset_end = iter.byte_offset();
            match command.command_type {
                CommandType::Set => {
                    let command_pos = CommandPos {
                        file_id,
                        pos: offset as u64,
                        len: offset_end - offset,
                    };
                    if index.insert(command.key, command_pos).is_some() {
                        uncompacted += (offset_end - offset) as u64;
                    }
                }
                CommandType::Remove => {
                    if let Some(command_pos) = index.remove(&command.key) {
                        uncompacted += command_pos.len as u64;
                    }
                }
            };
            offset = offset_end;
        };
    }
    Ok(uncompacted)
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
