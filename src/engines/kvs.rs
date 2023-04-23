use crate::err::Error;
use crate::err::Result;
use crate::KvsEngine;
use crossbeam_skiplist::SkipMap;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{hash_map, HashMap};
use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::{fs, io};
use std::sync::atomic::{AtomicU64, Ordering};

const MAX_COMPACT_SIZE: u64 = 1024;

/// `KvStore` stores key-value pairs in memory.
///
/// The pairs are stored in an internal HashMap.
///
/// Example
///
/// ```rust
/// use std::env;
/// use kvs::{KvsEngine, KvStore};
///
/// let mut kv = KvStore::open(env::current_dir().expect("current dir error")).expect("open error");
/// kv.set("key".to_owned(), "value".to_owned()).expect("set error");
/// let val = kv.get("key".to_owned()).expect("get error");
/// assert_eq!(val, Some("value".to_owned()));
///
/// ```
pub struct KvStore {
    writer: Arc<Mutex<KvStoreWriter>>,
    index: Arc<SkipMap<String, CommandPos>>,
    readers: KvStoreReader,
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

impl Clone for KvStore {
    fn clone(&self) -> Self {
        KvStore {
            writer: Arc::clone(&self.writer),
            index: Arc::clone(&self.index),
            readers: self.readers.clone(),
        }
    }
}

impl KvsEngine for KvStore {
    /// Sets a pair of key-value.
    ///
    /// The value will be overwritten if the key has existed.
    fn set(&self, key: String, value: String) -> Result<()> {
        self.writer.lock().unwrap().set(key, value)?;
        Ok(())
    }

    /// Gets the string value of the given string key.
    ///
    /// Returns `None` if the key does not exist.
    fn get(&self, key: String) -> Result<Option<String>> {
        return match self.index.get(&key) {
            Some(entry) => {
                let command_pos = entry.value();
                let command = self.readers.read_command(command_pos)?;
                Ok(Some(command.value))
            }
            None => Ok(None),
        };
    }

    /// Removes a given key.
    ///
    /// Does nothing if the key does not exist.
    fn remove(&self, key: String) -> Result<()> {
        self.writer.lock().unwrap().remove(key)?;
        Ok(())
    }
}

impl KvStore {
    /// Open file to store log
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();

        fs::create_dir_all(&path)?;
        let ids = gen_log_file_id(&path)?;
        let cur_file_id = ids.last().unwrap_or(&0) + 1;

        let mut readers = HashMap::new();
        let mut index = SkipMap::new();
        let mut uncompacted = 0;
        for id in ids {
            let file = path.join(format!("{}.log", id));
            let f = fs::File::open(file)?;
            let reader = BufReader::new(f);
            let mut reader = BufReaderWithPos::new(reader)?;

            uncompacted += load_data_from_file(id, &mut reader, &mut index)?;

            readers.insert(id, reader);
        }

        let path = Arc::new(path);
        let readers = KvStoreReader {
            readers: RefCell::new(readers),
            cur_file_id: Arc::new(AtomicU64::new(cur_file_id)),
            path: Arc::clone(&path),
        };

        let index = Arc::new(index);

        let writer = new_log_file(cur_file_id, &path)?;
        let writer = Arc::new(Mutex::new(KvStoreWriter {
            writer,
            readers: readers.clone(),
            index: Arc::clone(&index),
            path: Arc::clone(&path),
            file_id: cur_file_id,
            uncompacted,
        }));

        Ok(KvStore {
            writer,
            index,
            readers,
        })
    }
}

fn new_log_file(file_id: u64, path: &Path) -> Result<BufWriterWithPos> {
    let log_file = path.join(format!("{}.log", file_id));
    let f = match fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .append(true)
        .open(log_file.as_path())
    {
        Ok(f) => f,
        Err(e) => return Err(Error::IoError(e)),
    };

    let reader = BufReader::new(f.try_clone()?);
    let mut reader = BufReaderWithPos::new(reader)?;
    reader.seek(SeekFrom::Start(0))?;

    let writer = BufWriter::new(f.try_clone()?);
    let writer = BufWriterWithPos::new(writer)?;
    Ok(writer)
}

fn gen_log_file_id(path: &PathBuf) -> Result<Vec<u64>> {
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
    index: &mut SkipMap<String, CommandPos>,
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
                    if index.contains_key(&command.key) {
                        uncompacted += (offset_end - offset) as u64;
                    }
                    index.insert(command.key, command_pos);
                }
                CommandType::Remove => {
                    if let Some(command_pos) = index.remove(&command.key) {
                        uncompacted += command_pos.value().len as u64;
                    }
                }
            };
            offset = offset_end;
        };
    }
    Ok(uncompacted)
}

struct KvStoreReader {
    readers: RefCell<HashMap<u64, BufReaderWithPos>>,
    cur_file_id: Arc<AtomicU64>,
    path: Arc<PathBuf>,
}

impl KvStoreReader {
    fn read_command(&self, cmd_pos: &CommandPos) -> Result<Command> {
        let mut readers = self.readers.borrow_mut();
        if let hash_map::Entry::Vacant(_) = readers.entry(cmd_pos.file_id) {
            let file = self.path.join(format!("{}.log", cmd_pos.file_id));
            let f = fs::File::open(file)?;
            readers.insert(cmd_pos.file_id, BufReaderWithPos::new(BufReader::new(f))?);
        };

        let reader = readers.get_mut(&cmd_pos.file_id).unwrap();
        reader.seek(SeekFrom::Start(cmd_pos.pos))?;
        let mut cmd_reader = reader.take(cmd_pos.len as u64);
        let cmd: Command = serde_json::from_reader(&mut cmd_reader)?;
        Ok(cmd)
    }

    fn read_and_copy<W: Write>(&self, cmd_pos: &CommandPos, writer: &mut W) -> Result<u64> {
        let mut readers = self.readers.borrow_mut();
        if let hash_map::Entry::Vacant(_) = readers.entry(cmd_pos.file_id) {
            let file = self.path.join(format!("{}.log", cmd_pos.file_id));
            let f = fs::File::open(file)?;
            readers.insert(cmd_pos.file_id, BufReaderWithPos::new(BufReader::new(f))?);
        };

        let reader = readers.get_mut(&cmd_pos.file_id).unwrap();
        reader.seek(SeekFrom::Start(cmd_pos.pos))?;
        let mut cmd_reader = reader.take(cmd_pos.len as u64);
        let len = io::copy(&mut cmd_reader, writer)?;
        Ok(len)
    }

    fn close_files(&mut self) {
        let mut readers = self.readers.borrow_mut();
        while !readers.is_empty() {
            let file_id = *readers.keys().next().unwrap();
            if file_id >= self.cur_file_id.load(Ordering::SeqCst) {
                break;
            }
            readers.remove(&file_id);
        }
    }
}

impl Clone for KvStoreReader {
    fn clone(&self) -> Self {
        KvStoreReader {
            readers: RefCell::new(HashMap::new()),
            cur_file_id: Arc::clone(&self.cur_file_id),
            path: Arc::clone(&self.path),
        }
    }
}

struct KvStoreWriter {
    writer: BufWriterWithPos,
    readers: KvStoreReader,
    index: Arc<SkipMap<String, CommandPos>>,
    path: Arc<PathBuf>,
    file_id: u64,
    uncompacted: u64,
}

impl KvStoreWriter {
    fn set(&mut self, key: String, value: String) -> Result<()> {
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

        // key-value has saved, then increase the uncompacted length
        if self.index.contains_key(&key) {
            self.uncompacted += command_pos.len as u64;
        }

        // insert or overwrite
        self.index.insert(key, command_pos);

        if self.uncompacted > MAX_COMPACT_SIZE {
            self.compact()?;
        }
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        match self.index.remove(&key) {
            Some(entry) => {
                let command_pos = entry.value();
                self.uncompacted += command_pos.len as u64;
                let command = Command {
                    command_type: CommandType::Remove,
                    key: key.clone(),
                    value: String::new(),
                };
                let command_serialize = serde_json::to_vec(&command)?;
                self.writer.write_all(&command_serialize)?;
            }
            None => return Err(Error::RecordNotFound),
        }
        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        let compact_file_id = self.file_id + 1;
        self.file_id = compact_file_id + 1;
        self.writer = new_log_file(self.file_id, &self.path)?;
        let mut writer = new_log_file(compact_file_id, &self.path)?;
        let mut new_pos = 0;
        for entry in self.index.iter() {
            let command_pos = entry.value();
            let len = self.readers.read_and_copy(command_pos, &mut writer)?;
            let new_command_pos = CommandPos {
                file_id: compact_file_id,
                pos: new_pos,
                len: len as usize,
            };
            self.index.insert(entry.key().to_string(), new_command_pos);
            new_pos += len;
        }
        writer.flush()?;

        let rm_ids: Vec<u64> = gen_log_file_id(&self.path)?
            .into_iter()
            .filter(|&id| id < compact_file_id)
            .collect();
        for id in rm_ids {
            fs::remove_file(self.path.join(format!("{}.log", id)))?;
        }
        self.readers.close_files();

        self.uncompacted = 0;
        Ok(())
    }
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
