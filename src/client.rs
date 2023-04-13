use crate::common::{Request, Response, ResponseBody};
use crate::err;
use crate::err::Error;
use err::Result;
use serde::Deserialize;
use serde_json::de::IoRead;
use serde_json::Deserializer;
use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpStream, ToSocketAddrs};

/// KvsClient
/// Connect to remote server and send commands to server
pub struct KvsClient {
    writer: BufWriter<TcpStream>,
    reader: Deserializer<IoRead<BufReader<TcpStream>>>,
}

impl KvsClient {
    /// New a kvs client with socket addr
    pub fn new<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;
        let reader_stream = stream.try_clone()?;

        let writer = BufWriter::new(stream);
        let reader = BufReader::new(reader_stream);
        let reader = serde_json::Deserializer::from_reader(reader);
        Ok(KvsClient { writer, reader })
    }

    /// Get value of key from remote server
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let req = Request::Get { key };
        serde_json::to_writer(&mut self.writer, &req)?;
        self.writer.flush()?;

        let rsp = Response::deserialize(&mut self.reader)?;
        match rsp.body {
            ResponseBody::Ok(val) => Ok(val),
            ResponseBody::Err(e) => Err(Error::ClientGetError(e)),
        }
    }

    /// Set key-value to remote server
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let req = Request::Set { key, value };
        serde_json::to_writer(&mut self.writer, &req)?;
        self.writer.flush()?;

        let rsp = Response::deserialize(&mut self.reader)?;
        match rsp.body {
            ResponseBody::Ok(_) => Ok(()),
            ResponseBody::Err(e) => Err(Error::ClientSetError(e)),
        }
    }

    /// Remove key-value to remote server
    pub fn remove(&mut self, key: String) -> Result<()> {
        let req = Request::Remove { key };
        serde_json::to_writer(&mut self.writer, &req)?;
        self.writer.flush()?;

        let rsp = Response::deserialize(&mut self.reader)?;
        match rsp.body {
            ResponseBody::Ok(_) => Ok(()),
            ResponseBody::Err(e) => Err(Error::ClientRemoveError(e)),
        }
    }
}
