use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use log::{error, info};
use crate::{err, KvsEngine};
use err::Result;
use crate::common::{Request, Response, ResponseBody};
use crate::err::Error;

/// KvsServer contains engine
pub struct KvsServer<T: KvsEngine> {
    engine: T,
}

impl<T: KvsEngine> KvsServer<T> {
    /// New KvsServer with engine
    pub fn new(engine: T) -> Self {
        KvsServer { engine }
    }

    /// Run to listen the addr and process commands from client
    pub fn run<A: ToSocketAddrs>(&mut self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Err(e) =  self.handle(stream) {
                            error!("handle err: {:?}", e);
                            return Err(Error::Unknown);
                    }
                },
                Err(e) => {
                    error!("connection err {}", e);
                    return Err(Error::Unknown);
                },
            }
        }
        Ok(())
    }


    fn handle(&mut self, stream: TcpStream) -> Result<()> {
        let reader = BufReader::new(&stream);
        let mut writer = BufWriter::new(&stream);

        let req_iter = serde_json::Deserializer::from_reader(reader)
            .into_iter::<Request>();
        for req in req_iter {
            let req = req?;
            info!("rep {:?}", req);
            let rsp = match req {
                Request::Get { key } => {
                    match self.engine.get(key) {
                        Ok(val) => Response {
                            body: ResponseBody::Ok(val)
                        },
                        Err(e) => {
                            error!("get error {:?}", e);
                            Response {
                                body: ResponseBody::Err(format!("{:?}", e))
                            }
                        }
                    }
                },
                Request::Set { key, value } => {
                    match self.engine.set(key, value) {
                        Ok(()) => Response {
                            body: ResponseBody::Ok(None)
                        },
                        Err(e) => {
                            error!("set error {:?}", e);
                            Response {
                                body: ResponseBody::Err(format!("{:?}", e))
                            }
                        }
                    }
                }
                Request::Remove { key } => {
                    match self.engine.remove(key) {
                        Ok(()) => Response {
                            body: ResponseBody::Ok(None)
                        },
                        Err(e) => {
                            error!("rm error {:?}", e);
                            Response { body: ResponseBody::Err(e.to_string()) }
                        }
                    }
                }
            };
            info!("rsp {:?}", rsp);
            serde_json::to_writer(&mut writer, &rsp).unwrap();
            writer.flush()?;
        }

        Ok(())
    }
}



