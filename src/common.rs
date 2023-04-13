use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum Request {
    Get { key: String },
    Set { key: String, value: String },
    Remove { key: String },
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Response {
    pub body: ResponseBody,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum ResponseBody {
    Ok(Option<String>),
    Err(String),
}
