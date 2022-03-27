use crate::util::queue;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
#[allow(dead_code)]
pub enum Error {
    Ws(Box<tungstenite::Error>),
    Serde(Box<serde_json::Error>),
    Queue(Box<queue::Error>),
    Mongo(Box<mongodb::error::Error>),
    Generic(String),
}

impl From<tungstenite::Error> for Error {
    fn from(e: tungstenite::Error) -> Self {
        Error::Ws(Box::new(e))
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Serde(Box::new(e))
    }
}

impl From<queue::Error> for Error {
    fn from(e: queue::Error) -> Self {
        Error::Queue(Box::new(e))
    }
}

impl From<mongodb::error::Error> for Error {
    fn from(e: mongodb::error::Error) -> Self {
        Error::Mongo(Box::new(e))
    }
}
