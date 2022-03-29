use crate::queue::QueueError;
use crate::util::queue;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
#[allow(dead_code)]
pub enum Error {
    Ws(Box<tungstenite::Error>),
    Serde(Box<serde_json::Error>),
    Queue(Box<queue::QueueError>),
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

impl From<queue::QueueError> for Error {
    fn from(e: queue::QueueError) -> Self {
        Error::Queue(Box::new(e))
    }
}

impl <T> From<async_channel::SendError<T>> for Error {
    fn from(_: async_channel::SendError<T>) -> Self {
        Error::Queue(Box::new(QueueError::Closed))
    }
}

impl From<async_channel::RecvError> for Error {
    fn from(_: async_channel::RecvError) -> Self {
        Error::Queue(Box::new(QueueError::Closed))
    }
}

impl From<mongodb::error::Error> for Error {
    fn from(e: mongodb::error::Error) -> Self {
        Error::Mongo(Box::new(e))
    }
}
