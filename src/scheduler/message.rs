use bson::oid::ObjectId;
use serde::{Deserialize, Serialize};
use crate::util::error::{Result, Error};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum Message {
    /// Some error that will result in this connection being closed.
    Error(ErrorMessage),
    /// Some warning that should be conveyed to the node. Connection will not be closed.
    Warning(WarningMessage),

    /// A request for authentication.
    AuthenticationRequest(),
    /// An authentication response message with a token.
    Authentication(AuthenticationMessage),

    /// A request for a task.
    TaskGet(),
    /// A task message containing a job id and a number of samples to render.
    Task(TaskMessage),
    /// A notification that a task has been completed.
    TaskComplete(),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ErrorMessage {
    pub message: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct WarningMessage {
    pub message: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct AuthenticationMessage {
    pub token: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TaskMessage {
    #[serde(skip)]
    pub task_id: Option<ObjectId>,
    pub job_id: String,
    pub spp: u32,
}


impl Message {
    pub fn to_ws_message(&self) -> Result<tungstenite::Message> {
        match serde_json::to_string(&self) {
            Ok(v) => Ok(tungstenite::Message::Text(v)),
            Err(e) => Err(Error::from(e)),
        }
    }
    
    pub fn error_message(message: &str) -> Message {
        Message::Error(ErrorMessage { message: message.to_string() })
    }
}
