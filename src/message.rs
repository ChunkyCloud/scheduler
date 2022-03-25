use serde::{Deserialize, Serialize};
use crate::error::{Result, Error};

#[derive(Serialize, Deserialize, Debug)]
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
    /// A response that there is no task currently available. A new task will be sent as soon
    /// as one is available.
    TaskWait(),
    /// A task message containing a job id and a number of samples to render.
    Task(TaskMessage),
    /// An acknowledgement a task has been received and will be processed
    TaskAck(),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ErrorMessage {
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WarningMessage {
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AuthenticationMessage {
    pub token: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskMessage {
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
