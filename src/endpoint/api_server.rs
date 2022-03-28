use log::*;
use tungstenite::http::Uri;
use uuid::Uuid;
use crate::{Backend, MessageWsStream};
use crate::scheduler::message::Message;
use crate::util::error::Result;

pub async fn accept(_peer_id: Uuid, stream: MessageWsStream, _uri: Uri, backend: Backend) -> Result<()> {
    // Authenticate
    stream.send(Message::AuthenticationRequest()).await?;
    loop {
        match stream.poll().await? {
            Message::Authentication(m) => {
                if let Some(admin_token) = &backend.admin_token {
                    if m.token.eq(admin_token) {
                        break;
                    }
                    info!(target: stream.target(), "Incorrect API token");
                } else {
                    break;
                }
            }
            m => {
                debug!(target: stream.target(), "Incorrect message: {:?}", m);
            }
        }
        stream.send(Message::AuthenticationRequest()).await?;
    }

    // Handle task messages
    loop {
        match stream.poll().await? {
            Message::Task(task) => {
                debug!(target: stream.target(), "Job received: {:?}", &task);
                backend.scheduler.submit(task).await;
            },
            m => {
                debug!(target: stream.target(), "Incorrect message: {:?}", m);
            }
        }
    }
}
