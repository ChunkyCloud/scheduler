use log::*;
use tungstenite::http::Uri;
use uuid::Uuid;
use crate::{Backend, MessageWsStream};
use crate::scheduler::message::Message;
use crate::util::error::Result;

pub async fn accept(_peer_id: Uuid, stream: MessageWsStream, _uri: Uri, backend: Backend) -> Result<()> {
    // Ask for authentication
    stream.send(Message::AuthenticationRequest()).await?;
    match stream.poll().await? {
        Message::Authentication(m) => {
            // Authenticate if an admin token was supplied
            if let Some(admin_token) = backend.admin_token {
                if m.token.eq(&admin_token) {
                    info!(target: stream.target(), "Authenticated to admin token");
                } else {
                    info!(target: stream.target(), "Incorrect API token");
                    return stream.close().await;
                }
            } else {
                info!(target: stream.target(), "No admin token supplied. User responded with token: {}", m.token);
            }
        }
        m => {
            info!(target: stream.target(), "Incorrect message received: {:?}", m);
            return stream.close().await;
        }
    }

    // Handle task messages
    loop {
        match stream.poll().await? {
            Message::Task(task) => {
                debug!(target: stream.target(), "Job received: {:?}", &task);
                backend.scheduler.submit(task).await?;
            },
            m => {
                debug!(target: stream.target(), "Incorrect message: {:?}", m);
            }
        }
    }
}
