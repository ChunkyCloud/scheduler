use futures_util::{SinkExt, StreamExt};
use log::*;
use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;
use tungstenite::http::Uri;
use uuid::Uuid;
use crate::Backend;
use crate::message::Message;
use crate::scheduler::Task;
use crate::error::Result;

pub async fn accept(peer_id: Uuid, mut stream: WebSocketStream<TcpStream>, _uri: Uri, backend: Backend) -> Result<()> {
    let peer_target = &peer_id.to_string();

    let e = Message::AuthenticationRequest().to_ws_message()?;
    stream.send(e.clone()).await?;
    while let Some(resp) = stream.next().await {
        let resp = resp?;
        if resp.is_text() {
            let text = resp.to_text()?;
            debug!(target: peer_target, "Received: {}", text);
            let m: serde_json::Result<Message> = serde_json::from_str(text);
            match m {
                Ok(Message::Authentication(m)) => {
                    if let Some(admin_token) = &backend.admin_token {
                        if m.token.eq(admin_token) {
                            break;
                        }
                        info!(target: peer_target, "Incorrect API token");
                    } else {
                        break;
                    }
                },
                Ok(m) => {
                    debug!(target: peer_target, "Incorrect message: {:?}", m);
                }
                Err(m) => {
                    debug!(target: peer_target, "Invalid message: {}", m);
                }
            }
        }
        stream.send(e.clone()).await?;
    }

    while let Some(msg) = stream.next().await {
        let msg = msg?;
        if msg.is_text() {
            let text = msg.to_text()?;
            debug!(target: peer_target, "Received: {}", text);
            let m: serde_json::Result<Message> = serde_json::from_str(text);
            match m {
                Ok(Message::Task(task)) => {
                    backend.work_queue.push(Task::from_message(task));
                    stream.send(Message::TaskAck().to_ws_message()?).await?;
                },
                Ok(m) => {
                    debug!(target: peer_target, "Incorrect message: {:?}", m);
                }
                Err(m) => {
                    debug!(target: peer_target, "Invalid message: {}", m);
                }
            }
        }
    }

    Ok(())
}
