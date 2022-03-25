use futures_util::{SinkExt, StreamExt};
use log::*;
use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;
use tungstenite::http::Uri;
use uuid::Uuid;
use crate::Backend;
use crate::message::Message;
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
                    info!(target: peer_target, "API key: {}", m.token);
                    break;
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
                Ok(Message::TaskGet()) => {
                    let task = match backend.work_queue.pop() {
                        Some(v) => v,
                        None => {
                            stream.send(Message::TaskWait()
                                .to_ws_message()
                                .expect("Error serializing TaskWait message."))
                                .await?;
                            backend.work_queue.poll().await
                        },
                    };

                    let task = task.to_message().to_ws_message()?;
                    stream.send(task).await?;
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
