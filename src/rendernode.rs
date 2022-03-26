use std::time::Duration;
use futures_util::stream::SplitStream;
use futures_util::StreamExt;
use log::*;
use tokio::net::TcpStream;
use tokio::try_join;
use tokio_tungstenite::WebSocketStream;
use tungstenite::http::Uri;
use uuid::Uuid;
use crate::Backend;
use crate::message::Message;
use crate::error::Result;
use crate::queue::Queue;
use crate::util::HandshakingSink;

pub async fn accept(peer_id: Uuid, stream: WebSocketStream<TcpStream>, _uri: Uri, backend: Backend) -> Result<()> {
    let peer_target = peer_id.to_string();
    let message_queue: Queue<Message> = Queue::new();

    let (outgoing, incoming) = stream.split();
    let outgoing = HandshakingSink::new(outgoing, Duration::from_secs(30));

    try_join!(
        outgoing.handshake(),
        handle_message(message_queue.clone(), incoming, &peer_target),
        handle(message_queue.clone(), &outgoing, &peer_target, backend)
    )?;

    Ok(())
}

async fn handle_message(message_queue: Queue<Message>, mut incoming: SplitStream<WebSocketStream<TcpStream>>, peer_target: &str) -> Result<()> {
    while let Some(msg) = incoming.next().await {
        let msg = msg?;
        if msg.is_text() {
            let text = msg.to_text()?;
            debug!(target: peer_target, "Received: {}", text);
            match serde_json::from_str(text) {
                Ok(m) => {
                    message_queue.push(m);
                }
                Err(m) => {
                    debug!(target: peer_target, "Invalid message: {}", m);
                }
            }
        }
    }
    Ok(())
}

async fn handle(message_queue: Queue<Message>, outgoing: &HandshakingSink, peer_target: &str, backend: Backend) -> Result<()> {
    // Request for authentication
    let e = Message::AuthenticationRequest().to_ws_message()?;
    outgoing.send(e.clone()).await?;
    loop {
        match message_queue.poll().await {
            Message::Authentication(m) => {
                info!(target: peer_target, "API key: {}", m.token);
                break;
            }
            m => {
                debug!(target: peer_target, "Incorrect message: {:?}", m);
            }
        }
        outgoing.send(e.clone()).await?;
    }

    // Handle task messages
    loop {
        match message_queue.poll().await {
            Message::TaskGet() => {
                let task = match backend.work_queue.pop() {
                    Some(v) => v,
                    None => {
                        outgoing.send(Message::TaskWait().to_ws_message()?).await?;
                        backend.work_queue.poll().await
                    },
                };

                let task = task.to_message().to_ws_message()?;
                outgoing.send(task).await?;
            },
            m => {
                debug!(target: peer_target, "Incorrect message: {:?}", m);
            }
        }
    }
}
