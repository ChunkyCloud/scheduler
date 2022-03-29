use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant};
use async_channel::{Receiver, Sender};
use crossbeam::atomic::AtomicCell;
use futures_util::{SinkExt, StreamExt};
use futures_util::stream::{SplitSink, SplitStream};
use log::debug;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::try_join;
use tokio_tungstenite::WebSocketStream;
use crate::scheduler::message::Message;
use crate::util::error::Result;

/// We really shouldn't ever have more than a handful of messages buffered.
const BUFFER_SIZE: usize = 8;

#[derive(Clone)]
pub struct MessageWsStream {
    rx_message_receiver: Receiver<Message>,
    tx_message_sender: Sender<NotifyingMessage>,
    target: String,
}

pub struct MessageWsStreamHandler<T> where T: AsyncRead + AsyncWrite + Unpin {
    rx_message_sender: Sender<Message>,
    tx_message_receiver: Receiver<NotifyingMessage>,
    message_stream: MessageWsStream,
    heartbeat_interval: Duration,
    stream: tokio_tungstenite::WebSocketStream<T>,
    target: String,
}

#[derive(Clone)]
struct NotifyingMessage {
    message: tungstenite::Message,
    notify: Arc<tokio::sync::Notify>,
}

impl From<tungstenite::Message> for NotifyingMessage {
    fn from(message: tungstenite::Message) -> Self {
        NotifyingMessage {
            message,
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }
}

impl Debug for NotifyingMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.message.fmt(f)
    }
}

impl MessageWsStream {
    pub async fn send(&self, message: Message) -> Result<()> {
        self.send_message(message.to_ws_message()?).await
    }

    pub async fn poll(&self) -> Result<Message> {
        Ok(self.rx_message_receiver.recv().await?)
    }

    pub async fn close(&self) -> Result<()> {
        self.send_message(tungstenite::Message::Close(None)).await
    }

    async fn send_message(&self, message: tungstenite::Message) -> Result<()> {
        let message = NotifyingMessage::from(message);
        let notify = message.notify.clone();
        let waiter = notify.notified();

        self.tx_message_sender.send(message).await?;
        waiter.await;
        Ok(())
    }

    pub fn target(&self) -> &str {
        &self.target
    }
}

impl <T> MessageWsStreamHandler<T> where T: AsyncRead + AsyncWrite + Unpin {
    pub fn new(stream: tokio_tungstenite::WebSocketStream<T>, target: &str, heartbeat_interval: &Duration) -> MessageWsStreamHandler<T> {
        let (rx_m_s, rx_m_r) = async_channel::bounded(BUFFER_SIZE);
        let (tx_m_s, tx_m_r) = async_channel::bounded(BUFFER_SIZE);
        MessageWsStreamHandler {
            rx_message_sender: rx_m_s,
            tx_message_receiver: tx_m_r,
            message_stream: MessageWsStream {
                rx_message_receiver: rx_m_r,
                tx_message_sender: tx_m_s,
                target: target.to_string(),
            },
            heartbeat_interval: *heartbeat_interval,
            stream,
            target: target.to_string(),
        }
    }

    pub fn message_stream(&self) -> MessageWsStream {
        self.message_stream.clone()
    }

    pub async fn handle(self) -> Result<()> where T: AsyncRead + AsyncWrite + Unpin {
        let (sink, stream) = self.stream.split();
        let target = self.target;
        let next_heartbeat = AtomicCell::new(Instant::now() + self.heartbeat_interval);
        let heartbeat_interval = self.heartbeat_interval;
        let tx_m_r = self.tx_message_receiver;
        let rx_m_s = self.rx_message_sender;

        try_join!(
            MessageWsStreamHandler::handle_tx(sink, tx_m_r, &target, &next_heartbeat, &heartbeat_interval),
            MessageWsStreamHandler::handle_rx(stream, rx_m_s, &target, &next_heartbeat, &heartbeat_interval),
            handle_heartbeat(self.message_stream.tx_message_sender.clone(), &next_heartbeat)
        )?;
        Ok(())
    }

    async fn handle_tx(mut sink: SplitSink<WebSocketStream<T>, tungstenite::Message>,
                       tx_m_r: Receiver<NotifyingMessage>,
                       target: &str,
                       next_heartbeat: &AtomicCell<Instant>,
                       heartbeat_interval: &Duration) -> Result<()> {
        loop {
            let message = tx_m_r.recv().await?;
            debug!(target: target, "Sending: {:?}", message);
            sink.send(message.message).await?;
            message.notify.notify_waiters();
            set_next_heartbeat(next_heartbeat, heartbeat_interval);
        }
    }

    async fn handle_rx(mut stream: SplitStream<WebSocketStream<T>>,
                       tx_m_s: Sender<Message>,
                       target: &str,
                       next_heartbeat: &AtomicCell<Instant>,
                       heartbeat_interval: &Duration) -> Result<()> {
        while let Some(message) = stream.next().await {
            set_next_heartbeat(next_heartbeat, heartbeat_interval);
            let message = message?;
            if message.is_text() {
                let text = message.to_text()?;
                debug!(target: target, "Received: {}", text);
                match serde_json::from_str(text) {
                    Ok(m) => {
                        tx_m_s.send(m).await?;
                    }
                    Err(m) => {
                        debug!(target: target, "Invalid message: {}", m);
                    }
                }
            }
        }
        Ok(())
    }
}

async fn handle_heartbeat(sender: Sender<NotifyingMessage>,
                          next_heartbeat: &AtomicCell<Instant>) -> Result<()> {
    loop {
        tokio::time::sleep_until(tokio::time::Instant::from(next_heartbeat.load())).await;
        if Instant::now() >= next_heartbeat.load() {
            let message = NotifyingMessage::from(tungstenite::Message::Ping(vec![]));
            let notify = message.notify.clone();
            let waiter = notify.notified();

            sender.send(message).await?;
            waiter.await;
        }
    }
}

fn set_next_heartbeat(next_heartbeat: &AtomicCell<Instant>,
                      heartbeat_interval: &Duration) {
    let new = Instant::now() + (*heartbeat_interval);
    let current = next_heartbeat.load();
    if new > current {
        let _ = next_heartbeat.compare_exchange(current, new);
    }
}
