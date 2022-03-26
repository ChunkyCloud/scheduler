use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::{Duration, Instant};
use futures_util::SinkExt;
use futures_util::stream::SplitSink;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, MutexGuard};
use tokio::time::sleep_until;
use tokio_tungstenite::WebSocketStream;
use crate::error::Result;

#[derive(Clone)]
pub struct HandshakingSink {
    inner: Arc<Mutex<HandshakingSinkImpl>>
}

struct HandshakingSinkImpl {
    sink: SplitSink<WebSocketStream<TcpStream>, tungstenite::Message>,
    heartbeat_interval: Duration,
    next_heartbeat: Instant,
}

pub struct HandshakingSinkGuard<'a> {
    guard: MutexGuard<'a, HandshakingSinkImpl>,
}

impl HandshakingSink {
    pub fn new(sink: SplitSink<WebSocketStream<TcpStream>, tungstenite::Message>, heartbeat_interval: Duration) -> HandshakingSink {
        HandshakingSink {
            inner: Arc::new(Mutex::new(HandshakingSinkImpl {
                sink,
                heartbeat_interval,
                next_heartbeat: Instant::now() + heartbeat_interval,
            }))
        }
    }

    pub async fn handshake(&self) -> Result<()> {
        loop {
            let next_heartbeat;
            {
                let mut inner = self.inner.lock().await;
                let now = Instant::now();
                if now >= inner.next_heartbeat {
                    inner.send_with_instant(tungstenite::Message::Ping(vec![]), now).await?;
                }
                next_heartbeat = tokio::time::Instant::from(inner.next_heartbeat);
            }
            sleep_until(next_heartbeat).await;
        }
    }

    pub async fn send(&self, message: tungstenite::Message) -> Result<()> {
        self.inner.lock().await.send(message).await?;
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn lock(&self) -> HandshakingSinkGuard<'_> {
        HandshakingSinkGuard {
            guard: self.inner.lock().await
        }
    }
}

impl HandshakingSinkImpl {
    async fn send(&mut self, message: tungstenite::Message) -> tungstenite::Result<()> {
        self.send_with_instant(message, Instant::now()).await
    }

    async fn send_with_instant(&mut self, message: tungstenite::Message, now: Instant) -> tungstenite::Result<()> {
        let res = self.sink.send(message).await;
        self.next_heartbeat = now + self.heartbeat_interval;
        return res;
    }
}

impl <'a> Deref for HandshakingSinkGuard<'a> {
    type Target = SplitSink<WebSocketStream<TcpStream>, tungstenite::Message>;

    fn deref(&self) -> &Self::Target {
        &self.guard.sink
    }
}

impl <'a> DerefMut for HandshakingSinkGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard.sink
    }
}
