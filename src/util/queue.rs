use std::sync::Arc;
use async_channel::{Receiver, Sender};
use crate::util::error::{Result, Error};

#[derive(Debug)]
pub enum QueueError {
    Closed,
}

struct QueueImpl<T> {
    sender: Sender<T>,
    receiver: Receiver<T>,
}

pub struct Queue<T> {
    queue: Arc<QueueImpl<T>>
}

impl <T> Queue<T> {
    #[allow(dead_code)]
    pub fn bounded(bound: usize) -> Queue<T> {
        let (sender, receiver) = async_channel::bounded(bound);
        Queue { queue: Arc::new(QueueImpl { sender, receiver } ) }
    }

    pub fn unbounded() -> Queue<T> {
        let (sender, receiver) = async_channel::unbounded();
        Queue { queue: Arc::new(QueueImpl { sender, receiver } ) }
    }

    pub async fn send(&self, message: T) -> Result<()> {
        if self.queue.sender.send(message).await.is_ok() {
            Ok(())
        } else {
            Err(Error::from(QueueError::Closed))
        }
    }

    pub async fn poll(&self) -> Result<T> {
        if let Ok(v) = self.queue.receiver.recv().await {
            Ok(v)
        } else {
            Err(Error::from(QueueError::Closed))
        }
    }

    pub fn len(&self) -> usize {
        self.queue.receiver.len()
    }

    #[allow(dead_code)]
    pub fn close(&self) {
        self.queue.sender.close();
        self.queue.receiver.close();
    }
}
