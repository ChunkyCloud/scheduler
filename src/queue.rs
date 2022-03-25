use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

pub struct Queue<T> {
    q: std::sync::Arc<crossbeam::queue::SegQueue<T>>,
    wakers: std::sync::Arc<crossbeam::queue::SegQueue<Waker>>,
}

struct QueuePollFuture<T> {
    q: Queue<T>,
}

impl <T> Future for QueuePollFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.q.q.pop() {
            Some(v) => Poll::Ready(v),
            None => {
                self.q.wakers.push(cx.waker().clone());
                Poll::Pending
            },
        }
    }
}

impl <T> Clone for Queue<T> {
    fn clone(&self) -> Self {
        Queue {
            q: self.q.clone(),
            wakers: self.wakers.clone(),
        }
    }
}

#[allow(dead_code)]
impl <T> Queue<T> {
    /// Create a new unbounded queue
    pub fn new() -> Queue<T> {
        Queue {
            q: std::sync::Arc::new(crossbeam::queue::SegQueue::new()),
            wakers: std::sync::Arc::new(crossbeam::queue::SegQueue::new()),
        }
    }

    /// Push an item into the queue
    pub fn push(&self, value: T) {
        self.q.push(value);
        if let Some(waker) = self.wakers.pop() {
            waker.wake();
        }
    }

    /// Pop an item from the queue. `None` if the queue is empty.
    pub fn pop(&self) -> Option<T> {
        self.q.pop()
    }

    /// Poll for an item from the queue
    pub async fn poll(&self) -> T {
        QueuePollFuture { q: self.clone() }.await
    }

    /// Return `true` if the queue is empty at the time of calling
    pub fn is_empty(&self) -> bool {
        self.q.is_empty()
    }

    /// Return the number of elements in the queue at the time of calling
    pub fn len(&self) -> usize {
        self.q.len()
    }
}
