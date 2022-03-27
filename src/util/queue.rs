use std::collections::BinaryHeap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub enum Error {
    Full,
    Closed,
}

struct BoundedChannel<T> {
    queue: crossbeam::queue::ArrayQueue<T>,
    notify: tokio::sync::Notify,
    closed: AtomicBool,
}

pub mod mpsc {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use crate::util::queue::{BoundedChannel, Error};

    pub fn new<T>(bound: usize) -> (Sender<T>, Receiver<T>) {
        let channel = Arc::new(BoundedChannel {
            queue: crossbeam::queue::ArrayQueue::new(bound),
            notify: tokio::sync::Notify::new(),
            closed: AtomicBool::new(false),
        });
        (Sender { channel: channel.clone(), count: Arc::new(AtomicUsize::new(1)) }, Receiver { channel })
    }

    pub struct Receiver<T> {
        channel: Arc<BoundedChannel<T>>
    }

    impl <T> Receiver<T> {
        pub async fn poll(&self) -> Result<T, Error> {
            loop {
                if let Some(item) = self.channel.queue.pop() {
                    return Ok(item);
                }
                if self.channel.closed.load(Ordering::Relaxed) {
                    return Err(Error::Closed);
                }
                self.channel.notify.notified().await;
            }
        }

        pub fn close(&self) {
            self.channel.closed.swap(true, Ordering::Relaxed);
        }
    }

    impl <T> Drop for Receiver<T> {
        fn drop(&mut self) {
            self.close();
        }
    }

    pub struct Sender<T> {
        channel: Arc<BoundedChannel<T>>,
        count: Arc<AtomicUsize>,
    }

    impl <T> Sender<T> {
        pub async fn send(&self, item: T) -> Result<(), Error> {
            if self.channel.queue.push(item).is_err() {
                return Err(Error::Full);
            }
            if self.channel.closed.load(Ordering::Relaxed) {
                return Err(Error::Closed);
            }
            self.channel.notify.notify_one();
            Ok(())
        }

        pub fn close(&self) {
            self.channel.closed.swap(true, Ordering::Relaxed);
        }
    }

    impl <T> Clone for Sender<T> {
        fn clone(&self) -> Self {
            self.count.fetch_add(1, Ordering::Relaxed);
            Sender {
                channel: self.channel.clone(),
                count: self.count.clone(),
            }
        }
    }

    impl <T> Drop for Sender<T> {
        fn drop(&mut self) {
            if self.count.fetch_sub(1, Ordering::Relaxed) == 0 {
                self.close();
            }
        }
    }
}

pub mod mpmc {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use crate::util::queue::{BoundedChannel, Error};

    pub fn new<T>(bound: usize) -> (Sender<T>, Receiver<T>) {
        let channel = Arc::new(BoundedChannel {
            queue: crossbeam::queue::ArrayQueue::new(bound),
            notify: tokio::sync::Notify::new(),
            closed: AtomicBool::new(false),
        });
        (
            Sender { channel: channel.clone(), count: Arc::new(AtomicUsize::new(1)) },
            Receiver { channel, count: Arc::new(AtomicUsize::new(1)) }
        )
    }

    pub struct Receiver<T> {
        channel: Arc<BoundedChannel<T>>,
        count: Arc<AtomicUsize>,
    }

    impl <T> Receiver<T> {
        pub async fn poll(&self) -> Result<T, Error> {
            loop {
                let waiter = self.channel.notify.notified();
                if let Some(item) = self.channel.queue.pop() {
                    self.channel.notify.notify_waiters();
                    return Ok(item);
                }
                if self.channel.closed.load(Ordering::Relaxed) {
                    return Err(Error::Closed);
                }
                waiter.await;
            }
        }

        pub fn close(&self) {
            self.channel.closed.swap(true, Ordering::Acquire);
            self.channel.notify.notify_waiters();
        }
    }

    impl <T> Clone for Receiver<T> {
        fn clone(&self) -> Self {
            self.count.fetch_add(1, Ordering::Relaxed);
            Receiver {
                channel: self.channel.clone(),
                count: self.count.clone(),
            }
        }
    }

    impl <T> Drop for Receiver<T> {
        fn drop(&mut self) {
            if self.count.fetch_sub(1, Ordering::Relaxed) == 0 {
                self.close();
            }
        }
    }

    pub struct Sender<T> {
        channel: Arc<BoundedChannel<T>>,
        count: Arc<AtomicUsize>,
    }

    impl <T> Sender<T> {
        #[allow(dead_code)]
        pub async fn send(&self, item: T) -> Result<(), Error> {
            if self.channel.queue.push(item).is_err() {
                return Err(Error::Full);
            }
            if self.channel.closed.load(Ordering::Relaxed) {
                return Err(Error::Closed);
            }
            self.channel.notify.notify_waiters();
            Ok(())
        }

        pub async fn send_blocking(&self, item: T) -> Result<(), Error> {
            let mut item = item;
            loop {
                let waiter = self.channel.notify.notified();
                match self.channel.queue.push(item) {
                    Err(i) => {
                        item = i;
                    }
                    Ok(_) => {
                        self.channel.notify.notify_waiters();
                        return Ok(())
                    }
                }
                if self.channel.closed.load(Ordering::Relaxed) {
                    return Err(Error::Closed);
                }
                waiter.await;
            }
        }

        pub fn close(&self) {
            self.channel.closed.swap(true, Ordering::Acquire);
            self.channel.notify.notify_waiters();
        }
    }

    impl <T> Clone for Sender<T> {
        fn clone(&self) -> Self {
            self.count.fetch_add(1, Ordering::Relaxed);
            Sender {
                channel: self.channel.clone(),
                count: self.count.clone(),
            }
        }
    }

    impl <T> Drop for Sender<T> {
        fn drop(&mut self) {
            if self.count.fetch_sub(1, Ordering::Relaxed) == 0 {
                self.close();
            }
        }
    }
}


#[derive(Clone)]
pub struct PriorityQueue<T> {
    heap: Arc<Mutex<BinaryHeap<T>>>,
    notify: Arc<tokio::sync::Notify>,
}

impl <T> PriorityQueue<T> where T: Ord {
    pub fn new() -> PriorityQueue<T> {
        PriorityQueue {
            heap: Arc::new(Mutex::new(BinaryHeap::new())),
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    pub fn push(&self, item: T) {
        self.heap.lock().unwrap().push(item);
        self.notify.notify_waiters();
    }

    pub async fn poll(&self) -> T {
        loop {
            let waiter = self.notify.notified();
            if let Some(item) = self.heap.lock().unwrap().pop() {
                return item;
            }
            waiter.await;
        }
    }
}
