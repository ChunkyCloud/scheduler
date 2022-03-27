use std::cell::Cell;
use log::*;
use tungstenite::http::Uri;
use uuid::Uuid;
use crate::{Backend, MessageWsStream};
use crate::scheduler::message::{Message, TaskMessage};
use crate::util::error::Result;

pub async fn accept(_peer_id: Uuid, stream: MessageWsStream, _uri: Uri, backend: Backend) -> Result<()> {
    // Authenticate
    stream.send(Message::AuthenticationRequest()).await?;
    loop {
        match stream.poll().await? {
            Message::Authentication(m) => {
                info!(target: stream.target(), "API key: {}", m.token);
                break;
            }
            m => {
                debug!(target: stream.target(), "Incorrect message: {:?}", m);
            }
        }
        stream.send(Message::AuthenticationRequest()).await?;
    }

    // Handle task messages
    let guard = TaskGuard {
        task: Cell::new(Option::None),
        backend: backend.clone(),
    };
    loop {
        let task = guard.task.take();
        match stream.poll().await? {
            Message::TaskGet() => {
                match task {
                    None => {
                        let task = backend.scheduler.poll().await;
                        guard.task.set(Some(task.clone()));
                        stream.send(Message::Task(task)).await?;
                    }
                    Some(task) => {
                        // Illegal state
                        info!(target: stream.target(), "Illegal state. Rendering task {:?} but TaskGet received. Rescheduling task.", &task);
                        backend.scheduler.fail(task);
                        stream.send(Message::error_message("Illegal state. Task assigned and was expecting TaskComplete but TaskGet message received.")).await?;
                        stream.close().await?;
                    }
                }
            },
            Message::TaskComplete() => {
                match task {
                    Some(task) => {
                        info!(target: stream.target(), "Task completed: {:?}", &task);
                        backend.scheduler.complete(task);
                        guard.task.set(None);
                    }
                    None => {
                        // Illegal state
                        info!(target: stream.target(), "Illegal state. No task assigned but TaskComplete received.");
                        stream.send(Message::error_message("Illegal state. No task assigned but TaskComplete received.")).await?;
                        stream.close().await?;
                    }
                }
            }
            m => {
                debug!(target: stream.target(), "Incorrect message: {:?}", m);
            }
        }
    }
}

struct TaskGuard {
    task: Cell<Option<TaskMessage>>,
    backend: Backend,
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            self.backend.scheduler.fail(task);
        }
    }
}
