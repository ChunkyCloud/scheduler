use std::cell::Cell;
use log::*;
use mongodb::bson::{doc, Document};
use tokio::runtime::Handle;
use tungstenite::http::Uri;
use uuid::Uuid;
use crate::{Backend, MessageWsStream};
use crate::scheduler::message::{Message, ServerInfoMessage, TaskMessage};
use crate::util::error::Result;

pub async fn accept(_peer_id: Uuid, stream: MessageWsStream, _uri: Uri, backend: Backend) -> Result<()> {
    // Give server information
    stream.send(Message::ServerInfo(ServerInfoMessage::new())).await?;

    // Authenticate
    stream.send(Message::AuthenticationRequest()).await?;
    match stream.poll().await? {
        Message::Authentication(m) => {
            // Authenticate if mongo client was supplied
            if let Some(client) = &backend.mongo_client {
                let filter = doc! { "apiKey": m.token };  // User with matching API key
                if let Some(user) = client
                    .database("renderservice")
                    .collection::<Document>("users")
                    .find_one(filter, None)
                    .await? {
                    // TODO: Use api server

                    // User found
                    info!(target: stream.target(), "User authenticated: {}", user.get_str("username").unwrap_or("<UNKNOWN>"));
                } else {
                    info!(target: stream.target(), "Failed to authenticate.");
                    return stream.close().await;
                }
            } else {
                info!(target: stream.target(), "No MongoDB client was supplied. User responded with token: {}", m.token);
            }
        }
        m => {
            info!(target: stream.target(), "Incorrect message: {:?}", m);
            return stream.close().await;
        }
    }
    stream.send(Message::AuthenticationOk()).await?;

    // Handle task messages
    loop {
        // Waiting for task
        let guard;
        if let Message::TaskGet() = stream.poll().await? {
            let task = backend.scheduler.poll().await?;
            guard = TaskGuard::new(&task, &backend);
            stream.send(Message::Task(task)).await?;
        } else {
            let message = "Illegal state: waiting. TaskGet message expected.";
            info!(target: stream.target(), "{}", message);
            stream.send(Message::error_message(message)).await?;
            return stream.close().await;
        }

        // Rendering task
        if let Message::TaskComplete() = stream.poll().await? {
            info!(target: stream.target(), "Task completed: {:?}", &guard.task);
            backend.scheduler.complete(guard.task.clone()).await?;
            TaskGuard::release(guard);
        } else {
            let message = "Illegal state: rendering. TaskComplete message expected.";
            info!(target: stream.target(), "{}", message);
            stream.send(Message::error_message(message)).await?;
            return stream.close().await;
        }
    }
}

struct TaskGuard<'a> {
    task: TaskMessage,
    backend: &'a Backend,
    live: Cell<bool>,
}

impl <'a> TaskGuard<'a> {
    fn new(task: &TaskMessage, backend: &'a Backend) -> TaskGuard<'a> {
        TaskGuard {
            task: task.clone(),
            backend,
            live: Cell::new(true)
        }
    }

    /// Release the guard without re-scheduling the task.
    fn release(guard: Self) {
        guard.live.set(false);
        drop(guard);
    }
}

impl <'a> Drop for TaskGuard<'a> {
    fn drop(&mut self) {
        if self.live.get() {
            let _ = Handle::current().enter();
            let _ = futures::executor::block_on(self.backend.scheduler.fail(self.task.clone()));
        }
    }
}
