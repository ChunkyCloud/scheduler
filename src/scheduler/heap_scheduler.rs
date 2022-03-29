use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use bson::doc;
use bson::oid::ObjectId;
use futures_util::TryStreamExt;
use log::{info, trace, warn};
use mongodb::Client;
use mongodb::options::ReplaceOptions;
use tokio::sync::mpsc;
use serde::{Serialize, Deserialize};
use tokio::select;
use crate::queue::{Queue, QueueError};
use crate::scheduler::message::TaskMessage;
use crate::scheduler::{Scheduler, SchedulerFactory};
use crate::scheduler::heap_scheduler::TaskPriority::Standard;
use crate::scheduler::heap_scheduler::TaskStatus::Completed;
use crate::util::error::Result;
use crate::util::error::Error;

pub struct HeapScheduler {
    to_scheduled: Queue<Task>,
    re_scheduled: Queue<Task>,
    scheduled: Mutex<HashSet<Task>>,
    mongo_update: mpsc::Sender<Task>,
}

enum TaskPriority {
    Standard,
    Rescheduled,
}

pub struct HeapSchedulerFactory;

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
enum TaskStatus {
    #[serde(rename = "queued")]
    Queued,
    #[serde(rename = "scheduled")]
    Scheduled,
    #[serde(skip)]
    Completed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Task {
    #[serde(rename = "_id")]
    task_id: ObjectId,
    #[serde(rename = "jobId")]
    job_id: String,
    spp: u32,
    status: TaskStatus,
    scheduler: String
}

impl Hash for Task {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.task_id.hash(state);
    }
}

impl PartialEq<Self> for Task {
    fn eq(&self, other: &Self) -> bool {
        self.task_id.eq(&other.task_id)
    }
}

impl Eq for Task { }

impl TaskStatus {
    pub fn is_complete(&self) -> bool {
        matches!(self, Completed)
    }
}

impl Task {
    pub fn new(task: TaskMessage) -> Task {
        Task {
            task_id: ObjectId::new(),
            job_id: task.job_id,
            spp: task.spp,
            status: TaskStatus::Queued,
            scheduler: "heap".to_string(),
        }
    }

    pub fn to_message(&self) -> TaskMessage {
        TaskMessage {
            task_id: Some(self.task_id),
            job_id: self.job_id.clone(),
            spp: self.spp,
        }
    }
}

impl HeapScheduler {
    async fn mongo_sync(mongo: Option<Client>, mut recv: mpsc::Receiver<Task>) {
        if let Some(client) = mongo {
            let collection = client
                .database("renderservice")
                .collection::<Task>("tasks");
            while let Some(task) = recv.recv().await {
                if task.status.is_complete() {
                    if let Err(e) = collection.delete_one(doc! { "_id": task.task_id }, None).await {
                        warn!("Failed to remove completed task from MongoDB: {:?}", e);
                    }
                } else if let Err(e) = collection.replace_one(doc! { "_id": task.task_id }, task,
                                                              ReplaceOptions::builder().upsert(true).build()).await {
                    warn!("Failed to update task in MongoDB: {:?}", e);
                }
            }
        } else {
            while let Some(task) = recv.recv().await {
                trace!(target: "heap scheduler", "Task update: {:?}", task);
            }
        }
    }

    async fn schedule_task(&self, task: Task, priority: TaskPriority) -> Result<()> {
        match priority {
            TaskPriority::Standard => self.to_scheduled.send(task).await,
            TaskPriority::Rescheduled => self.re_scheduled.send(task).await,
        }
    }
}

#[async_trait]
impl Scheduler for HeapScheduler {
    async fn submit(&self, task: TaskMessage) -> Result<()> {
        let task = Task::new(task);
        self.schedule_task(task.clone(), Standard).await?;
        self.mongo_update.send(task).await.unwrap();
        Ok(())
    }

    async fn poll(&self) -> Result<TaskMessage> {
        let mut task: Task = select! {
            biased;  // Make sure re-scheduled tasks are scheduled first
            Ok(t) = self.re_scheduled.poll() => t,
            Ok(t) = self.to_scheduled.poll() => t,
            else => return Err(Error::from(QueueError::Closed)),
        };
        task.status = TaskStatus::Scheduled;
        let out = task.to_message();
        self.scheduled.lock().unwrap().insert(task.clone());
        self.mongo_update.send(task).await.unwrap();
        return Ok(out);
    }

    async fn complete(&self, task: TaskMessage) -> Result<()> {
        if let Some(id) = task.task_id {
            let task = Task {
                task_id: id,
                job_id: task.job_id,
                spp: task.spp,
                status: TaskStatus::Completed,
                scheduler: "heap".to_string(),
            };
            self.scheduled.lock().unwrap().remove(&task);
            self.mongo_update.send(task).await.unwrap();
            Ok(())
        } else {
            Err(Error::Generic(format!("Attempted to complete unknown task: {:?}", task)))
        }
    }

    async fn fail(&self, task: TaskMessage) -> Result<()> {
        if let Some(id) = task.task_id {
            let task = Task {
                task_id: id,
                job_id: task.job_id,
                spp: task.spp,
                status: TaskStatus::Scheduled,
                scheduler: "heap".to_string(),
            };
            self.scheduled.lock().unwrap().remove(&task);
            self.schedule_task(task.clone(), TaskPriority::Rescheduled).await?;
            self.mongo_update.send(task).await.unwrap();
            Ok(())
        } else {
            Err(Error::Generic(format!("Attempted to complete unknown task: {:?}", task)))
        }
    }

    fn task_count(&self) -> usize {
        self.to_scheduled.len() + self.re_scheduled.len()
    }
}

#[async_trait]
impl SchedulerFactory<HeapScheduler> for HeapSchedulerFactory {
    async fn create(&self, mongo: Option<Client>) -> Arc<HeapScheduler> {
        let (send, recv) = mpsc::channel(64);

        tokio::spawn(HeapScheduler::mongo_sync(mongo.clone(), recv));

        let scheduler = Arc::new(HeapScheduler {
            re_scheduled: Queue::unbounded(),
            to_scheduled: Queue::unbounded(),
            scheduled: Mutex::new(HashSet::new()),
            mongo_update: send,
        });

        if let Some(client) = mongo {
            let cursor = client
                .database("renderservice")
                .collection::<Task>("tasks")
                .find(doc! { "scheduler": "heap" }, None)
                .await;
            if let Ok(mut cursor) = cursor {
                while let Ok(Some(task)) = cursor.try_next().await {
                    match task.status {
                        TaskStatus::Queued => { scheduler.schedule_task(task, TaskPriority::Standard).await.unwrap(); }
                        TaskStatus::Scheduled => { scheduler.schedule_task(task, TaskPriority::Rescheduled).await.unwrap(); }
                        _ => {},
                    }
                }
            } else {
                warn!("Failed to load tasks from MongoDB.");
            }
        }
        info!("Created HeapScheduler with {} tasks ({} were queued, {} were scheduled).",
            scheduler.task_count(), scheduler.to_scheduled.len(), scheduler.re_scheduled.len());
        return scheduler;
    }
}
