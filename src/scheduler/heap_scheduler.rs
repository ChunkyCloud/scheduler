use std::cmp::Ordering;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::atomic::AtomicUsize;
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
use crate::queue;
use crate::scheduler::message::TaskMessage;
use crate::scheduler::{Scheduler, SchedulerFactory};
use crate::scheduler::heap_scheduler::TaskStatus::Completed;
use crate::util::error::Result;
use crate::util::error::Error;

/// Standard priority
const STANDARD: u8 = 4;
/// Rescheduled priority
const RESCHEDULED: u8 = 8;

pub struct HeapScheduler {
    to_schedule: queue::PriorityQueue<PrioritizedTask>,
    scheduled: Mutex<HashSet<Task>>,
    order: AtomicUsize,
    mongo_update: mpsc::Sender<Task>,
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

struct PrioritizedTask {
    task: Task,
    priority: u8,
    order: usize,
}

impl Eq for PrioritizedTask {}

impl PartialEq<Self> for PrioritizedTask {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
    }
}

impl PartialOrd<Self> for PrioritizedTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
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

impl Ord for PrioritizedTask {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.priority.cmp(&other.priority) {
            Ordering::Equal => self.order.cmp(&other.order),
            o => o,
        }
    }
}

impl HeapScheduler {
    fn get_task(&self, task: Task, priority: u8) -> PrioritizedTask {
        let order = self.order.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        PrioritizedTask {
            task,
            priority,
            order
        }
    }

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
}

#[async_trait]
impl Scheduler for HeapScheduler {
    async fn submit(&self, task: TaskMessage) {
        let task = Task::new(task);
        {
            let task = self.get_task(task.clone(), STANDARD);
            self.to_schedule.push(task);
        }
        self.mongo_update.send(task).await.unwrap();
    }

    async fn poll(&self) -> TaskMessage {
        let task = self.to_schedule.poll().await;
        let mut task = task.task;
        task.status = TaskStatus::Scheduled;
        let out = task.to_message();
        self.mongo_update.send(task).await.unwrap();
        return out;
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
                status: TaskStatus::Queued,
                scheduler: "heap".to_string(),
            };
            self.scheduled.lock().unwrap().remove(&task);
            self.to_schedule.push(self.get_task(task.clone(), RESCHEDULED));
            self.mongo_update.send(task).await.unwrap();
            Ok(())
        } else {
            Err(Error::Generic(format!("Attempted to complete unknown task: {:?}", task)))
        }
    }
}

#[async_trait]
impl SchedulerFactory<HeapScheduler> for HeapSchedulerFactory {
    async fn create(&self, mongo: Option<Client>) -> Arc<HeapScheduler> {
        let (send, recv) = mpsc::channel(64);

        tokio::spawn(HeapScheduler::mongo_sync(mongo.clone(), recv));

        let scheduler = Arc::new(HeapScheduler {
            to_schedule: queue::PriorityQueue::new(),
            scheduled: Mutex::new(HashSet::new()),
            order: AtomicUsize::new(0),
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
                        TaskStatus::Queued => scheduler.to_schedule.push(scheduler.get_task(task, STANDARD)),
                        TaskStatus::Scheduled => {
                            let mut task = task.clone();
                            task.status = TaskStatus::Queued;
                            scheduler.mongo_update.send(task.clone()).await.unwrap();
                            scheduler.to_schedule.push(scheduler.get_task(task, RESCHEDULED));
                        },
                        _ => {},
                    }
                }
            } else {
                warn!("Failed to load tasks from MongoDB.");
            }
        }
        info!("Created HeapScheduler with {} tasks.", scheduler.to_schedule.len());
        return scheduler;
    }
}
