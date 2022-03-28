use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex;
use async_trait::async_trait;
use bson::{doc, Document};
use bson::oid::ObjectId;
use futures_util::TryStreamExt;
use log::info;
use mongodb::Client;
use crate::queue;
use crate::scheduler::message::TaskMessage;
use crate::scheduler::{Scheduler, SchedulerFactory};

/// Standard priority
const STANDARD: u8 = 4;
/// Rescheduled priority
const RESCHEDULED: u8 = 8;

pub struct HeapScheduler {
    to_schedule: queue::PriorityQueue<PrioritizedTask>,
    scheduled: Mutex<HashSet<TaskMessage>>,
    order: AtomicUsize,
    mongo: Option<mongodb::Client>,
}

pub struct HeapSchedulerFactory;

struct PrioritizedTask {
    task: TaskMessage,
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

impl Ord for PrioritizedTask {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.priority.cmp(&other.priority) {
            Ordering::Equal => self.order.cmp(&other.order),
            o => o,
        }
    }
}

impl HeapScheduler {
    fn get_task(&self, task: TaskMessage, priority: u8) -> PrioritizedTask {
        let order = self.order.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        PrioritizedTask {
            task,
            priority,
            order
        }
    }
}

#[async_trait]
impl Scheduler for HeapScheduler {
    fn submit(&self, task: TaskMessage) {
        let task = self.get_task(task, STANDARD);
        self.to_schedule.push(task);
    }

    async fn poll(&self) -> TaskMessage {
        let task = self.to_schedule.poll().await;
        return task.task;
    }

    fn complete(&self, task: TaskMessage) {
        self.scheduled.lock().unwrap().remove(&task);
    }

    fn fail(&self, task: TaskMessage) {
        self.scheduled.lock().unwrap().remove(&task);
        self.to_schedule.push(self.get_task(task, RESCHEDULED));
    }
}

#[async_trait]
impl SchedulerFactory<HeapScheduler> for HeapSchedulerFactory {
    async fn create(&self, mongo: Option<Client>) -> HeapScheduler {
        let scheduler = HeapScheduler {
            to_schedule: queue::PriorityQueue::new(),
            scheduled: Mutex::new(HashSet::new()),
            order: AtomicUsize::new(0),
            mongo: mongo.clone()
        };

        if let Some(client) = mongo {
            let cursor = client
                .database("renderservice")
                .collection::<Document>("tasks")
                .find(doc! { "scheduler": "heap" }, None)
                .await;
            if let Ok(mut cursor) = cursor {
                while let Ok(Some(task)) = cursor.try_next().await {
                    if let Ok(status) = task.clone().get_str("status") {
                        if let Some(task) = document_to_task(task) {
                            match status {
                                "scheduled" => scheduler.to_schedule.push(scheduler.get_task(task, STANDARD)),
                                "queued" => scheduler.to_schedule.push(scheduler.get_task(task, RESCHEDULED)),
                                _ => ()
                            };
                        }
                    }
                }
            }
        }
        info!("Created HeapScheduler with {} tasks.", scheduler.to_schedule.len());
        return scheduler;
    }
}


fn task_to_document(task: &TaskMessage, scheduled: bool) -> Option<Document> {
    let id = ObjectId::parse_str(&task.task_id);
    if id.is_err() {
        return None;
    }
    Some(doc! {
        "_id": id.unwrap(),
        "jobId": task.job_id.clone(),
        "spp": task.spp,
        "status": if scheduled { "scheduled" } else { "queued" },
        "scheduler": "heap"
    })
}

fn document_to_task(document: Document) -> Option<TaskMessage> {
    let id = document.get_object_id("_id");
    let job_id = document.get_str("jobId");
    let spp = document.get_i32("spp");

    if let Ok(id) = id {
        if let Ok(job_id) = job_id {
            if let Ok(spp) = spp {
                if let Ok(spp) = u32::try_from(spp) {
                    return Some(TaskMessage {
                        task_id: id.to_string(),
                        job_id: job_id.to_string(),
                        spp,
                    });
                }
            }
        }
    }
    None
}
