use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex;
use async_trait::async_trait;
use crate::queue;
use crate::scheduler::message::TaskMessage;
use crate::scheduler::Scheduler;

/// Standard priority
const STANDARD: u8 = 4;
/// Rescheduled priority
const RESCHEDULED: u8 = 8;

pub struct HeapScheduler {
    to_schedule: queue::PriorityQueue<PrioritizedTask>,
    scheduled: Mutex<HashSet<TaskMessage>>,
    order: AtomicUsize,
}

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
    pub fn new() -> HeapScheduler {
        HeapScheduler {
            to_schedule: queue::PriorityQueue::new(),
            scheduled: Mutex::new(HashSet::new()),
            order: AtomicUsize::new(0),
        }
    }

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
