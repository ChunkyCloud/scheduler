pub mod message;
pub mod heap_scheduler;

use async_trait::async_trait;
use crate::scheduler::message::TaskMessage;

#[async_trait]
pub trait Scheduler {
    /// Submit a new task to this scheduler
    fn submit(&self, task: TaskMessage);

    /// Get a new task from this scheduler
    async fn poll(&self) -> TaskMessage;

    /// Successfully complete a task
    fn complete(&self, task: TaskMessage);

    /// Fail to complete a task and return it to be scheduled
    fn fail(&self, task: TaskMessage);
}

