pub mod message;
pub mod heap_scheduler;

use std::sync::Arc;
use async_trait::async_trait;
use crate::scheduler::message::TaskMessage;
use crate::util::error::Result;

#[async_trait]
pub trait Scheduler {
    /// Submit a new task to this scheduler
    async fn submit(&self, task: TaskMessage) -> Result<()>;

    /// Get a new task from this scheduler
    async fn poll(&self) -> Result<TaskMessage>;

    /// Successfully complete a task
    async fn complete(&self, task: TaskMessage) -> Result<()>;

    /// Fail to complete a task and return it to be scheduled
    async fn fail(&self, task: TaskMessage) -> Result<()>;

    /// Get the approximate number of tasks left
    fn task_count(&self) -> usize;
}

#[async_trait]
pub trait SchedulerFactory<T> where T: Scheduler + Send + Sync {
    /// Create a new scheduler and pull tasks from mongodb
    async fn create(&self, mongo: Option<mongodb::Client>) -> Arc<T>;
}
