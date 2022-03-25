use crate::scheduler::Task;
use crate::queue::Queue;

#[derive(Clone)]
pub struct Backend {
    pub admin_token: Option<String>,
    pub work_queue: Queue<Task>,
}

impl Backend {
    pub fn new(admin_token: Option<String>) -> Backend {
        Backend {
            admin_token: admin_token.clone(),
            work_queue: Queue::new(),
        }
    }
}
