use std::sync::Arc;
use crate::scheduler::Scheduler;

#[derive(Clone)]
pub struct Backend {
    pub admin_token: Option<String>,
    pub scheduler: Arc<dyn Scheduler + Send + Sync>,
}

impl Backend {
    pub fn new<T: 'static>(admin_token: Option<String>, scheduler: T) -> Backend where T: Scheduler + Send + Sync {
        Backend {
            admin_token: admin_token.clone(),
            scheduler: Arc::new(scheduler),
        }
    }
}
