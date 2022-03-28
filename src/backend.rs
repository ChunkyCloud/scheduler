use std::sync::Arc;
use log::error;
use crate::scheduler::Scheduler;
use crate::scheduler::SchedulerFactory;

#[derive(Clone)]
pub struct Backend {
    pub admin_token: Option<String>,
    pub mongo_client: Option<mongodb::Client>,
    pub scheduler: Arc<dyn Scheduler + Send + Sync>,
}

impl Backend {
    async fn mongo_client(mongo_url: Option<String>) -> Option<mongodb::Client> {
        match mongo_url {
            None => None,
            Some(url) => match mongodb::Client::with_uri_str(url).await {
                Ok(client) => Some(client),
                Err(e) => {
                    error!("Failed to load mongodb: {:?}", e);
                    None
                }
            },
        }
    }

    pub async fn new<T: 'static, F>(admin_token: Option<String>, mongo_url: Option<String>, scheduler: F) -> Backend
        where T: Scheduler + Send + Sync, F: SchedulerFactory<T> {

        let client = Backend::mongo_client(mongo_url).await;
        Backend {
            admin_token: admin_token.clone(),
            mongo_client: client.clone(),
            scheduler: scheduler.create(client).await,
        }
    }
}
