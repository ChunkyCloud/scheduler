use std::sync::Arc;
use log::error;
use crate::scheduler::Scheduler;

#[derive(Clone)]
pub struct Backend {
    pub admin_token: Option<String>,
    pub mongo_client: Option<mongodb::Client>,
    pub scheduler: Arc<dyn Scheduler + Send + Sync>,
}

impl Backend {
    pub async fn new<T: 'static>(admin_token: Option<String>, mongo_url: Option<String>, scheduler: T) -> Backend where T: Scheduler + Send + Sync {
        let client = match mongo_url {
            None => None,
            Some(url) => match mongodb::Client::with_uri_str(url).await {
                Ok(client) => Some(client),
                Err(e) => {
                    error!("Failed to load mongodb: {:?}", e);
                    None
                }
            },
        };

        Backend {
            admin_token: admin_token.clone(),
            mongo_client: client,
            scheduler: Arc::new(scheduler),
        }
    }
}
