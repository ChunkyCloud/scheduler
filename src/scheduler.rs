use crate::message::{Message, TaskMessage};

pub struct Task {
    pub job_id: String,
    pub spp: u32,
}

impl Task {
    pub fn to_message(&self) -> Message {
        Message::Task(TaskMessage {
            job_id: self.job_id.clone(),
            spp: self.spp,
        })
    }

    pub fn from_message(message: TaskMessage) -> Task {
        Task {
            job_id: message.job_id.clone(),
            spp: message.spp,
        }
    }
}
