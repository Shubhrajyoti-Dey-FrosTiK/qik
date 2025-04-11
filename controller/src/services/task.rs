use anyhow::Result;
use prost_types::Struct;
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::Status;

use crate::rpc::controller::JobStreamResponse;

pub struct Task {
    pub queue: String,
    pub task_id: String,
    pub task: String,
}

impl Task {
    pub async fn get_task(
        rx: &mut Receiver<Self>,
        tx: &mut Sender<Result<JobStreamResponse, Status>>,
    ) -> Result<()> {
        while let Some(task) = rx.blocking_recv() {
            tx.send(Ok(JobStreamResponse {
                queue_name: task.queue,
                task_id: task.task_id,
                item: Some(Struct {
                    ..Default::default()
                }),
            }))
            .await
            .unwrap();
        }
        Ok(())
    }
}
