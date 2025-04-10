use anyhow::Result;
use prost_types::Struct;
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::Status;

use crate::rpc::controller::ListenResponse;

pub struct Task {
    pub task_id: String,
    pub task: String,
}

impl Task {
    pub async fn get_task(
        rx: &mut Receiver<Self>,
        tx: &mut Sender<Result<ListenResponse, Status>>,
    ) -> Result<()> {
        for task in rx.blocking_recv() {
            tx.send(Ok(ListenResponse {
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
