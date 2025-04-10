use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::rpc::controller::ListenResponse;

use super::controller::ControllerService;
use anyhow::Result;
use redis::client::{RedisClient, Update};
use serde_json::from_str;
use tokio::{
    spawn,
    sync::{mpsc::Sender, Mutex},
    time::sleep,
};
use tokio_stream::StreamExt;

impl ControllerService {
    pub async fn run_background_triggers(server: Arc<ControllerService>) -> Result<()> {
        let mut pubsub = RedisClient::get_pubsub().await.unwrap();
        let controller = Arc::new(Mutex::new(Self::new().await.unwrap()));
        pubsub.subscribe("UPDATE").await.unwrap();

        while let Some(msg) = pubsub.on_message().next().await {
            let payload: String = msg.get_payload()?;
            let update = from_str::<Update>(&payload).unwrap();
            println!("{:#?}", update.clone());

            let server = server.clone();

            match update.update_type {
                redis::client::UpdateType::AddItem => {
                    spawn(ControllerService::handle_add_item(
                        server.clone(),
                        controller.clone(),
                        update.clone(),
                    ));
                }

                redis::client::UpdateType::ItemAcked => {}
                redis::client::UpdateType::ItemLeased => {}
            }
        }

        Ok(())
    }

    pub async fn handle_add_item(
        server: Arc<ControllerService>,
        controller: Arc<Mutex<ControllerService>>,
        update: Update,
        // subscribers: Vec<Sender<Result<ListenResponse, Status>>>,
        // server: Self,
    ) -> Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        sleep(if update.to_be_consumed_at.unwrap() >= now {
            Duration::from_millis((update.to_be_consumed_at.unwrap() - now) as u64)
        } else {
            Duration::from_millis(0)
        })
        .await;

        println!("HELL");

        let subscriber = server.subscribers.get(&update.queue_name.clone());
        if subscriber.is_none() {
            println!("NO WORKERS CONNECTED");
            return Ok(());
        }
        let subscribers = subscriber.unwrap().value().clone();

        let tasks = controller
            .lock()
            .await
            .db
            .get_tasks(update.queue_name, 2 as i32)
            .await
            .unwrap();

        println!("RECEIVED {} TASKS", tasks.len());

        for task_idx in 0..tasks.len() {
            subscribers
                .get(task_idx)
                .unwrap()
                .send(Ok(ListenResponse {
                    task_id: update.task_id.clone(),
                    item: None,
                }))
                .await
                .unwrap();
        }

        Ok(())
    }
}
