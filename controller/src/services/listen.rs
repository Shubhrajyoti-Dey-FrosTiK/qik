use super::controller::ControllerService;
use crate::rpc::{controller::JobStreamResponse, util::string_to_struct};
use anyhow::Result;
use redis::client::{RedisClient, Update, UpdateType};
use serde_json::from_str;
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{spawn, sync::Mutex, time::sleep};
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
                redis::client::UpdateType::AddJob => {
                    spawn(ControllerService::handle_add_job(
                        server.clone(),
                        controller.clone(),
                        update.clone(),
                    ));
                }

                redis::client::UpdateType::ItemAcked => {}
                redis::client::UpdateType::ItemLeased => {
                    spawn(ControllerService::handle_lease_item(
                        controller.clone(),
                        update.clone(),
                    ));
                }
            }
        }

        Ok(())
    }

    pub async fn handle_add_job(
        server: Arc<ControllerService>,
        controller: Arc<Mutex<ControllerService>>,
        update: Update,
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
            .get_tasks(update.queue_name.clone(), subscribers.len() as i32)
            .await
            .unwrap();

        println!("RECEIVED {} TASKS", tasks.len());

        for task_idx in 0..tasks.len() {
            let task_id = tasks.get(task_idx).unwrap().clone();
            let lease_time = controller
                .lock()
                .await
                .db
                .get_lease_time_by_id(task_id.clone())
                .await
                .unwrap();

            let task = controller
                .lock()
                .await
                .db
                .get_task_by_id(task_id.clone())
                .await
                .unwrap();
            println!("{}", task.clone());
            let task = string_to_struct(&task).unwrap();

            subscribers
                .get(task_idx)
                .unwrap()
                .send(Ok(JobStreamResponse {
                    task_id: update.task_id.clone(),
                    item: Some(task),
                }))
                .await
                .unwrap();

            controller
                .lock()
                .await
                .db
                .update(Update {
                    queue_name: update.queue_name.clone(),
                    task_id: update.task_id.clone(),
                    to_be_consumed_at: Some(update.to_be_consumed_at.unwrap() + lease_time),
                    update_type: UpdateType::ItemLeased,
                })
                .await
                .unwrap();
        }

        Ok(())
    }

    pub async fn handle_lease_item(
        controller: Arc<Mutex<ControllerService>>,
        update: Update,
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

        controller
            .lock()
            .await
            .db
            .handle_lease_timeout(update.queue_name, update.task_id)
            .await
            .unwrap();

        Ok(())
    }
}
