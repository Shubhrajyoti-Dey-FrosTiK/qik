use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use redis::client::Update;
use rpc::controller::controller_server::ControllerServer;
use tokio::{
    spawn,
    sync::{mpsc::channel, Mutex},
};
use tonic::transport::Server;
pub mod rpc;
pub mod services;
use dotenv::dotenv;
use services::controller::ControllerService;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok(); // Load the .env
    let addr = "[::1]:50051".parse()?;
    let mut controller = ControllerService::new().await.unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    // let rx = Arc::new(Mutex::new(rx));
    let cloned_controller = Arc::new(controller.clone());

    spawn(ControllerService::run_background_triggers(
        cloned_controller.clone(),
    ));

    controller
        .db
        .add_scheduled_task(String::from("queue1"), String::from("task1"), now, 10000)
        .await
        .unwrap();

    // let task_id = controller
    //     .db
    //     .get_tasks(String::from("queue1"))
    //     .await
    //     .unwrap();

    // controller
    //     .db
    //     .ack_task(String::from("queue1"), String::from(task_id.unwrap()))
    //     .await
    //     .unwrap();

    Server::builder()
        .add_service(ControllerServer::new(controller))
        .serve(addr)
        .await?;

    Ok(())
}
