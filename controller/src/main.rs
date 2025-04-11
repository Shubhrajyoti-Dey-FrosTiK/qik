use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use controller::rpc::util::struct_to_json;
use prost_types::{Struct, Value};
use rpc::controller::controller_server::ControllerServer;
use serde_json::to_string;
use tokio::{spawn, time::sleep};
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

    spawn(ControllerService::run_periodic_job_checker(
        cloned_controller.clone(),
    ));

    sleep(Duration::from_secs(2)).await;

    let mut task = Struct::default();
    task.fields
        .insert(String::from("a"), Value::from(String::from("12")));
    controller
        .db
        .add_scheduled_task(
            String::from("queue1"),
            to_string(&struct_to_json(&task)).unwrap(),
            now,
            10000,
        )
        .await
        .unwrap();

    controller
        .db
        .add_periodic_task(
            String::from("PERIODICITY"),
            String::from("something"),
            10000,
            now,
            now + 10000,
            4000,
        )
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
