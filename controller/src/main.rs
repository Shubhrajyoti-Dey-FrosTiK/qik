use rpc::controller::controller_server::ControllerServer;
use tonic::transport::Server;
pub mod rpc;
pub mod services;
use dotenv::dotenv;
use services::controller::ControllerService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok(); // Load the .env
    let addr = "[::1]:50051".parse()?;
    let mut controller = ControllerService::new().await.unwrap();

    // controller
    //     .db
    //     .add_scheduled_task(String::from("queue1"), String::from("task1"), 0, 10000)
    //     .await
    //     .unwrap();

    // controller
    //     .db
    //     .get_task(String::from("queue1"))
    //     .await
    //     .unwrap();

    // controller
    // .db
    // .ack_task(
    //     String::from("queue1"),
    //     String::from("659ca1a9-be95-47b8-97e8-d5b0ebb0ddc1"),
    // )
    // .await
    // .unwrap();

    Server::builder()
        .add_service(ControllerServer::new(controller))
        .serve(addr)
        .await?;

    Ok(())
}
