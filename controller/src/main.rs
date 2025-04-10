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

    controller
        .db
        .get_task(String::from("queue1"))
        .await
        .unwrap();

    Server::builder()
        .add_service(ControllerServer::new(controller))
        .serve(addr)
        .await?;

    Ok(())
}
