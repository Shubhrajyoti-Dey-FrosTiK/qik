use controller::rpc::controller::{controller_client::ControllerClient, AddItemRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = ControllerClient::connect("http://[::1]:50051").await?;

    let request = tonic::Request::new(AddItemRequest {
        ..Default::default()
    });

    let mut stream = client.add_item(request).await?.into_inner();

    while let Ok(Some(item_response)) = stream.message().await {
        println!("RESPONSE={:?}", item_response);
    }
    Ok(())
}
