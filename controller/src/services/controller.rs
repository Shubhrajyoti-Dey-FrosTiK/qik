use crate::rpc::controller::{controller_server::Controller, AddItemRequest, AddItemResponse};
use anyhow::Result;
use redis::client::RedisClient;
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{Request, Response, Status};

#[derive(Debug, Clone)]
pub struct ControllerService {
    pub db: RedisClient,
}

impl ControllerService {
    pub async fn new() -> Result<Self> {
        Ok(Self {
            db: RedisClient::new().await.unwrap(),
        })
    }
}

type StreamResult<T> = Result<Response<T>, Status>;

#[tonic::async_trait]
impl Controller for ControllerService {
    type AddItemStream = Pin<Box<dyn Stream<Item = Result<AddItemResponse, Status>> + Send>>;

    async fn add_item(
        &self,
        request: Request<AddItemRequest>,
    ) -> StreamResult<Self::AddItemStream> {
        println!("Got a request: {:?}", request);

        let item = request.into_inner().get_item_string().unwrap();

        let (tx, rx) = mpsc::channel(128);
        tx.send(Ok(AddItemResponse { success: true }))
            .await
            .unwrap();

        let output_stream = ReceiverStream::new(rx);

        Ok(Response::new(Box::pin(output_stream) as Self::AddItemStream))
    }
}
