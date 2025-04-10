use crate::rpc::controller::{
    controller_server::Controller, AddItemRequest, AddItemResponse, ListenRequest, ListenResponse,
};
use anyhow::Result;
use dashmap::DashMap;
use redis::client::RedisClient;
use std::{pin::Pin, sync::Arc, time::Duration};
use tokio::{
    spawn,
    sync::{
        mpsc::{self, Receiver, Sender},
        Mutex,
    },
};
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct ControllerService {
    pub db: RedisClient,
    pub num_of_workers: Arc<Mutex<i32>>,
    pub subscribers: Arc<DashMap<String, Vec<Sender<Result<ListenResponse, Status>>>>>,
}

impl ControllerService {
    pub async fn new() -> Result<Self> {
        Ok(Self {
            db: RedisClient::new().await.unwrap(),
            num_of_workers: Arc::new(Mutex::new(0)),
            subscribers: Arc::new(DashMap::new()),
        })
    }
}

type StreamResult<T> = Result<Response<T>, Status>;

#[tonic::async_trait]
impl Controller for ControllerService {
    type AddItemStream = Pin<Box<dyn Stream<Item = Result<AddItemResponse, Status>> + Send>>;
    type ListenStream = Pin<Box<dyn Stream<Item = Result<ListenResponse, Status>> + Send>>;

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

    async fn listen(&self, request: Request<ListenRequest>) -> StreamResult<Self::ListenStream> {
        let (tx, rx) = mpsc::channel(128);
        let queue_name = request.into_inner().queue_name;
        if self.subscribers.contains_key(&queue_name) {
            let mut subscribers = self.subscribers.get_mut(&queue_name).unwrap();
            subscribers.push(tx);
        } else {
            self.subscribers.insert(queue_name, vec![tx]);
        }
        spawn(async { loop {} }); // Keep the client connected

        let output_stream = ReceiverStream::new(rx);

        Ok(Response::new(Box::pin(output_stream) as Self::ListenStream))
    }
}
