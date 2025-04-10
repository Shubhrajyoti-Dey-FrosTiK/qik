use crate::rpc::controller::{
    controller_server::Controller, AddJobRequest, JobCompleteResponse, JobStreamResponse,
    ListenRequest, SuccessResponse,
};
use anyhow::Result;
use dashmap::DashMap;
use redis::client::RedisClient;
use std::{pin::Pin, sync::Arc};
use tokio::{
    spawn,
    sync::{
        mpsc::{channel, Sender},
        Mutex,
    },
};
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct ControllerService {
    pub db: RedisClient,
    pub mutexed_db: Arc<Mutex<RedisClient>>,
    pub num_of_workers: Arc<Mutex<i32>>,
    pub subscribers: Arc<DashMap<String, Vec<Sender<Result<JobStreamResponse, Status>>>>>,
}

impl ControllerService {
    pub async fn new() -> Result<Self> {
        Ok(Self {
            db: RedisClient::new().await.unwrap(),
            mutexed_db: Arc::new(Mutex::new(RedisClient::new().await.unwrap())),
            num_of_workers: Arc::new(Mutex::new(0)),
            subscribers: Arc::new(DashMap::new()),
        })
    }
}

type StreamResult<T> = Result<Response<T>, Status>;

#[tonic::async_trait]
impl Controller for ControllerService {
    type ClientAddJobStream = Pin<Box<dyn Stream<Item = Result<SuccessResponse, Status>> + Send>>;
    type WorkerListenStream = Pin<Box<dyn Stream<Item = Result<JobStreamResponse, Status>> + Send>>;

    async fn client_add_job(
        &self,
        request: Request<AddJobRequest>,
    ) -> StreamResult<Self::ClientAddJobStream> {
        println!("Got a request: {:?}", request);

        let request = request.into_inner();

        self.mutexed_db
            .lock()
            .await
            .add_scheduled_task(
                request.queue_name.clone(),
                request.get_item_string().unwrap(),
                request.start_time as u128,
                request.lease_time as u128,
            )
            .await
            .unwrap();

        let (tx, rx) = channel(128);
        tx.send(Ok(SuccessResponse { success: true }))
            .await
            .unwrap();

        let output_stream = ReceiverStream::new(rx);

        Ok(Response::new(
            Box::pin(output_stream) as Self::ClientAddJobStream
        ))
    }

    async fn worker_listen(
        &self,
        request: Request<ListenRequest>,
    ) -> StreamResult<Self::WorkerListenStream> {
        let (tx, rx) = channel(128);
        let queue_name = request.into_inner().queue_name;
        if self.subscribers.contains_key(&queue_name) {
            let mut subscribers = self.subscribers.get_mut(&queue_name).unwrap();
            subscribers.push(tx);
            println!("{:#?}", self.subscribers);
        } else {
            self.subscribers.insert(queue_name, vec![tx]);
        }

        spawn(async { loop {} }); // Keep the client connected

        let output_stream = ReceiverStream::new(rx);

        Ok(Response::new(
            Box::pin(output_stream) as Self::WorkerListenStream
        ))
    }

    async fn worker_job_complete(
        &self,
        request: Request<JobCompleteResponse>,
    ) -> Result<Response<SuccessResponse>, Status> {
        let reply = SuccessResponse { success: true };

        Ok(Response::new(reply))
    }
}
