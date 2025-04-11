
use controller::rpc::controller::{
    controller_client::ControllerClient, JobCompleteResponse, ListenRequest,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = ControllerClient::connect("http://[::1]:50051").await?;

    let request = tonic::Request::new(ListenRequest {
        queue_name: String::from("queue1"),
    });

    let mut stream = client.worker_listen(request).await?.into_inner();

    while let Ok(Some(item_response)) = stream.message().await {
        println!("RESPONSE={:?}", item_response);
        let task_id = item_response.task_id;

        let ack_job_request = tonic::Request::new(JobCompleteResponse {
            queue_name: item_response.queue_name,
            task_id: task_id.clone(),
            result: String::from("Dummy"),
        });

        let ack_job = client.worker_job_complete(ack_job_request).await.unwrap();

        if ack_job.into_inner().success {
            println!("ACKED JOB {}", task_id);
        }
    }
    Ok(())
}
