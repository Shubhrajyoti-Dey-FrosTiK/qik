use super::controller::ControllerService;
use anyhow::Result;
use std::{
    sync::Arc,
    time::Duration,
};
use tokio::time::sleep;

impl ControllerService {
    pub async fn run_periodic_job_checker(server: Arc<ControllerService>) {
        loop {
            server
                .mutexed_db
                .lock()
                .await
                .schedule_periodic_tasks(String::from("PERIODICITY"))
                .await
                .unwrap();
            sleep(Duration::from_secs(5)).await;
            println!("Running periodic job checker");
        }
    }
}
