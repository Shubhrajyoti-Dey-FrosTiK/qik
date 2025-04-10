use super::controller::ControllerService;
use anyhow::Result;
use redis::client::{RedisClient, Update};
use serde_json::from_str;

impl ControllerService {
    pub async fn run_background_triggers<'a>(&'a self) -> Result<Self> {
        let mut conn = RedisClient::get_connection().await.unwrap();
        let mut pubsub = conn.as_pubsub();
        pubsub.subscribe("UPDATE").unwrap();
        loop {
            let msg = pubsub.get_message()?;
            let payload: String = msg.get_payload()?;
            let update = from_str::<Update>(&payload).unwrap();
            println!("{:#?}", update);
        }
    }
}
