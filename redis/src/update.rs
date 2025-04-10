use crate::client::{RedisClient, Update};
use anyhow::Result;
use redis::Commands;
use serde_json::to_string;

impl RedisClient {
    pub async fn update(&mut self, update: Update) -> Result<()> {
        self.client
            .publish::<String, String, String>(String::from("UPDATE"), to_string(&update).unwrap())
            .unwrap();

        Ok(())
    }
}
