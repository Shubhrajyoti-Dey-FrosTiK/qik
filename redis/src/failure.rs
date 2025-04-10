use crate::client::{RedisClient, Update, UpdateType};
use anyhow::Result;
use redis::{AsyncCommands, Commands, Script};
use serde_json::to_string;
use std::time::{SystemTime, UNIX_EPOCH};

impl RedisClient {
    pub async fn handle_lease_timeout(
        &mut self,
        queue_name: String,
        task_id: String,
    ) -> Result<()> {
        let is_task_present: bool = self
            .redis
            .exists(&Self::get_item_key(task_id.clone()))
            .await
            .unwrap();
        if !is_task_present {
            return Ok(());
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let script = Script::new(
            r"
            local lease_queue_name = ARGV[1]
            local queue_name = ARGV[2]
            local task_id = ARGV[3]
            local now = tonumber(ARGV[4])

            redis.call('ZREM', lease_queue_name, task_id)
            redis.call('ZADD', queue_name, now, task_id)
            ",
        );

        script
            .arg(Self::get_lease_queue_name(task_id.clone()))
            .arg(queue_name.clone())
            .arg(task_id.clone())
            .arg(now.to_string())
            .invoke::<()>(&mut self.client)
            .unwrap();

        self.client
            .publish::<String, String, String>(
                String::from("UPDATE"),
                to_string(&Update {
                    queue_name,
                    task_id,
                    update_type: UpdateType::AddItem,
                    to_be_consumed_at: Some(now),
                })
                .unwrap(),
            )
            .unwrap();

        Ok(())
    }
}
