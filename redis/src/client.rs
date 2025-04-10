use anyhow::{Ok, Result};
use redis::{aio::MultiplexedConnection, AsyncCommands, Client, Connection, Script};
use serde::{Deserialize, Serialize};
use serde_json::to_string;
use std::{
    env,
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
pub enum UpdateType {
    AddItem,
    ItemLeased,
    ItemAcked,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct Update {
    queue_name: String,
    task_id: String,
    update_type: UpdateType,
}

#[derive(Clone)]
pub struct RedisClient {
    pub client: Client,
    pub redis: MultiplexedConnection,
}

impl RedisClient {
    pub async fn new() -> Result<Self> {
        let connection_uri = format!(
            "redis://{}:{}",
            env::var("REDIS_HOST").unwrap(),
            env::var("REDIS_PORT").unwrap()
        );
        let client = Client::open(connection_uri)?;
        let redis: MultiplexedConnection =
            client.clone().get_multiplexed_async_connection().await?;

        Ok(Self { redis, client })
    }

    pub async fn get_connection() -> Result<Connection> {
        let connection_uri = format!(
            "redis://{}:{}",
            env::var("REDIS_HOST").unwrap(),
            env::var("REDIS_PORT").unwrap()
        );
        let client = Client::open(connection_uri)?;
        Ok(client.get_connection().unwrap())
    }

    pub fn get_lease_time_key(item_id: String) -> String {
        format!("LEASE_TIME:{}", item_id)
    }

    pub fn get_item_key(item_id: String) -> String {
        format!("ITEM:{}", item_id)
    }

    pub fn get_lease_queue_name(queue_name: String) -> String {
        format!("LEASE_SETS:{}", queue_name)
    }

    pub async fn add_scheduled_task(
        &mut self,
        queue_name: String,
        task: String,
        time: i32,
        lease_time: i32,
    ) -> Result<()> {
        let task_id = Uuid::new_v4().to_string();
        self.redis
            .set::<String, String, String>(Self::get_item_key(task_id.clone()), task.clone())
            .await
            .unwrap();
        self.redis
            .set::<String, String, String>(
                Self::get_lease_time_key(task_id.clone()),
                lease_time.to_string(),
            )
            .await
            .unwrap();
        self.redis
            .zadd::<String, i32, String, String>(queue_name.clone(), task_id.clone(), time)
            .await
            .unwrap();

        self.redis
            .publish::<String, String, String>(
                String::from("UPDATE"),
                to_string(&Update {
                    queue_name,
                    task_id,
                    update_type: UpdateType::AddItem,
                })
                .unwrap(),
            )
            .await
            .unwrap();

        Ok(())
    }

    pub async fn get_task(&mut self, queue_name: String) -> Result<Option<String>> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let lease_queue_name = Self::get_lease_queue_name(queue_name.clone());
        let script = Script::new(
            r"
            local queue_name = ARGV[1]
            local lease_queue_name = ARGV[2]
            local now = tonumber(ARGV[3])
            local tasks = redis.call('ZRANGEBYSCORE', queue_name, '-inf', now, 'LIMIT', 0,  1)

            for _, task in ipairs(tasks) do
                local lease_time_key = 'LEASE_TIME:' .. task
                local lease_time = tonumber(redis.call('GET', lease_time_key))
                redis.call('ZREM', queue_name, task)
                redis.call('ZADD', lease_queue_name, now + lease_time, task)
            end

            return tasks
            ",
        );

        let result = script
            .arg(queue_name)
            .arg(lease_queue_name)
            .arg(now.to_string())
            .arg(now.to_string())
            .invoke::<Vec<String>>(&mut self.client)
            .unwrap();

        let result = result.get(0);

        if result.is_none() {
            return Ok(None);
        } else {
            return Ok(Some(result.unwrap().clone()));
        }
    }

    pub async fn ack_task(&mut self, queue_name: String, task_id: String) -> Result<bool> {
        let is_task_present: bool = self
            .redis
            .exists(&Self::get_item_key(task_id.clone()))
            .await
            .unwrap();
        if !is_task_present {
            return Ok(false);
        }

        let script = Script::new(
            r"
            local lease_time_key = ARGV[1]
            local lease_sets_key = ARGV[2]
            local item_key = ARGV[3]
            local task_id = ARGV[4]

            redis.call('DEL', lease_time_key)
            redis.call('DEL', item_key)
            redis.call('ZREM', lease_sets_key, task_id)

            return true
            ",
        );

        let acked = script
            .arg(Self::get_lease_time_key(task_id.clone()))
            .arg(Self::get_lease_queue_name(queue_name.clone()))
            .arg(Self::get_item_key(task_id.clone()))
            .arg(task_id.clone())
            .invoke::<bool>(&mut self.client)
            .unwrap();

        println!("{:#?}", acked);

        self.redis
            .publish::<String, String, String>(
                String::from("UPDATE"),
                to_string(&Update {
                    queue_name,
                    task_id,
                    update_type: UpdateType::ItemAcked,
                })
                .unwrap(),
            )
            .await
            .unwrap();

        Ok(acked)
    }
}
