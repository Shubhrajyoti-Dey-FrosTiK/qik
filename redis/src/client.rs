use anyhow::{Ok, Result};
use redis::{
    aio::{MultiplexedConnection, PubSub},
    AsyncCommands, Client, Commands, Script,
};
use serde::{Deserialize, Serialize};
use serde_json::to_string;
use std::{
    env,
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub enum UpdateType {
    #[default]
    AddJob,
    ItemLeased,
    ItemAcked,
}
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Update {
    pub queue_name: String,
    pub task_id: String,
    pub update_type: UpdateType,
    pub to_be_consumed_at: Option<u128>,
}

#[derive(Clone)]
pub struct RedisClient {
    pub client: Client,
    pub redis: MultiplexedConnection,
}

impl RedisClient {
    pub async fn new() -> Result<Self> {
        let connection_uri = format!(
            "redis://{}:{}?protocol=resp3",
            env::var("REDIS_HOST").unwrap(),
            env::var("REDIS_PORT").unwrap()
        );
        let client = Client::open(connection_uri)?;
        let redis: MultiplexedConnection =
            client.clone().get_multiplexed_async_connection().await?;

        Ok(Self { redis, client })
    }

    pub async fn get_pubsub() -> Result<PubSub> {
        let connection_uri = format!(
            "redis://{}:{}",
            env::var("REDIS_HOST").unwrap(),
            env::var("REDIS_PORT").unwrap()
        );
        // let client = Client::open(connection_uri)?;
        let client = Client::open(connection_uri)?;
        let pubsub = client.get_async_pubsub().await.unwrap();

        Ok(pubsub)
    }

    pub fn get_lease_queue_name(queue_name: String) -> String {
        format!("LEASE_SETS:{}", queue_name)
    }

    pub fn get_item_prefix() -> String {
        String::from("ITEM")
    }

    pub fn get_lease_time_prefix() -> String {
        String::from("LEASE_TIME")
    }

    pub fn get_insert_queue_name(queue_name: String) -> String {
        format!("INSERT_QUEUE:{}", queue_name)
    }

    pub async fn get_task_by_id(&mut self, task_id: String) -> Result<String> {
        let task: String = self
            .redis
            .hget(&Self::get_item_prefix(), task_id.clone())
            .await
            .unwrap();
        Ok(task)
    }

    pub async fn get_lease_time_by_id(&mut self, task_id: String) -> Result<u128> {
        let lease_time: u128 = self
            .redis
            .hget(&Self::get_lease_time_prefix(), task_id.clone())
            .await
            .unwrap();
        Ok(lease_time)
    }

    pub async fn add_scheduled_task(
        &mut self,
        queue_name: String,
        task: String,
        time: u128,
        lease_time: u128,
    ) -> Result<()> {
        let task_id = Uuid::new_v4().to_string();
        self.redis
            .hset::<String, String, String, String>(
                Self::get_item_prefix(),
                task_id.clone(),
                task.clone(),
            )
            .await
            .unwrap();
        self.redis
            .hset::<String, String, String, String>(
                Self::get_lease_time_prefix(),
                task_id.clone(),
                lease_time.to_string(),
            )
            .await
            .unwrap();
        self.redis
            .zadd::<String, f64, String, String>(
                Self::get_insert_queue_name(queue_name.clone()),
                task_id.clone(),
                time as f64,
            )
            .await
            .unwrap();

        self.client
            .publish::<String, String, String>(
                String::from("UPDATE"),
                to_string(&Update {
                    queue_name,
                    task_id,
                    update_type: UpdateType::AddJob,
                    to_be_consumed_at: Some(time + lease_time),
                })
                .unwrap(),
            )
            .unwrap();

        Ok(())
    }

    pub async fn get_tasks(&mut self, queue_name: String, no_of_tasks: i32) -> Result<Vec<String>> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let lease_queue_name = Self::get_lease_queue_name(queue_name.clone());
        let script = Script::new(
            r"
            local queue_name = ARGV[1]
            local lease_queue_name = ARGV[2]
            local lease_time_prefix = ARGV[3]
            local now = tonumber(ARGV[4])
            local no_of_tasks = tonumber(ARGV[5])
            local tasks = redis.call('ZRANGEBYSCORE', queue_name, '-inf', now, 'LIMIT', 0, no_of_tasks)

            for _, task in ipairs(tasks) do
                local lease_time = tonumber(redis.call('HGET', lease_time_prefix, task))
                redis.call('ZREM', queue_name, task)
                redis.call('ZADD', lease_queue_name, now + lease_time, task)
            end

            return tasks
            ",
        );

        let task_ids = script
            .arg(Self::get_insert_queue_name(queue_name.clone()))
            .arg(lease_queue_name)
            .arg(Self::get_lease_time_prefix())
            .arg(now.to_string())
            .arg(no_of_tasks.to_string())
            .invoke::<Vec<String>>(&mut self.client)
            .unwrap();

        for task_id in task_ids.clone() {
            let lease_time: u128 = self
                .redis
                .hget(Self::get_lease_time_prefix(), task_id.clone())
                .await
                .unwrap();

            self.client
                .publish::<String, String, String>(
                    String::from("UPDATE"),
                    to_string(&Update {
                        queue_name: queue_name.clone(),
                        task_id: task_id.clone(),
                        update_type: UpdateType::ItemLeased,
                        to_be_consumed_at: Some(lease_time + now),
                    })
                    .unwrap(),
                )
                .unwrap();
        }

        return Ok(task_ids);
    }

    pub async fn ack_task(&mut self, queue_name: String, task_id: String) -> Result<bool> {
        let is_task_present: bool = self
            .redis
            .hexists(&Self::get_item_prefix(), task_id.clone())
            .await
            .unwrap();
        if !is_task_present {
            return Ok(false);
        }

        let script = Script::new(
            r"
            local lease_time_prefix = ARGV[1]
            local lease_queue_name = ARGV[2]
            local item_prefix = ARGV[3]
            local task_id = ARGV[4]

            redis.call('HDEL', lease_time_prefix, task_id)
            redis.call('HDEL', item_prefix, task_id)
            redis.call('ZREM', lease_queue_name, task_id)

            return true
            ",
        );

        let acked = script
            .arg(Self::get_lease_time_prefix())
            .arg(Self::get_lease_queue_name(queue_name.clone()))
            .arg(Self::get_item_prefix())
            .arg(task_id.clone())
            .invoke::<bool>(&mut self.client)
            .unwrap();

        println!("{:#?}", acked);

        // self.redis.subscribe("UPDATE").await.unwrap();
        self.client
            .publish::<String, String, String>(
                String::from("UPDATE"),
                to_string(&Update {
                    queue_name,
                    task_id,
                    update_type: UpdateType::ItemAcked,
                    to_be_consumed_at: None,
                })
                .unwrap(),
            )
            .unwrap();

        Ok(acked)
    }
}
