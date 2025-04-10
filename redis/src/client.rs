use anyhow::Result;
use redis::{aio::MultiplexedConnection, AsyncCommands, Client, Script};
use std::{
    env,
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct RedisClient {
    client: Client,
    redis: MultiplexedConnection,
}

impl RedisClient {
    pub async fn new() -> Result<Self> {
        let connection_uri = format!(
            "redis://{}:{}",
            env::var("REDIS_HOST").unwrap(),
            env::var("REDIS_PORT").unwrap()
        );
        let client = Client::open(connection_uri)?;
        let redis: MultiplexedConnection = client.get_multiplexed_async_connection().await?;

        Ok(Self { redis, client })
    }

    pub async fn add_scheduled_task(
        &mut self,
        queue_name: String,
        task: String,
        time: i32,
    ) -> Result<()> {
        let task_id = Uuid::new_v4().to_string();
        self.redis
            .set::<String, String, String>(task_id.clone(), task.clone())
            .await
            .unwrap();
        self.redis
            .zadd::<String, i32, String, String>(queue_name, task_id, time)
            .await
            .unwrap();
        Ok(())
    }

    pub async fn get_task(&mut self, queue_name: String) -> Result<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let lease_queue_name = format!("{}_LEASE", queue_name.clone());
        let script = Script::new(
            r"
            local queue_name = ARGV[1]
            local lease_queue_name = ARGV[2]
            local now = tonumber(ARGV[3])
            local lease_time = tonumber(ARGV[4])
            local tasks = redis.call('ZRANGEBYSCORE', queue_name, '-inf', now)

            for _, task in ipairs(tasks) do
                redis.call('ZREM', queue_name, task)
                redis.call('ZADD', lease_queue_name, lease_time, task)
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

        println!("{:#?}", result);

        Ok(String::new())
    }
}
