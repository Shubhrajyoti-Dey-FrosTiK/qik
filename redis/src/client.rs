use anyhow::{Ok, Result};
use redis::{
    aio::{MultiplexedConnection, PubSub},
    AsyncCommands, Client, Commands, Connection, Script,
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
    AddItem,
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

    pub fn get_lease_time_key(item_id: String) -> String {
        format!("LEASE_TIME:{}", item_id)
    }

    pub fn get_item_key(item_id: String) -> String {
        format!("ITEM:{}", item_id)
    }

    pub fn get_lease_queue_name(queue_name: String) -> String {
        format!("LEASE_SETS:{}", queue_name)
    }

    pub fn get_periodic_set_name(queue_name: String) -> String {
        format!("PERIODIC_SETS:{}", queue_name)
    }

    pub fn get_item_period_end_time(queue_name: String) -> String {
        format!("ITEM_END_TIME:{}", queue_name)
    }

    pub fn get_item_period_interval(queue_name: String) -> String {
        format!("ITEM_INTERVAL:{}", queue_name)
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
            .zadd::<String, f64, String, String>(queue_name.clone(), task_id.clone(), time as f64)
            .await
            .unwrap();

        self.client
            .publish::<String, String, String>(
                String::from("UPDATE"),
                to_string(&Update {
                    queue_name,
                    task_id,
                    update_type: UpdateType::AddItem,
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
            local now = tonumber(ARGV[3])
            local no_of_tasks = tonumber(ARGV[4])
            local tasks = redis.call('ZRANGEBYSCORE', queue_name, '-inf', now, 'LIMIT', 0, no_of_tasks)

            for _, task in ipairs(tasks) do
                local lease_time_key = 'LEASE_TIME:' .. task
                local lease_time = tonumber(redis.call('GET', lease_time_key))
                redis.call('ZREM', queue_name, task)
                redis.call('ZADD', lease_queue_name, now + lease_time, task)
            end

            return tasks
            ",
        );

        let task_ids = script
            .arg(queue_name.clone())
            .arg(lease_queue_name)
            .arg(now.to_string())
            .arg(no_of_tasks.to_string())
            .invoke::<Vec<String>>(&mut self.client)
            .unwrap();

        for task_id in task_ids.clone() {
            let lease_time: u128 = self
                .redis
                .get(Self::get_lease_time_key(task_id.clone()))
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

    pub async fn add_periodic_task(
        &mut self,
        queue_name: String,
        task: String,
        lease_time: u128,
        task_start_time: u128,
        task_end_time: u128,
        task_interval: u128,
    ) -> Result<bool> {
        let task_id = Uuid::new_v4().to_string();

        // save task_id -> task, task_id -> lease_time, task_id -> end_time, task_id -> interval
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
            .set::<String, String, String>(
                Self::get_item_period_end_time(task_id.clone()),
                task_end_time.to_string(),
            )
            .await
            .unwrap();

        self.redis
            .set::<String, String, String>(
                Self::get_item_period_interval(task_id.clone()),
                task_interval.to_string(),
            )
            .await
            .unwrap();

        self.redis
            .zadd::<String, f64, String, String>(
                Self::get_periodic_set_name(queue_name.clone()),
                task_id.clone(),
                task_start_time as f64,
            )
            .await
            .unwrap();

        Ok(true)
    }

    pub async fn schedule_periodic_tasks(
        &mut self,
        queue_name: String,
    ) -> Result<bool> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let script = Script::new(
            r"
            local periodic_set_name = ARGV[1]
            local queue_name = ARGV[2]
            local now = tonumber(ARGV[3])

            local tasksDue = redis.call('ZRANGEBYSCORE', periodic_set_name, '-inf', now, 'WITHSCORES')

            for i = 1, #tasksDue, 2 do
                local task = tasksDue[i]
                local current_start_time = tonumber(tasksDue[i+1]) -- This is the score that triggered the run

                local interval_key = 'ITEM_INTERVAL:' .. task
                local end_time_key = 'ITEM_END_TIME:' .. task

                local interval = tonumber(redis.call('GET', interval_key))
                local end_time = tonumber(redis.call('GET', end_time))


                -- artificially become a client and add to tasks
                redis.call('ZADD', queue_name, current_start_time, task)

                -- update the current_start_time to current
                local next_start_time = current_start_time + interval
                if next_start_time <= end_time then
                    redis.call('ZADD', periodic_set_name, next_start_time, task)
                else
                    redis.call('ZREM', periodic_set_name, task)
                end
            end

            return true
            ",
        );

        let acked = script
            .arg(Self::get_periodic_set_name(queue_name.clone()))
            .arg(Self::get_lease_queue_name(queue_name.clone()))
            .arg(now.to_string())
            .invoke::<bool>(&mut self.client)
            .unwrap();

        println!("{:#?}", acked);

        Ok(acked)
    }
}
