use anyhow::Result;
use redis::AsyncCommands;
use redis::Script;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::client::RedisClient;

impl RedisClient {
    pub fn get_periodic_set_name(queue_name: String) -> String {
        format!("PERIODIC_SETS:{}", queue_name)
    }

    pub fn get_item_period_end_time(queue_name: String) -> String {
        format!("ITEM_END_TIME:{}", queue_name)
    }

    pub fn get_item_period_interval(queue_name: String) -> String {
        format!("ITEM_INTERVAL:{}", queue_name)
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

    pub async fn schedule_periodic_tasks(&mut self, queue_name: String) -> Result<bool> {
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
                local end_time = tonumber(redis.call('GET', end_time_key))


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
            .arg(queue_name)
            .arg(now.to_string())
            .invoke::<bool>(&mut self.client)
            .unwrap();

        println!("{:#?}", acked);

        Ok(acked)
    }
}
