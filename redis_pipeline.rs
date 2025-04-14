// src/models/redis_pipeline.rs

/*
    // 基础操作
    let result = state.redis.pipeline().await?
        .set("key1", "value1")
        .get("key1")
        .execute()
        .await?;

    // Hash操作
    let mut fields = HashMap::new();
    fields.insert("field1", "value1".to_string());
    fields.insert("field2", "value2".to_string());

    let result = state.redis.pipeline().await?
        .hset_multiple("hash_key", fields)
        .hgetall("hash_key")
        .execute()
        .await?;

    // 原子性操作
    let result = state.redis.pipeline().await?
        .atomic()
        .increment("counter")
        .get("counter")
        .execute()
        .await?;

    // 事务操作
    let result = state.redis.pipeline().await?
        .multi()
        .set("key1", "value1")
        .set("key2", "value2")
        .exec()
        .execute()
        .await?;

    // 使用结果扩展trait
    let count = result.get_last_i64()?;

*/


use crate::models::redis::MyError;
use r2d2_redis::redis::{pipe, Pipeline, Value, FromRedisValue};
use r2d2::PooledConnection;
use r2d2_redis::RedisConnectionManager;
use std::collections::HashMap;

pub struct RedisPipeline {
    conn: PooledConnection<RedisConnectionManager>,
    pipe: Pipeline,
}

impl RedisPipeline {
    pub(crate) fn new(conn: PooledConnection<RedisConnectionManager>) -> Self {
        Self {
            conn,
            pipe: pipe(),
        }
    }

    // 基础 Redis 操作
    pub fn set<T: ToString>(mut self, key: &str, value: T) -> Self {
        self.pipe.cmd("SET").arg(key).arg(value.to_string());
        self
    }

    pub fn set_i64(mut self, key: &str, value: i64) -> Self {
        self.pipe.cmd("SET").arg(key).arg(value);
        self
    }

    pub fn set_i32(mut self, key: &str, value: i32) -> Self {
        self.pipe.cmd("SET").arg(key).arg(value);
        self
    }

    pub fn set_with_expiry<T: ToString>(mut self, key: &str, value: T, expiry: i64) -> Self {
        self.pipe.cmd("SETEX").arg(key).arg(expiry).arg(value.to_string());
        self
    }

    pub fn get(mut self, key: &str) -> Self {
        self.pipe.cmd("GET").arg(key);
        self
    }

    pub fn get_i64(mut self, key: &str) -> Self {
        self.pipe.cmd("GET").arg(key);
        self
    }

    pub fn get_i32(mut self, key: &str) -> Self {
        self.pipe.cmd("GET").arg(key);
        self
    }

    pub fn get_str(mut self, key: &str) -> Self {
        self.pipe.cmd("GET").arg(key);
        self
    }

    pub fn set_str(mut self, key: &str, value: &str) -> Self {
        self.pipe.cmd("SET").arg(key).arg(value);
        self
    }

    pub fn increment(mut self, key: &str) -> Self {
        self.pipe.cmd("INCR").arg(key);
        self
    }

    pub fn decrement(mut self, key: &str) -> Self {
        self.pipe.cmd("DECR").arg(key);
        self
    }

    pub fn delete(mut self, key: &str) -> Self {
        self.pipe.cmd("DEL").arg(key);
        self
    }

    pub fn exists(mut self, key: &str) -> Self {
        self.pipe.cmd("EXISTS").arg(key);
        self
    }

    pub fn set_expiry(mut self, key: &str, expiry: usize) -> Self {
        self.pipe.cmd("EXPIRE").arg(key).arg(expiry);
        self
    }

    // List 操作
    pub fn rpush_multiple(mut self, key: &str, values: &[String]) -> Self {
        self.pipe.cmd("RPUSH").arg(key).arg(values);
        self
    }

    pub fn lrange(mut self, key: &str, start: isize, end: isize) -> Self {
        self.pipe.cmd("LRANGE").arg(key).arg(start).arg(end);
        self
    }

    // Set 操作
    pub fn sadd_multiple(mut self, key: &str, values: &[String]) -> Self {
        self.pipe.cmd("SADD").arg(key).arg(values);
        self
    }

    pub fn smembers(mut self, key: &str) -> Self {
        self.pipe.cmd("SMEMBERS").arg(key);
        self
    }

    // Hash 操作
    pub fn hset(mut self, key: &str, field: &str, value: &str) -> Self {
        self.pipe.cmd("HSET").arg(key).arg(field).arg(value);
        self
    }

    pub fn hget(mut self, key: &str, field: &str) -> Self {
        self.pipe.cmd("HGET").arg(key).arg(field);
        self
    }

    pub fn hdel(mut self, key: &str, field: &str) -> Self {
        self.pipe.cmd("HDEL").arg(key).arg(field);
        self
    }

    pub fn hgetall(mut self, key: &str) -> Self {
        self.pipe.cmd("HGETALL").arg(key);
        self
    }

    pub fn hexists(mut self, key: &str, field: &str) -> Self {
        self.pipe.cmd("HEXISTS").arg(key).arg(field);
        self
    }

    pub fn hset_multiple(mut self, key: &str, fields: HashMap<&str, String>) -> Self {
        for (field, value) in fields {
            self.pipe.cmd("HSET").arg(key).arg(field).arg(value);
        }
        self
    }

    // Pattern 操作
    pub fn get_keys_by_pattern(mut self, pattern: &str) -> Self {
        self.pipe.cmd("KEYS").arg(pattern);
        self
    }

    // Eval 操作
    pub fn eval(mut self, script: &str, keys: &[&str], args: &[&str]) -> Self {
        self.pipe.cmd("EVAL")
            .arg(script)
            .arg(keys.len())
            .arg(keys)
            .arg(args);
        self
    }

    // 执行pipeline
    pub async fn execute(mut self) -> Result<Vec<Value>, MyError> {
        self.pipe.query(&mut *self.conn)
            .map_err(|e| MyError::RedisError(e.to_string()))
    }

    // 执行pipeline并获取单个结果
    pub async fn execute_single<T: FromRedisValue>(mut self) -> Result<T, MyError> {
        let mut results: Vec<T> = self.pipe.query(&mut *self.conn)
            .map_err(|e| MyError::RedisError(e.to_string()))?;
        
        results.pop()
            .ok_or_else(|| MyError::RedisError("No result returned".to_string()))
    }

    // 原子性控制
    pub fn atomic(mut self) -> Self {
        self.pipe.atomic();
        self
    }

    // 事务控制
    pub fn multi(mut self) -> Self {
        self.pipe.cmd("MULTI");
        self
    }

    pub fn exec(mut self) -> Self {
        self.pipe.cmd("EXEC");
        self
    }

    // 辅助方法
    pub fn cmd(mut self, cmd: &str) -> Self {
        self.pipe.cmd(cmd);
        self
    }

    pub fn arg<T: ToString>(mut self, arg: T) -> Self {
        self.pipe.arg(arg.to_string());
        self
    }
}

// 结果处理的辅助trait
pub trait PipelineResultExt {
    fn get_last_i64(&self) -> Result<i64, MyError>;
    fn get_last_i32(&self) -> Result<i32, MyError>;
    fn get_last_string(&self) -> Result<String, MyError>;
    fn get_last_bool(&self) -> Result<bool, MyError>;
}

impl PipelineResultExt for Vec<Value> {
    fn get_last_i64(&self) -> Result<i64, MyError> {
        self.last()
            .and_then(|v| match v {
                Value::Int(i) => Some(*i),
                _ => None
            })
            .ok_or_else(|| MyError::RedisError("Failed to get i64 value".to_string()))
    }

    fn get_last_i32(&self) -> Result<i32, MyError> {
        self.last()
            .and_then(|v| match v {
                Value::Int(i) => Some(*i as i32),
                _ => None
            })
            .ok_or_else(|| MyError::RedisError("Failed to get i32 value".to_string()))
    }

    fn get_last_string(&self) -> Result<String, MyError> {
        self.last()
            .and_then(|v| match v {
                Value::Data(bytes) => String::from_utf8(bytes.clone()).ok(),
                _ => None
            })
            .ok_or_else(|| MyError::RedisError("Failed to get string value".to_string()))
    }

    fn get_last_bool(&self) -> Result<bool, MyError> {
        self.last()
            .and_then(|v| match v {
                Value::Int(i) => Some(*i != 0),
                _ => None
            })
            .ok_or_else(|| MyError::RedisError("Failed to get bool value".to_string()))
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::redis::RedisPools;
    use std::env;

    async fn setup() -> RedisPools {
        let redis_uri = env::var("redis://:meteplus@121.201.101.85/").unwrap();
        RedisPools::new(redis_uri, 1)
    }

    #[tokio::test]
    async fn test_basic_operations() {
        let redis = setup().await;
        let result = redis.pipeline().await.unwrap()
            .set("key1", "value1")
            .get("key1")
            .execute()
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_hash_operations() {
        let redis = setup().await;
        let mut fields = HashMap::new();
        fields.insert("field1", "value1".to_string());
        fields.insert("field2", "value2".to_string());

        let result = redis.pipeline().await.unwrap()
            .hset_multiple("hash_key", fields)
            .hgetall("hash_key")
            .execute()
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_atomic_operations() {
        let redis = setup().await;
        let result = redis.pipeline().await.unwrap()
            .atomic()
            .increment("counter")
            .get("counter")
            .execute()
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_transaction_operations() {
        let redis = setup().await;
        let result = redis.pipeline().await.unwrap()
            .multi()
            .set("key1", "value1")
            .set("key2", "value2")
            .exec()
            .execute()
            .await;
        assert!(result.is_ok());
    }
}