use std::{
    mem,
    sync::{Arc, Mutex},
    thread,
};

use redis::{Client, Commands, ControlFlow, FromRedisValue, PubSubCommands};

pub struct RedisDatatype<T> {
    client: redis::Client,
    thread: Option<thread::JoinHandle<()>>,
    value: Arc<Mutex<T>>,
    key: String,
}

impl<T> Drop for RedisDatatype<T> {
    fn drop(&mut self) {
        let _handle = mem::take(&mut self.thread);
        //handle.unwrap().join().unwrap();
    }
}

impl<T> RedisDatatype<T>
where
    T: serde::de::DeserializeOwned
        + serde::Serialize
        + std::marker::Send
        + Clone
        + FromRedisValue
        + 'static,
{
    pub fn new(val: T, uri: &str) -> Self {
        let client = redis::Client::open(uri).unwrap();
        let key = uuid::Uuid::new_v4().to_string();
        let value = Arc::new(Mutex::new(val.clone()));

        let handle = RedisDatatype::subscribe(value.clone(), key.to_string(), &client);

        let mut str = RedisDatatype {
            client,
            value,
            key,
            thread: Some(handle),
        };

        str.set(val).unwrap();
        str
    }

    pub fn subscribe(value: Arc<Mutex<T>>, key: String, client: &Client) -> thread::JoinHandle<()> {
        let mut con = client.get_connection().unwrap();

        thread::spawn(move || {
            let _: () = con
                .subscribe(&[key], |msg| {
                    let received: String = msg.get_payload().unwrap();
                    let message_obj = serde_json::from_str::<T>(&received).unwrap();
                    {
                        let mut val = value.lock().unwrap();
                        *val = message_obj;
                    }

                    ControlFlow::Continue
                })
                .unwrap();
        })
    }

    pub fn set(&mut self, val: T) -> redis::RedisResult<()> {
        let mut con = self.client.get_connection().unwrap();
        con.set(&self.key, serde_json::to_string(&val).unwrap())?;

        {
            let mut value = self.value.lock().unwrap();
            *value = val;
        }

        Ok(())
    }

    pub fn get(&self) -> T {
        let mut con = self.client.get_connection().unwrap();
        let val = con.get::<String, String>(self.key.to_string()).unwrap();

        serde_json::from_str::<T>(&val).unwrap()
    }
}

#[cfg(test)]
mod test_super {
    use super::*;

    #[test]
    fn test_string() {
        let mut str = RedisDatatype::new("hey".to_string(), "redis://127.0.0.1/");
        assert_eq!("hey", str.get());

        str.set("val".to_string()).unwrap();
        assert_eq!("val", str.get());
    }

    #[test]
    fn test_int() {
        let mut str = RedisDatatype::new(123i32, "redis://127.0.0.1/");
        assert_eq!(123, str.get());

        str.set(333i32).unwrap();
        assert_eq!(333, str.get());
    }

    #[test]
    fn test_bool() {
        let mut str = RedisDatatype::new(true, "redis://127.0.0.1/");
        assert!(str.get());

        str.set(false).unwrap();
        assert!(!str.get());
    }
}
