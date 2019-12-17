use std::path::Path;
use std::collections::BTreeMap;
use serde_json::Value as JsonVal;
use std::sync::Mutex;
use crate::replay::*;
use std::io::{self};

pub struct Cache {
    pub map: BTreeMap<String, JsonVal>,
}

impl Cache {
    fn new() -> Cache {
        Cache { map: BTreeMap::new() }
    }

    fn write(&mut self, key: String, val: JsonVal) -> Option<JsonVal> {
        self.map.insert(key, val)
    }
}

/// The in-memory database shared amongst all clients.
///
/// This database will be shared via `Arc`, so to mutate the internal map we're
/// going to use a `Mutex` for interior mutability.
pub struct Database {
    pub cache: Cache,
    pub log: ReplayLog,
}

impl Database {
    pub fn open<P:AsRef<Path>>(path: P) -> io::Result<Database> {
        let mut log = ReplayLog::open("replay.memson")?;
        let cache = Cache { map: log.replay() };
        Ok(Database { cache, log })
    }

    pub fn write(&mut self, key: String, val: JsonVal) -> io::Result<Option<JsonVal>> {
        self.log.write(&key, &val)?;
        Ok(self.cache.write(key, val))
    }

    pub fn get(&self, key: &str) -> Option<&JsonVal> {
        self.cache.map.get(key)
    }
}