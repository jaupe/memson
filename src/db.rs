use std::path::Path;
use std::collections::BTreeMap;
use serde_json::Value as JsonVal;
use crate::json::*;
use crate::replay::*;
use std::io::{self};

pub struct Cache {
    map: BTreeMap<String, JsonVal>,
}

impl Cache {
    pub fn new() -> Cache {
        Cache { map: BTreeMap::new() }
    }

    pub fn insert(&mut self, key: String, val: JsonVal) -> Option<JsonVal> {
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
    pub fn open<P:AsRef<Path>>(path: P) -> Res<Database> {
        let mut log = ReplayLog::open(path).map_err(|err| { eprintln!("{:?}", err); "bad io"})?;
        let cache = log.replay()?;
        Ok(Database { cache, log })
    }

    pub fn set(&mut self, key: String, val: JsonVal) -> io::Result<Option<JsonVal>> {
        self.log.write(&key, &val)?;
        Ok(self.cache.insert(key, val))
    }

    pub fn get(&self, key: &str) -> Option<&JsonVal> {
        self.cache.map.get(key)
    }

    pub fn del(&mut self, key: &str) -> io::Result<Option<JsonVal>> {
        self.log.remove(&key)?;
        Ok(self.cache.map.remove(key))
    }

    pub fn eval<'a>(&'a mut self, line: &str) -> JsonRes<'a> {
        let json_val: Cmd = parse_json_str(line)?;
        eval_json_cmd(json_val, self)
    }

    pub fn len(&self) -> usize {
        self.cache.map.len()
    }
}