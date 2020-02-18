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
    cache: Cache,
    log: ReplayLog,
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

    pub fn eval<'a, S:Into<String>>(&'a mut self, line: S) -> JsonRes<'a> {
        let json_val: Cmd = parse_json_str(line)?;
        eval_json_cmd(json_val, self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use either::Either;

    use assert_approx_eq::assert_approx_eq;
    
    fn s(key: &str) -> String {
        "\"".to_string() + key + "\""
    }

    fn get(key: &str) -> String {
        "{\"get\":".to_string() + &s(key) + "}" 
    }

    fn first(key: &str) -> String {
        json_fn("first", key)
    }

    fn last(key: &str) -> String {
        json_fn("last", key)
    }

    fn max(key: &str) -> String {
        json_fn("max", key)
    }  
    
    fn min(key: &str) -> String {
        json_fn("min", key)
    } 
    
    fn avg(key: &str) -> String {
        json_fn("avg", key)
    }   
    
    fn var(key: &str) -> String {
        json_fn("var", key)
    }   
    
    fn dev(key: &str) -> String {
        json_fn("dev", key)
    }       

    fn json_fn(f: &str, key: &str) -> String {
        "{\"".to_string() + f + "\":\"" + key + "\"}"  
    }

    fn json_fn_get(f: &str, key: &str) -> String {
        "{\"".to_string() + f + "\": {\"get\":\"" + key + "\"}}"  
    }

    fn test_db() -> Database {
        Database::open("test.log").unwrap()
    }

    fn json_f64(v: &JsonVal) -> f64 {
        v.as_f64().unwrap()
    }

    fn val_f64<'a>(v: Either<JsonVal, &'a JsonVal>) -> f64 {
        match v {
            Either::Left(ref val) => json_f64(val),
            Either::Right(val) => json_f64(val),
        }
    }

    fn eval<'a, S: Into<String>>(db: &'a mut Database, line: S) -> JsonRet<'a> {
        db.eval(line).unwrap()
    }

    #[test]
    fn open_db() {
        let db = test_db();
        assert_eq!(7, db.cache.map.len());
    }

    #[test]
    fn test_get() {
        let mut db = test_db();
        let val = eval(&mut db, get("b"));
        assert_eq!(Either::Right(&JsonVal::Bool(true)), val);

        let val = eval(&mut db, "\"b\"");
        assert_eq!(Either::Right(&JsonVal::Bool(true)), val);

        let val = db.eval(get("ia")).unwrap();
        assert_eq!(
            Either::Right(&JsonVal::Array(vec![
                JsonVal::from(1),
                JsonVal::from(2),
                JsonVal::from(3),
                JsonVal::from(4),
                JsonVal::from(5)
            ])),
            val
        );

        let val = db.eval(&json_fn("get", "i")).unwrap();
        assert_eq!(
            Either::Right(&JsonVal::from(3)),
            val
        );        
    }

    #[test]
    fn test_first() {
        let mut db = test_db();
        let val = eval(&mut db, first("b"));
        assert_eq!(Either::Left(JsonVal::Bool(true)), val);
        let val = db.eval(&json_fn_get("first", "b")).unwrap();
        assert_eq!(Either::Left(JsonVal::Bool(true)), val);
        let val = db.eval(&json_fn("first", "f")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3.3)), val);
        let val = db.eval(&json_fn("first", "i")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3)), val);
        let val = db.eval(&json_fn("first", "fa")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(1.0)), val);
        let val = db.eval(&json_fn("first", "ia")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(1)), val);        
    }

    #[test]
    fn test_last() {
        let mut db = test_db();
        let val = eval(&mut db, last("b"));
        assert_eq!(Either::Left(JsonVal::from(true)), val);
        let val = db.eval(&json_fn_get("last", "b")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(true)), val);
        let val = db.eval(&json_fn("last", "f")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3.3)), val);
        let val = db.eval(&json_fn("last", "i")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3)), val);
        let val = db.eval(&json_fn("last", "fa")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(5.0)), val);
        let val = db.eval(&json_fn("last", "ia")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(5)), val);        
    }

    #[test]
    fn test_max() {
        let mut db = test_db();
        let val = eval(&mut db, max("b"));
        assert_eq!(Either::Left(JsonVal::Bool(true)), val);
        let val = db.eval(r#"{"max": {"get": "i"}}"#).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3)), val);
        let val = db.eval(r#"{"max": {"get": "f"}}"#).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3.3)), val);
        let val = db.eval(r#"{"max": {"get": "ia"}}"#).unwrap();
        assert_eq!(Either::Left(JsonVal::from(5.0)), val);
        let val = db.eval(r#"{"max": {"get": "fa"}}"#).unwrap();
        assert_eq!(Either::Left(JsonVal::from(5.0)), val);
    }
    #[test]
    fn test_min() {
        let mut db = test_db();
        let val = eval(&mut db, min("b"));
        assert_eq!(Either::Left(JsonVal::Bool(true)), val);
        let val = eval(&mut db, min("i"));
        assert_eq!(Either::Left(JsonVal::from(3)), val);
        let val = eval(&mut db, min("f"));
        assert_eq!(Either::Left(JsonVal::from(3.3)), val);
        let val = eval(&mut db, min("fa"));
        assert_eq!(Either::Left(JsonVal::from(1.0)), val);
        let val = eval(&mut db, min("ia"));
        assert_eq!(Either::Left(JsonVal::from(1.0)), val);
    }

    #[test]
    fn test_avg() {
        let mut db = test_db();
        let val = eval(&mut db, avg("f"));
        assert_eq!(Either::Left(JsonVal::from(3.3)), val);
        let val = db.eval(&json_fn("avg", "i")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3)), val);
        let val = db.eval(&json_fn("avg", "fa")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3.0)), val);
        let val = db.eval(&json_fn("avg", "ia")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3.0)), val);        
    }

    #[test]
    fn test_var() {
        let mut db = test_db();
        let val = eval(&mut db, var("f"));
        assert_eq!(Either::Left(JsonVal::from(3.3)), val);
        let val = db.eval(&json_fn("var", "i")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3)), val);
        let val = db.eval(&json_fn("var", "fa")).unwrap();
        assert_approx_eq!(2.56, val_f64(val), 0.0249f64);
        let val = db.eval(&json_fn("var", "ia")).unwrap();
        assert_approx_eq!(2.56, val_f64(val), 0.0249f64);        
    } 
    
    #[test]
    fn test_dev() {
        let mut db = test_db();
        let val = eval(&mut db, dev("f"));
        assert_eq!(Either::Left(JsonVal::from(3.3)), val);
        let val = db.eval(&json_fn("dev", "i")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3)), val);
        let val = db.eval(&json_fn("dev", "fa")).unwrap();
        assert_approx_eq!(1.4, val_f64(val), 0.0249f64);
        let val = db.eval(&json_fn("dev", "ia")).unwrap();
        assert_approx_eq!(1.4, val_f64(val), 0.0249f64);        
    }     
}
