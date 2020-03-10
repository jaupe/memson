use std::path::Path;
use std::collections::BTreeMap;
use serde_json::Value as JsonVal;
use crate::json::*;
use crate::replay::*;
use std::io::{self};

// Type wrapper
pub type Cache = BTreeMap<String, JsonVal>;

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
        self.cache.get(key)
    }

    pub fn del(&mut self, key: &str) -> io::Result<Option<JsonVal>> {
        self.log.remove(&key)?;
        Ok(self.cache.remove(key))
    }

    pub fn eval<'a, S:Into<String>>(&'a mut self, line: S) -> JsonRes<'a> {
        let line = line.into();
        println!("line={:?}", line);
        let json_val: Cmd = parse_json_str(line)?;
        eval_json_cmd(json_val, self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use either::Either;

    use assert_approx_eq::assert_approx_eq;
    
    fn s<S:Into<String>>(arg: S) -> String {
        "\"".to_string() + &arg.into() + "\""
    }

    fn get<S:Into<String>>(arg: S) -> String {
        "{\"get\":".to_string() + &arg.into() + "}" 
    }

    fn first<S:Into<String>>(arg: S) -> String {
        json_fn("first", arg)
    }

    fn last<S:Into<String>>(arg: S) -> String {
        json_fn("last", arg)
    }

    fn max<S:Into<String>>(arg: S) -> String {
        json_fn("max", arg)
    }  
    
    fn min<S:Into<String>>(arg: S) -> String {
        json_fn("min", arg)
    } 
    
    fn avg<S:Into<String>>(arg: S) -> String {
        json_fn("avg", arg)
    }   
    
    fn var<S:Into<String>>(arg: S) -> String {
        json_fn("var", arg)
    }   
    
    fn dev<S:Into<String>>(arg: S) -> String {
        json_fn("dev", arg)
    }       

    fn json_fn<S: Into<String>>(f: &str, arg: S) -> String {
        "{\"".to_string() + f + "\":" + &arg.into() + "}"  
    }

    fn json_fn_get<S:Into<String>>(f: &str, arg: S) -> String {
        json_fn(f, get(arg)) 
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
        assert_eq!(7, db.cache.len());
    }

    #[test]
    fn test_get() {
        let mut db = test_db();
        let val = eval(&mut db, get(s("b")));
        assert_eq!(Either::Right(&JsonVal::Bool(true)), val);

        let val = eval(&mut db, "\"b\"");
        assert_eq!(Either::Right(&JsonVal::Bool(true)), val);

        let val = db.eval(get(s("ia"))).unwrap();
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

        let val = db.eval(json_fn("get", s("i"))).unwrap();
        assert_eq!(
            Either::Right(&JsonVal::from(3)),
            val
        );        
    }

    #[test]
    fn test_first() {
        let mut db = test_db();
        let val = eval(&mut db, first(s("b")));
        assert_eq!(Either::Left(JsonVal::Bool(true)), val);
        let val = db.eval(&json_fn_get("first", s("b"))).unwrap();
        assert_eq!(Either::Left(JsonVal::Bool(true)), val);
        let val = db.eval(&json_fn("first", s("f"))).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3.3)), val);
        let val = db.eval(&json_fn("first", s("i"))).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3)), val);
        let val = db.eval(&json_fn("first", s("fa"))).unwrap();
        assert_eq!(Either::Left(JsonVal::from(1.0)), val);
        let val = db.eval(&json_fn("first", s("ia"))).unwrap();
        assert_eq!(Either::Left(JsonVal::from(1)), val);        
    }

    #[test]
    fn test_last() {
        let mut db = test_db();
        let val = eval(&mut db, last(s("b")));
        assert_eq!(Either::Left(JsonVal::from(true)), val);
        let val = db.eval(&json_fn_get("last", s("b"))).unwrap();
        assert_eq!(Either::Left(JsonVal::from(true)), val);
        let val = db.eval(&json_fn("last", s("f"))).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3.3)), val);
        let val = db.eval(&json_fn("last", s("i"))).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3)), val);
        let val = db.eval(&json_fn("last", s("fa"))).unwrap();
        assert_eq!(Either::Left(JsonVal::from(5.0)), val);
        let val = db.eval(&json_fn("last", s("ia"))).unwrap();
        assert_eq!(Either::Left(JsonVal::from(5)), val);        
    }

    #[test]
    fn test_max() {
        let mut db = test_db();
        let val = eval(&mut db, max(s("b")));
        assert_eq!(Either::Left(JsonVal::Bool(true)), val);
        let val = eval(&mut db, max(&get(s("b"))));
        assert_eq!(Either::Left(JsonVal::Bool(true)), val);
        let val = eval(&mut db, max(&get(s("i"))));
        assert_eq!(Either::Left(JsonVal::from(3)), val);
        let val = eval(&mut db, max(s("f")));
        assert_eq!(Either::Left(JsonVal::from(3.3)), val);
        let val = eval(&mut db, max(s("ia")));
        assert_eq!(Either::Left(JsonVal::from(5.0)), val);
        let val = eval(&mut db, max(s("fa")));
        assert_eq!(Either::Left(JsonVal::from(5.0)), val);
    }

    #[test]
    fn test_min() {
        let mut db = test_db();
        let val = eval(&mut db, min(s("b")));
        assert_eq!(Either::Left(JsonVal::Bool(true)), val);
        let val = eval(&mut db, min(s("i")));
        assert_eq!(Either::Left(JsonVal::from(3)), val);
        let val = eval(&mut db, min(s("f")));
        assert_eq!(Either::Left(JsonVal::from(3.3)), val);
        let val = eval(&mut db, min(s("fa")));
        assert_eq!(Either::Left(JsonVal::from(1.0)), val);
        let val = eval(&mut db, min(s("ia")));
        assert_eq!(Either::Left(JsonVal::from(1.0)), val);
    }

    #[test]
    fn test_avg() {
        let mut db = test_db();
        let val = eval(&mut db, avg(s("f")));
        assert_eq!(Either::Left(JsonVal::from(3.3)), val);
        let val = db.eval(&json_fn("avg", s("i"))).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3)), val);
        let val = db.eval(&json_fn("avg", s("fa"))).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3.0)), val);
        let val = db.eval(&json_fn("avg", s("ia"))).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3.0)), val);        
    }

    #[test]
    fn test_var() {
        let mut db = test_db();
        let val = eval(&mut db, var(s("f")));
        assert_eq!(Either::Left(JsonVal::from(3.3)), val);
        let val = db.eval(&json_fn("var", s("i"))).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3)), val);
        let val = db.eval(&json_fn("var", s("fa"))).unwrap();
        assert_approx_eq!(2.56, val_f64(val), 0.0249f64);
        let val = db.eval(&json_fn("var", s("ia"))).unwrap();
        assert_approx_eq!(2.56, val_f64(val), 0.0249f64);        
    } 
    
    #[test]
    fn test_dev() {
        let mut db = test_db();
        let val = eval(&mut db, dev(s("f")));
        assert_eq!(Either::Left(JsonVal::from(3.3)), val);
        let val = eval(&mut db, dev(s("i")));
        assert_eq!(Either::Left(JsonVal::from(3)), val);
        let val = eval(&mut db, dev(s("fa")));
        assert_approx_eq!(1.4, val_f64(val), 0.0249f64);
        let val = eval(&mut db, dev(s("ia")));
        assert_approx_eq!(1.4, val_f64(val), 0.0249f64);        
    }
}
