use crate::db::Database;
use either::{Either};
use serde_json::{Value as JsonVal, Map};
use serde::{Deserialize, Serialize};

pub type Res<T> = Result<T, &'static str>;

pub type JsonRef<'a> = &'a JsonVal;

pub type JsonRet<'a> = Either<JsonVal, JsonRef<'a>>;

pub type JsonRes<'a> = Res<JsonRet<'a>>;

const BAD_TYPE: &str = "bad type";

const BAD_WRITE: &str = "bad write";

const BAD_KEY: &str = "bad key";

const BAD_IO: &str = "bad io";

use serde_json::Number;

const NOT_NUM: &str = "expected number";

pub fn json_first(val: &JsonVal) -> Res<JsonVal> {
    match val {
        JsonVal::Number(val) => Ok(JsonVal::Number(val.clone())),
        JsonVal::Array(ref arr) => json_arr_first(arr),
        val => Ok(val.clone())
    }
} 

pub fn json_last(val: &JsonVal) -> Res<JsonVal> {
    match val {
        JsonVal::Number(val) => Ok(JsonVal::Number(val.clone())),
        JsonVal::Array(ref arr) => json_arr_last(arr),
        val => Ok(val.clone())
    }
} 

pub fn json_sum(val: &JsonVal) -> Res<JsonVal> {
    match val {
        JsonVal::Number(val) => Ok(JsonVal::Number(val.clone())),
        JsonVal::Array(ref arr) => json_arr_sum(arr),
        val => Ok(val.clone())
    }
} 

pub fn json_avg(val: &JsonVal) -> Res<JsonVal> {
    match val {
        JsonVal::Number(val) => Ok(JsonVal::Number(val.clone())),
        JsonVal::Array(ref arr) => json_arr_avg(arr),
        val => Ok(val.clone())
    }
} 

pub fn json_var(val: &JsonVal) -> Res<JsonVal> {
    match val {
        JsonVal::Number(val) => Ok(JsonVal::Number(val.clone())),
        JsonVal::Array(ref arr) => json_arr_var(arr),
        val => Ok(val.clone())
    }
} 

pub fn json_dev(val: &JsonVal) -> Res<JsonVal> {
    match val {
        JsonVal::Number(val) => Ok(JsonVal::Number(val.clone())),
        JsonVal::Array(ref arr) => json_arr_dev(arr),
        val => Ok(val.clone())
    }
}

pub fn json_max(val: &JsonVal) -> Res<JsonVal> {
    match val {
        JsonVal::Number(val) => Ok(JsonVal::Number(val.clone())),
        JsonVal::Array(ref arr) => match arr_max(arr)? {
            Some(val) => Ok(val),
            None => Ok(JsonVal::Null),
        },
        val => Ok(val.clone()),
    }
}

pub fn json_min(val: &JsonVal) -> Res<JsonVal> {
    match val {
        JsonVal::Number(val) => Ok(JsonVal::Number(val.clone())),
        JsonVal::Array(ref arr) => match arr_min(arr)? {
            Some(val) => Ok(val),
            None => Ok(JsonVal::Null),
        },
        val => Ok(val.clone()),
    }
}

fn json_arr_sum(s: &[JsonVal]) -> Res<JsonVal> {
    let mut total = 0.0f64;
    for val in s {
        match val {
            JsonVal::Number(num) => {
                total += num.as_f64().unwrap();
            }
            _ => return Err(NOT_NUM),
        }
    }
    let num = Number::from_f64(total).ok_or(NOT_NUM)?;
    Ok(JsonVal::Number(num)) 
}

fn json_arr_first(s: &[JsonVal]) -> Res<JsonVal> {
    if s.is_empty() {
        Err("emprty arr")
    } else {
        Ok(s[0].clone())
    }
}

fn json_arr_last(s: &[JsonVal]) -> Res<JsonVal> {
    if s.is_empty() {
        Err("emprty arr")
    } else {
        Ok(s[s.len()-1].clone())
    }
}

fn json_arr_avg(s: &[JsonVal]) -> Res<JsonVal> {
    let mut total = 0.0f64;
    for val in s {
        total += json_f64(val).ok_or(NOT_NUM)?;
    }
    let val = total / (s.len() as f64);
    let num = Number::from_f64(val).ok_or(NOT_NUM)?;
    Ok(JsonVal::Number(num)) 
}

fn json_arr_var(s: &[JsonVal]) -> Res<JsonVal> {
    let mut sum = 0.0f64;
    for val in s {
        sum += json_f64(val).ok_or(NOT_NUM)?;
    }
    let mean = sum / ((s.len() -1) as f64);
    let mut var = 0.0f64;
    for val in s {
        var += (json_f64(val).ok_or(NOT_NUM)? - mean).powf(2.0);
    }
    var /= (s.len()) as f64;
    let num = Number::from_f64(var).ok_or(NOT_NUM)?;
    Ok(JsonVal::Number(num)) 
}

fn json_arr_dev(s: &[JsonVal]) -> Res<JsonVal> {
    let mut sum = 0.0f64;
    for val in s {
        sum += json_f64(val).ok_or(NOT_NUM)?;
    }
    let avg = sum / (s.len() as f64);
    let mut var = 0.0f64;
    for val in s {
        var += (json_f64(val).ok_or(NOT_NUM)? - avg).powf(2.0);
    }
    var /= s.len() as f64;
    let num = Number::from_f64(var.sqrt()).ok_or(NOT_NUM)?;
    Ok(JsonVal::Number(num)) 
}

fn json_f64(val: &JsonVal) -> Option<f64> {
    match val {
        JsonVal::Number(num) => num.as_f64(),
        _ => None,
    }
}

fn arr_max(s: &[JsonVal]) -> Res<Option<JsonVal>> {
    if s.is_empty() {
        return Ok(None);
    }
    let mut max = match json_f64(&s[0]) {
        Some(val) => val,
        None => return Err("bad type"),
    };
    for val in s.iter().skip(1) {
        match val {
            JsonVal::Number(num) => {
                let v = num.as_f64().unwrap();
                if v > max {
                    max = v;
                }
            }
            _ => return Err("bad type")
        }
    }
    Ok(Some(JsonVal::from(max)))
}

fn arr_min(s: &[JsonVal]) -> Res<Option<JsonVal>> {
    if s.is_empty() {
        return Ok(None);
    }
    let mut min = match json_f64(&s[0]) {
        Some(val) => val,
        None => return Err("bad type"),
    };
    for val in s.iter().skip(1) {
        match val {
            JsonVal::Number(num) => {
                let v = num.as_f64().unwrap();
                if v < min {
                    min = v;
                }
            }
            _ => return Err("bad type")
        }
    }
    Ok(Some(JsonVal::from(min)))
}

#[derive(Serialize, Deserialize)]
pub enum Cmd {
    #[serde(rename = "get")]
    Get(String),
    #[serde(rename = "set")]
    Set(String, JsonVal),
    #[serde(rename = "sum")]
    Sum(Box<Cmd>),
    #[serde(rename = "max")]
    Max(Box<Cmd>),   
    #[serde(rename = "min")]
    Min(Box<Cmd>),
    Val(JsonVal),     
    Avg(Box<Cmd>),
    Dev(Box<Cmd>),
    Var(Box<Cmd>), 
    First(Box<Cmd>),
    Last(Box<Cmd>),
    Del(String),
}

pub fn parse_json_str<S:Into<String>>(s: S) -> Res<Cmd> {
    let json_val = serde_json::from_str(&s.into()).map_err(|_| "bad json")?;
    parse_json_val(json_val)
}

fn parse_json_val(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::String(key) => Ok(Cmd::Get(key)),
        JsonVal::Object(obj) => parse_obj(obj),
        _ => unimplemented!(),
    }
}

fn parse_obj(obj: Map<String, JsonVal>) -> Res<Cmd> {
    if obj.len() != 1 {
        return Err("not one key");
    }
    for (key, val) in obj {
        match key.as_ref() {
            "get" => return parse_get(val),
            "del" => return parse_del(val),
            "set" => return parse_set(val), 
            "min" => return parse_min(val),
            "max" => return parse_max(val),  
            "sum" => return parse_sum(val),  
            "avg" => return parse_avg(val),
            "var" => return parse_var(val),
            "dev" => return parse_dev(val),
            "first" => return parse_first(val),
            "last" => return parse_last(val),
            _ => unimplemented!(),
        }
    }
    Ok(Cmd::Set("k1".to_string(), JsonVal::Bool(true)))
}

fn parse_min(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::String(key) => Ok(Cmd::Min(Box::new(Cmd::Get(key)))),      
        JsonVal::Object(obj) => Ok(Cmd::Min(Box::new(parse_obj(obj)?))),  
        val => Ok(Cmd::Min(Box::new(Cmd::Val(val)))),
    }
}

fn parse_max(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::String(key) => Ok(Cmd::Max(Box::new(Cmd::Get(key)))),
        JsonVal::Object(obj) => Ok(Cmd::Max(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Max(Box::new(Cmd::Val(val)))),
    }
}

fn parse_avg(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::String(key) => Ok(Cmd::Avg(Box::new(Cmd::Get(key)))),
        JsonVal::Object(obj) => Ok(Cmd::Avg(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Sum(Box::new(Cmd::Val(val)))),
    }
}

fn parse_var(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::String(key) => Ok(Cmd::Var(Box::new(Cmd::Get(key)))),
        JsonVal::Object(obj) => Ok(Cmd::Var(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Sum(Box::new(Cmd::Val(val)))),
    }
}

fn parse_dev(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::String(key) => Ok(Cmd::Dev(Box::new(Cmd::Get(key)))),
        JsonVal::Object(obj) => Ok(Cmd::Dev(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Sum(Box::new(Cmd::Val(val)))),
    }
}

fn parse_sum(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::String(key) => Ok(Cmd::Sum(Box::new(Cmd::Get(key)))),
        JsonVal::Object(obj) => Ok(Cmd::Sum(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Sum(Box::new(Cmd::Val(val)))),
    }
}

fn parse_get(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::String(key) => Ok(Cmd::Get(key)),        
        _ => Err(BAD_TYPE),
    }
}

fn parse_del(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::String(key) => Ok(Cmd::Del(key)),
        _ => Err(BAD_TYPE),
    }
}

fn parse_set(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Array(mut arr) => {
            let val = arr.remove(1);
            let key = arr.remove(0);
            let key = match key {
                JsonVal::String(key) => key,
                _ => unimplemented!(),
            };
            Ok(Cmd::Set(key, val))
        }
        JsonVal::Object(_obj) => unimplemented!(),
        _ => unimplemented!()
    }
}

fn parse_first(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::String(key) => Ok(Cmd::First(Box::new(Cmd::Get(key)))),
        JsonVal::Object(obj) => Ok(Cmd::First(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::First(Box::new(Cmd::Val(val)))),
    }
}

fn parse_last(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::String(key) => Ok(Cmd::Last(Box::new(Cmd::Get(key)))),
        JsonVal::Object(obj) => Ok(Cmd::Last(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Last(Box::new(Cmd::Val(val)))),
    }
}

pub fn eval_json_cmd(cmd: Cmd, db: &mut Database) -> JsonRes<'_> {
    match cmd {
        Cmd::Get(ref key) => db.get(key).ok_or(BAD_KEY).map(Either::Right),
        Cmd::Del(ref key) => {
            match db.del(key) {
                Ok(Some(val)) => Ok(Either::Left(val)),
                Ok(None) => Ok(Either::Left(JsonVal::Null)),
                Err(_) => Err(BAD_IO),
            }
        }
        Cmd::Set(key, val) => db_write(db, key, val),
        Cmd::Sum(arg) => eval_sum(*arg, db),
        Cmd::Min(arg) => eval_min(*arg, db),
        Cmd::Max(arg) => eval_max(*arg, db),
        Cmd::Val(val) => Ok(Either::Left(val)),
        Cmd::Avg(arg) => eval_avg(*arg, db),
        Cmd::Dev(arg) => eval_dev(*arg, db),
        Cmd::Var(arg) => eval_var(*arg, db),
        Cmd::First(arg) => eval_first(*arg, db),
        Cmd::Last(arg) => eval_last(*arg, db),
    }
}

fn db_write(db: &mut Database, key: String, val: JsonVal) -> JsonRes<'_> {
    match db.set(key, val) {
        Ok(Some(val)) => Ok(Either::Left(val)),
        Ok(None) => Ok(Either::Left(JsonVal::Null)),
        Err(_) => Err(BAD_WRITE),
    }
}

fn eval_sum(arg: Cmd, db: &mut Database) -> JsonRes<'_> {
    match eval_json_cmd(arg, db) {
        Ok(Either::Left(ref val)) => {
            json_sum(val).map(Either::Left)
        }
        Ok(Either::Right(val)) => {
            json_sum(val).map(Either::Left)
        }
        Err(err) => Err(err),
    }
}

fn eval_avg(arg: Cmd, db: &mut Database) -> JsonRes<'_> {
    match eval_json_cmd(arg, db) {
        Ok(Either::Left(ref val)) => {
            json_avg(val).map(Either::Left)
        }
        Ok(Either::Right(val)) => {
            json_avg(val).map(Either::Left)
        }
        Err(err) => Err(err),
    }
}

fn eval_dev(arg: Cmd, db: &mut Database) -> JsonRes<'_> {
    match eval_json_cmd(arg, db) {
        Ok(Either::Left(ref val)) => {
            json_dev(val).map(Either::Left)
        }
        Ok(Either::Right(val)) => {
            json_dev(val).map(Either::Left)
        }
        Err(err) => Err(err),
    }
}

fn eval_var(arg: Cmd, db: &mut Database) -> JsonRes<'_> {
    match eval_json_cmd(arg, db) {
        Ok(Either::Left(ref val)) => {
            json_var(val).map(Either::Left)
        }
        Ok(Either::Right(val)) => {
            json_var(val).map(Either::Left)
        }
        Err(err) => Err(err),
    }
}

fn eval_first(arg: Cmd, db: &mut Database) -> JsonRes<'_> {
    match eval_json_cmd(arg, db) {
        Ok(Either::Left(ref val)) => {
            json_first(val).map(Either::Left)
        }
        Ok(Either::Right(val)) => {
            json_first(val).map(Either::Left)
        }
        Err(err) => Err(err),
    }
}

fn eval_last(arg: Cmd, db: &mut Database) -> JsonRes<'_> {
    match eval_json_cmd(arg, db) {
        Ok(Either::Left(ref val)) => {
            json_last(val).map(Either::Left)
        }
        Ok(Either::Right(val)) => {
            json_last(val).map(Either::Left)
        }
        Err(err) => Err(err),
    }
}

fn eval_max(arg: Cmd, db: &mut Database) -> JsonRes<'_> {
    match eval_json_cmd(arg, db) {
        Ok(Either::Left(ref val)) => {
            json_max(val).map(Either::Left)
        }
        Ok(Either::Right(val)) => {
            json_max(val).map(Either::Left)
        }
        Err(err) => Err(err),
    }
}

fn eval_min(arg: Cmd, db: &mut Database) -> JsonRes<'_> {
    match eval_json_cmd(arg, db) {
        Ok(Either::Left(ref val)) => {
            json_min(val).map(Either::Left)
        }
        Ok(Either::Right(val)) => {
            json_min(val).map(Either::Left)
        }
        Err(err) => Err(err),
    }
}
