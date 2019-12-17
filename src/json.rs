use crate::db::Database;
use either::{Either, Left, Right};
use serde_json::{Map, Value as JsonVal, Number};
use std::collections::BTreeMap;

type Cache = BTreeMap<String, JsonVal>;

fn json_f64(val: &JsonVal) -> Result<f64, String> {
    val.as_f64().ok_or("bad float".to_string())
}

fn json_arr_sum(arr: &[JsonVal]) -> Option<JsonVal> {
    let mut total = 0.0f64;
    for val in arr {
        let val = match val.as_f64() {
            Some(val) => val,
            None => return None,
        };
        total += val;
    }
    Some(JsonVal::from(total))
}

fn json_sum<'a>(val: &JsonVal) -> Option<JsonVal> {
    println!("json_sum={:?}", val);
    match val {
        JsonVal::Array(arr) => json_arr_sum(arr),
        JsonVal::Bool(b) => Some(if *b {
            JsonVal::from(1)
        } else {
            JsonVal::from(0)
        }),
        JsonVal::Null => Some(JsonVal::from(0)),
        JsonVal::Number(num) => Some(JsonVal::Number(num.clone())),
        JsonVal::Object(_) => None,
        JsonVal::String(_) => None,
    }
}

fn eval_json_set<'a>(val: &JsonVal, db: &'a mut Database) -> Option<Either<JsonVal, &'a JsonVal>> {
    match val {
        JsonVal::Array(arr) => {
            let key = match &arr[0] {
                JsonVal::String(key) => key.clone(),
                _ => unimplemented!(),
            };
            let val = match eval_json(&arr[1], db) {
                Some(Either::Left(val)) => val,
                Some(Either::Right(val)) => val.clone(),
                None => return None,
            };
            match db.write(key, val) {
                Ok(Some(val)) => Some(Either::Left(val)),
                Ok(None) | Err(_) => None,
            }
        }
        _ => None,
    }
}

fn eval_json_obj<'a>(
    obj: &'a Map<String, JsonVal>,
    db: &'a mut Database,
) -> Option<Either<JsonVal, &'a JsonVal>> {
    for (key, arg) in obj.iter() {
        match key.as_ref() {
            "get" => match arg {
                JsonVal::String(key) => return db.get(key).map(Either::Right),
                _ => unimplemented!(),
            },
            "set" => {
                return eval_json_set(arg, db);
            }
            "sum" => {
                return match eval_json(arg, db) {
                    Some(Either::Left(val)) => json_sum(&val).map(Left),
                    Some(Either::Right(val)) => json_sum(&val).map(Left),
                    None => None,
                };
            }
            "first" => {
                let v = eval_json(arg, db);
                return match v {
                    Some(Either::Left(_)) => unimplemented!(),
                    Some(Either::Right(val)) => json_first(val).map(Right),
                    None => None,
                };
            }
            "last" => {
                return match eval_json(arg, db) {
                    Some(Either::Left(_)) => unimplemented!(),
                    Some(Either::Right(val)) => json_last(val).map(Right),
                    None => None,
                };
            }
            "+" => {
                return match eval_json(arg, db) {
                    Some(Either::Left(ref val)) => json_add(val).map(Left),
                    Some(Either::Right(val)) => json_add(val).map(Left),
                    None => None,
                };
            }
            _ => unimplemented!(),
        }
    }
    None
}

pub fn eval_json<'a>(val: &'a JsonVal, db: &'a mut Database) -> Option<Either<JsonVal, &'a JsonVal>> {
    match val {
        JsonVal::String(msg) => db.get(msg).map(Right),
        JsonVal::Object(obj) => eval_json_obj(obj, db),
        val => Some(Either::Left(val.clone())),
    }
}

fn json_first<'a,'b:'a>(val: &'b JsonVal) -> Option<&'a JsonVal> {
    match val {
        JsonVal::Array(arr) => arr.get(0),
        val => Some(val),
    }
}

pub fn json_last(val: &JsonVal) -> Option<&JsonVal> {
    match val {
        JsonVal::Array(arr) => {
            if arr.is_empty() {
                None
            } else {
                let n = arr.len() - 1;
                arr.get(n)
            }
        }
        val => Some(val),
    }
}

fn json_add(val: &JsonVal) -> Option<JsonVal> {
    match val {
        JsonVal::Array(ref arr) => add_arr(arr),
        num@JsonVal::Number(_) => Some(num.clone()),
        _ => unimplemented!(),
    }
}

fn add_arr(arr: &[JsonVal]) -> Option<JsonVal> {
    let lhs = &arr[0];
    let rhs = &arr[1];
    match (lhs, rhs) {
        (JsonVal::Number(x), JsonVal::Number(y)) => Some(add_nums(x, y)),
        _ => unimplemented!(),
    }
}

fn add_nums(x: &Number, y: &Number) -> JsonVal {
    JsonVal::from(x.as_f64().unwrap() + y.as_f64().unwrap())
}
