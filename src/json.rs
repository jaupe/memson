use crate::db::Database;
use serde::{Deserialize, Serialize};
use serde_json::Number as JsonNum;
use serde_json::Number;
use serde_json::{Map, Value as JsonVal};

pub type Res<T> = Result<T, &'static str>;

const BAD_TYPE: &str = "bad type";

const BAD_WRITE: &str = "bad write";

const BAD_KEY: &str = "bad key";

const BAD_IO: &str = "bad io";

const BAD_JSON: &str = "bad json";

const BAD_NUM: &str = "bad number";

pub fn json_first(val: &JsonVal) -> Res<JsonVal> {
    match val {
        JsonVal::Number(val) => Ok(JsonVal::Number(val.clone())),
        JsonVal::Array(ref arr) => json_arr_first(arr),
        val => Ok(val.clone()),
    }
}

pub fn json_last(val: &JsonVal) -> Res<JsonVal> {
    match val {
        JsonVal::Number(val) => Ok(JsonVal::Number(val.clone())),
        JsonVal::Array(ref arr) => json_arr_last(arr),
        val => Ok(val.clone()),
    }
}

pub fn json_sum(val: &JsonVal) -> Res<JsonVal> {
    match val {
        JsonVal::Number(val) => Ok(JsonVal::Number(val.clone())),
        JsonVal::Array(ref arr) => json_arr_sum(arr),
        val => Ok(val.clone()),
    }
}

pub fn json_avg(val: &JsonVal) -> Res<JsonVal> {
    match val {
        JsonVal::Number(val) => Ok(JsonVal::Number(val.clone())),
        JsonVal::Array(ref arr) => json_arr_avg(arr),
        val => Ok(val.clone()),
    }
}

pub fn json_var(val: &JsonVal) -> Res<JsonVal> {
    match val {
        JsonVal::Number(val) => Ok(JsonVal::Number(val.clone())),
        JsonVal::Array(ref arr) => json_arr_var(arr),
        val => Ok(val.clone()),
    }
}

pub fn json_dev(val: &JsonVal) -> Res<JsonVal> {
    match val {
        JsonVal::Number(val) => Ok(JsonVal::Number(val.clone())),
        JsonVal::Array(ref arr) => json_arr_dev(arr),
        val => Ok(val.clone()),
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

//TODO(jaupe) add more cases
fn json_add(lhs: &JsonVal, rhs: &JsonVal) -> Res<JsonVal> {
    match (lhs, rhs) {
        (JsonVal::Array(lhs), JsonVal::Array(rhs)) => json_add_arrs(lhs, rhs),
        (JsonVal::Array(lhs), JsonVal::Number(rhs)) => json_add_arr_num(lhs, rhs),
        (JsonVal::Number(rhs), JsonVal::Array(lhs)) => json_add_arr_num(lhs, rhs),
        (JsonVal::Number(lhs), JsonVal::Number(rhs)) => json_add_nums(lhs, rhs),
        (JsonVal::String(lhs), JsonVal::String(rhs)) => json_add_str(lhs, rhs),    
        (JsonVal::String(lhs), JsonVal::Array(rhs)) => add_str_arr(lhs, rhs),
        (JsonVal::Array(lhs), JsonVal::String(rhs)) => add_arr_str(lhs, rhs),
        _ => Err(BAD_TYPE),
    }
}

fn json_sub(lhs: &JsonVal, rhs: &JsonVal) -> Res<JsonVal> {
    match (lhs, rhs) {
        (JsonVal::Array(lhs), JsonVal::Array(rhs)) => json_sub_arrs(lhs, rhs),
        (JsonVal::Array(lhs), JsonVal::Number(rhs)) => json_sub_arr_num(lhs, rhs),
        (JsonVal::Number(lhs), JsonVal::Array(rhs)) => json_sub_num_arr(lhs, rhs),
        (JsonVal::Number(lhs), JsonVal::Number(rhs)) => json_sub_nums(lhs, rhs),
        _ => Err(BAD_TYPE),
    }
}

fn json_mul(lhs: &JsonVal, rhs: &JsonVal) -> Res<JsonVal> {
    match (lhs, rhs) {
        (JsonVal::Array(x), JsonVal::Array(y)) => mul_arrs(x, y),
        (JsonVal::Array(x), JsonVal::Number(y)) => mul_arr_num(x, y),
        (JsonVal::Number(x), JsonVal::Array(y)) => mul_arr_num(y, x),
        (JsonVal::Number(x), JsonVal::Number(y)) => mul_nums(x, y),
        _ => Err(BAD_TYPE),
    }
}

fn mul_vals(x: &JsonVal, y: &JsonVal) -> Res<JsonVal> {
    match (x, y) {
        (JsonVal::Number(x), JsonVal::Number(y)) => mul_nums(x, y),
        _ => Err(BAD_TYPE),
    }
}

fn mul_nums(x: &JsonNum, y: &JsonNum) -> Res<JsonVal> {
    let val = x.as_f64().unwrap() * y.as_f64().unwrap();
    Ok(JsonVal::from(val))
}

fn mul_arr_num(x: &[JsonVal], y: &JsonNum) -> Res<JsonVal> {
    let mut arr = Vec::new();
    for x in x.iter() {
        arr.push(mul_val_num(x, y)?);
    }
    Ok(JsonVal::from(arr))
}

fn mul_val_num(x: &JsonVal, y: &JsonNum) -> Res<JsonVal> {
    match x {
        JsonVal::Number(ref x) => mul_nums(x, y),
        JsonVal::Array(ref arr) => mul_arr_num(arr, y),
        _ => Err(BAD_JSON),
    }
}

//TODO(jaupe) optimize by removing the temp allocs
fn mul_arrs(lhs: &[JsonVal], rhs: &[JsonVal]) -> Res<JsonVal> {
    let mut arr = Vec::new();
    for (x, y) in lhs.iter().zip(rhs.iter()) {
        arr.push(mul_vals(x, y)?);
    }
    Ok(JsonVal::from(arr))
}

fn json_div(lhs: &JsonVal, rhs: &JsonVal) -> Res<JsonVal> {
    println!("{:?}, {:?}", lhs, rhs);
    match (lhs, rhs) {
        (JsonVal::Array(ref lhs), JsonVal::Array(ref rhs)) => div_arrs(lhs, rhs),
        (JsonVal::Array(ref lhs), JsonVal::Number(ref rhs)) => div_arr_num(lhs, rhs),
        (JsonVal::Number(ref lhs), JsonVal::Array(ref rhs)) => div_num_arr(lhs, rhs),
        (JsonVal::Number(ref lhs), JsonVal::Number(ref rhs)) => div_nums(lhs, rhs),
        _ => Err(BAD_TYPE),
    }
}

fn div_nums(x: &JsonNum, y: &JsonNum) -> Res<JsonVal> {
    let val = x.as_f64().unwrap() / y.as_f64().unwrap();
    Ok(JsonVal::from(val))
}

fn div_arrs(x: &[JsonVal], y: &[JsonVal]) -> Res<JsonVal> {
    let mut arr = Vec::new();
    for (x, y) in x.iter().zip(y.iter()) {
        arr.push(json_div(x, y)?);
    }
    Ok(JsonVal::from(arr))
}

fn div_arr_num(x: &[JsonVal], y: &JsonNum) -> Res<JsonVal> {
    let mut arr = Vec::new();
    for x in x {
        arr.push(div_val_num(x, y)?);
    }
    Ok(JsonVal::from(arr))
}

fn div_val_num(x: &JsonVal, y: &JsonNum) -> Res<JsonVal> {
    match x {
        JsonVal::Number(ref x) => div_nums(x, y),
        JsonVal::Array(ref x) => div_arr_num(x, y),
        _ => Err(BAD_TYPE),
    }
}

fn div_num_arr(x: &JsonNum, y: &[JsonVal]) -> Res<JsonVal> {
    let mut arr = Vec::new();
    for y in y {
        arr.push(div_num_val(x, y)?);
    }
    Ok(JsonVal::from(arr))
}

fn div_num_val(x: &JsonNum, y: &JsonVal) -> Res<JsonVal> {
    match y {
        JsonVal::Number(ref y) => div_nums(x, y),
        JsonVal::Array(ref y) => div_num_arr(x, y),
        _ => Err(BAD_TYPE),
    }
}

fn json_add_str(x: &str, y: &str) -> Res<JsonVal> {
    let val = x.to_string() + y;
    Ok(JsonVal::String(val))
}

fn add_str_arr(x: &str, y: &[JsonVal]) -> Res<JsonVal> {
    let mut arr = Vec::with_capacity(y.len());
    for e in y {
        arr.push(add_str_val(x, e)?);
    }
    Ok(JsonVal::Array(arr))
}

fn add_str_val(x: &str, y: &JsonVal) -> Res<JsonVal> {
    match y {
        JsonVal::String(y) => Ok(JsonVal::from(x.to_string() + y)),
        _ => Err(BAD_TYPE),
    }
}

fn add_val_str(x: &JsonVal, y: &str) -> Res<JsonVal> {
    match x {
        JsonVal::String(x) => Ok(JsonVal::from(x.to_string() + y)),
        _ => Err(BAD_TYPE),
    }
}


fn add_arr_str(lhs: &[JsonVal], rhs: &str) -> Res<JsonVal> {
    let mut arr = Vec::with_capacity(lhs.len());
    for x in lhs {
        arr.push(add_val_str(x, rhs)?);
    }
    Ok(JsonVal::Array(arr))
}

//TODO(jaupe) add better error handlinge
fn json_add_arr_num(x: &[JsonVal], y: &JsonNum) -> Res<JsonVal> {
    let arr: Vec<JsonVal> = x
        .iter()
        .map(|x| JsonVal::from(x.as_f64().unwrap() + y.as_f64().unwrap()))
        .collect();
    Ok(JsonVal::Array(arr))
}

fn json_add_arrs<'a>(lhs: &[JsonVal], rhs: &[JsonVal]) -> Res<JsonVal> {
    let vec = lhs
        .iter()
        .zip(rhs.iter())
        .map(|(x, y)| json_add(x, y).unwrap())
        .collect();
    Ok(JsonVal::Array(vec))
}

fn json_add_nums(x: &JsonNum, y: &JsonNum) -> Res<JsonVal> {
    let val = x.as_f64().unwrap() + y.as_f64().unwrap();
    Ok(JsonVal::from(val))
}

fn json_sub_arr_num(x: &[JsonVal], y: &JsonNum) -> Res<JsonVal> {
    let arr = x
        .iter()
        .map(|x| JsonVal::from(x.as_f64().unwrap() - y.as_f64().unwrap()))
        .collect();
    Ok(JsonVal::Array(arr))
}

fn json_sub_num_arr(x: &JsonNum, y: &[JsonVal]) -> Res<JsonVal> {
    let arr = y
        .iter()
        .map(|y| JsonVal::from(x.as_f64().unwrap() - y.as_f64().unwrap()))
        .collect();
    Ok(JsonVal::Array(arr))
}

fn json_sub_arrs<'a>(lhs: &[JsonVal], rhs: &[JsonVal]) -> Res<JsonVal> {
    let vec = lhs
        .iter()
        .zip(rhs.iter())
        .map(|(x, y)| json_sub(x, y).unwrap())
        .collect();
    Ok(JsonVal::Array(vec))
}

fn json_sub_nums(x: &JsonNum, y: &JsonNum) -> Res<JsonVal> {
    let val = x.as_f64().unwrap() - y.as_f64().unwrap();
    Ok(JsonVal::from(val))
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
            _ => return Err(BAD_NUM),
        }
    }
    let num = Number::from_f64(total).ok_or(BAD_NUM)?;
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
        Ok(s[s.len() - 1].clone())
    }
}

fn json_arr_avg(s: &[JsonVal]) -> Res<JsonVal> {
    let mut total = 0.0f64;
    for val in s {
        total += json_f64(val).ok_or(BAD_NUM)?;
    }
    let val = total / (s.len() as f64);
    let num = Number::from_f64(val).ok_or(BAD_NUM)?;
    Ok(JsonVal::Number(num))
}

fn json_arr_var(s: &[JsonVal]) -> Res<JsonVal> {
    let mut sum = 0.0f64;
    for val in s {
        sum += json_f64(val).ok_or(BAD_NUM)?;
    }
    let mean = sum / ((s.len() - 1) as f64);
    let mut var = 0.0f64;
    for val in s {
        var += (json_f64(val).ok_or(BAD_NUM)? - mean).powf(2.0);
    }
    var /= (s.len()) as f64;
    let num = Number::from_f64(var).ok_or(BAD_NUM)?;
    Ok(JsonVal::Number(num))
}

fn json_arr_dev(s: &[JsonVal]) -> Res<JsonVal> {
    let mut sum = 0.0f64;
    for val in s {
        sum += json_f64(val).ok_or(BAD_NUM)?;
    }
    let avg = sum / (s.len() as f64);
    let mut var = 0.0f64;
    for val in s {
        var += (json_f64(val).ok_or(BAD_NUM)? - avg).powf(2.0);
    }
    var /= s.len() as f64;
    let num = Number::from_f64(var.sqrt()).ok_or(BAD_NUM)?;
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
            _ => return Err("bad type"),
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
            _ => return Err("bad type"),
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
    #[serde(rename = "avg")]
    Avg(Box<Cmd>),
    #[serde(rename = "dev")]
    Dev(Box<Cmd>),
    #[serde(rename = "var")]
    Var(Box<Cmd>),
    #[serde(rename = "first")]
    First(Box<Cmd>),
    #[serde(rename = "last")]
    Last(Box<Cmd>),
    #[serde(rename = "del")]
    Del(String),
    Val(JsonVal),
    #[serde(rename = "+")]
    Add(Box<Cmd>, Box<Cmd>),
    #[serde(rename = "-")]
    Sub(Box<Cmd>, Box<Cmd>),
    #[serde(rename = "*")]
    Mul(Box<Cmd>, Box<Cmd>),
    #[serde(rename = "/")]
    Div(Box<Cmd>, Box<Cmd>),
}

pub fn parse_json_str<S: Into<String>>(s: S) -> Res<Cmd> {
    let json_val = serde_json::from_str(&s.into()).map_err(|_| BAD_JSON)?;
    parse_json_val(json_val)
}

fn parse_json_val(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Object(obj) => parse_obj(obj),
        val => Ok(Cmd::Val(val)),
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
            "+" => return parse_add(val),
            "-" => return parse_sub(val),
            "*" => return parse_mul(val),
            "/" => return parse_div(val),
            _ => unimplemented!(),
        }
    }
    Ok(Cmd::Set("k1".to_string(), JsonVal::Bool(true)))
}

fn parse_min(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Object(obj) => Ok(Cmd::Min(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Min(Box::new(Cmd::Val(val)))),
    }
}

fn parse_max(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Object(obj) => Ok(Cmd::Max(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Max(Box::new(Cmd::Val(val)))),
    }
}

fn parse_avg(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Object(obj) => Ok(Cmd::Avg(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Sum(Box::new(Cmd::Val(val)))),
    }
}

fn parse_var(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Object(obj) => Ok(Cmd::Var(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Sum(Box::new(Cmd::Val(val)))),
    }
}

fn parse_dev(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Object(obj) => Ok(Cmd::Dev(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Sum(Box::new(Cmd::Val(val)))),
    }
}

fn parse_sum(val: JsonVal) -> Res<Cmd> {
    match val {
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
        _ => unimplemented!(),
    }
}

fn parse_first(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Object(obj) => Ok(Cmd::First(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::First(Box::new(Cmd::Val(val)))),
    }
}

fn parse_last(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Object(obj) => Ok(Cmd::Last(Box::new(parse_obj(obj)?))),
        val => Ok(Cmd::Last(Box::new(Cmd::Val(val)))),
    }
}

fn parse_add(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Array(mut arr) => {
            let rhs = parse_json_val(arr.remove(1))?;
            let lhs = parse_json_val(arr.remove(0))?;
            Ok(Cmd::Add(Box::new(lhs), Box::new(rhs)))
        }
        _ => unimplemented!(),
    }
}

fn parse_sub(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Array(mut arr) => {
            let rhs = parse_json_val(arr.remove(1))?;
            let lhs = parse_json_val(arr.remove(0))?;
            Ok(Cmd::Sub(Box::new(lhs), Box::new(rhs)))
        }
        _ => unimplemented!(),
    }
}

fn parse_mul(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Array(mut arr) => {
            let rhs = parse_json_val(arr.remove(1))?;
            let lhs = parse_json_val(arr.remove(0))?;
            Ok(Cmd::Mul(Box::new(lhs), Box::new(rhs)))
        }
        _ => unimplemented!(),
    }
}

fn parse_div(val: JsonVal) -> Res<Cmd> {
    match val {
        JsonVal::Array(mut arr) => {
            let rhs = parse_json_val(arr.remove(1))?;
            let lhs = parse_json_val(arr.remove(0))?;
            Ok(Cmd::Div(Box::new(lhs), Box::new(rhs)))
        }
        _ => unimplemented!(),
    }
}

pub fn eval_json_cmd(cmd: Cmd, db: &mut Database) -> Res<JsonVal> {
    match cmd {
        Cmd::Get(ref key) => db.get(key).map(|x| x.clone()).ok_or(BAD_KEY),
        Cmd::Del(ref key) => match db.del(key) {
            Ok(Some(val)) => Ok(val),
            Ok(None) => Ok(JsonVal::Null),
            Err(_) => Err(BAD_IO),
        },
        Cmd::Set(key, val) => db_write(db, key, val),
        Cmd::Sum(arg) => eval_sum(*arg, db),
        Cmd::Min(arg) => eval_min(*arg, db),
        Cmd::Max(arg) => eval_max(*arg, db),
        Cmd::Val(val) => Ok(val),
        Cmd::Avg(arg) => eval_avg(*arg, db),
        Cmd::Dev(arg) => eval_dev(*arg, db),
        Cmd::Var(arg) => eval_var(*arg, db),
        Cmd::First(arg) => eval_first(*arg, db),
        Cmd::Last(arg) => eval_last(*arg, db),
        Cmd::Add(lhs, rhs) => eval_add(*lhs, *rhs, db),
        Cmd::Sub(lhs, rhs) => eval_sub(*lhs, *rhs, db),
        Cmd::Mul(lhs, rhs) => eval_mul(*lhs, *rhs, db),
        Cmd::Div(lhs, rhs) => eval_div(*lhs, *rhs, db),
    }
}

fn db_write(db: &mut Database, key: String, val: JsonVal) -> Res<JsonVal> {
    match db.set(key, val) {
        Ok(Some(val)) => Ok(val),
        Ok(None) => Ok(JsonVal::Null),
        Err(_) => Err(BAD_WRITE),
    }
}

fn eval_sum(arg: Cmd, db: &mut Database) -> Res<JsonVal> {
    match eval_json_cmd(arg, db) {
        Ok(ref val) => json_sum(val),
        Err(err) => Err(err),
    }
}

fn eval_avg(arg: Cmd, db: &mut Database) -> Res<JsonVal> {
    match eval_json_cmd(arg, db) {
        Ok(ref val) => json_avg(val),
        Err(err) => Err(err),
    }
}

fn eval_dev(arg: Cmd, db: &mut Database) -> Res<JsonVal> {
    match eval_json_cmd(arg, db) {
        Ok(ref val) => json_dev(val),
        Err(err) => Err(err),
    }
}

fn eval_var(arg: Cmd, db: &mut Database) -> Res<JsonVal> {
    match eval_json_cmd(arg, db) {
        Ok(ref val) => json_var(val),
        Err(err) => Err(err),
    }
}

fn eval_first(arg: Cmd, db: &mut Database) -> Res<JsonVal> {
    match eval_json_cmd(arg, db) {
        Ok(ref val) => json_first(val),
        Err(err) => Err(err),
    }
}

fn eval_add(lhs: Cmd, rhs: Cmd, db: &mut Database) -> Res<JsonVal> {
    let x = eval_json_cmd(lhs, db)?;
    let y = eval_json_cmd(rhs, db)?;
    json_add(&x, &y)
}

fn eval_sub(lhs: Cmd, rhs: Cmd, db: &mut Database) -> Res<JsonVal> {
    let x = eval_json_cmd(lhs, db)?;
    let y = eval_json_cmd(rhs, db)?;
    json_sub(&x, &y)
}

fn eval_mul(lhs: Cmd, rhs: Cmd, db: &mut Database) -> Res<JsonVal> {
    let x = eval_json_cmd(lhs, db)?;
    let y = eval_json_cmd(rhs, db)?;
    json_mul(&x, &y)
}

fn eval_div(lhs: Cmd, rhs: Cmd, db: &mut Database) -> Res<JsonVal> {
    let x = eval_json_cmd(lhs, db)?;
    let y = eval_json_cmd(rhs, db)?;
    json_div(&x, &y)
}

fn eval_last(arg: Cmd, db: &mut Database) -> Res<JsonVal> {
    match eval_json_cmd(arg, db) {
        Ok(ref val) => json_last(val),
        Err(err) => Err(err),
    }
}

fn eval_max(arg: Cmd, db: &mut Database) -> Res<JsonVal> {
    match eval_json_cmd(arg, db) {
        Ok(ref val) => json_max(val),
        Err(err) => Err(err),
    }
}

fn eval_min(arg: Cmd, db: &mut Database) -> Res<JsonVal> {
    match eval_json_cmd(arg, db) {
        Ok(ref val) => json_min(val),
        Err(err) => Err(err),
    }
}
