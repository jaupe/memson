use serde_json::Value as JsonVal;
use std::collections::BTreeMap;

type Res<T> = Result<T, &'static str>;
type CmdRes<'a> = Result<Val<'a>, &'static str>;

#[derive(Debug, PartialEq)]
pub enum Val<'a> {
    Ref(&'a JsonVal),
    Val(JsonVal),
    Null,
}

pub enum Cmd {
    Get(String),
    Set(String, JsonVal),
    Sum(Box<Cmd>),
}

type Cache = BTreeMap<String, JsonVal>;

pub struct Db {
    cache: Cache,
}

impl Db {
    fn new() -> Db {
        Db {
            cache: Cache::new(),
        }
    }

    fn eval<'a>(&'a mut self, cmd: Cmd) -> Result<Val<'a>, &'static str> {
        match cmd {
            Cmd::Get(ref key) => {
                self.cache.get(key).ok_or("bad key").map(Val::Ref)
            }
            Cmd::Set(key, val) => {
                let old_val = self.cache.insert(key, val);
                match old_val {
                    Some(v) => Ok(Val::Val(v)),
                    None => Ok(Val::Null),
                }
            }
            Cmd::Sum(arg) => {
                let val = self.eval(*arg)?;
                sum_val(&val)
            }
        }
    }
}

fn sum_val<'b>(val: &Val) -> Result<Val<'b>, &'static str> {
    match val {
        Val::Val(ref v) => sum_json(&v),
        Val::Ref(v) => sum_json(v),
        Val::Null => Err("sum null"),
    }
}

fn sum_json<'a>(val: &JsonVal) -> Result<Val<'a>, &'static str> {
    match val {
        JsonVal::Array(arr) => sum_json_arr(arr),
        JsonVal::Bool(val) => {
            let v = if *val { JsonVal::from(1) } else { JsonVal::from(0) };
            Ok(Val::Val(v))
        }
        JsonVal::Number(val) => Ok(Val::Val(JsonVal::Number(val.clone()))),
        _ => unimplemented!(),
    }
}

fn sum_json_arr<'a>(arr: &[JsonVal]) -> CmdRes<'a> {
    let mut total = 0.0;
    for val in arr {
        total += json_f64(val)?;
    }
    Ok(Val::Val(JsonVal::from(total)))
}

fn json_f64(val: &JsonVal) -> Res<f64> {
    match val {
        JsonVal::Number(num) => num.as_f64().ok_or("bad num"),
        _ => unimplemented!(),
    }
} 

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    fn set<S:Into<String>>(key: S, val: JsonVal) -> Cmd {
        Cmd::Set(key.into(), val)
    }

    fn get<S:Into<String>>(key: S) -> Cmd {
        Cmd::Get(key.into())
    }

    fn sum(arg: Cmd) -> Cmd {
        Cmd::Sum(Box::new(arg))
    }

    #[test]
    fn test_db_new() {
        let db = Db::new();
        assert!(db.cache.is_empty());
    }

    #[test]
    fn test_db_put_get() {
        let mut db = Db::new();
        let key = "k1";
        let val = JsonVal::String("foobar".to_string());

        let r = db.eval(set(key, val.clone()));
        assert_eq!(Ok(Val::Null), r);
        
        let r = db.eval(get(key));
        let v = Val::Ref(&val);
        assert_eq!(Ok(v), r);
    }

    #[test]
    fn test_db_sum_arr_nums() {
        let mut db = Db::new();
        let key = "k1";
        let arr = vec![
            JsonVal::from(1.0),
            JsonVal::from(1.0),
            JsonVal::from(1.0),
            JsonVal::from(1.0),
            JsonVal::from(1.0),
        ];
        db.eval(set(key, JsonVal::Array(arr))).unwrap();

        assert_eq!(Ok(Val::Val(JsonVal::from(5.0))), db.eval(sum(get(key))));
    }

}
