use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonVal};

use crate::db::{Database, Table};
use crate::{Res, Row};
use std::fmt;
use serde::export::Formatter;

macro_rules! row(
    { $($key:expr => $value:expr),+ } => {
        {
            let mut m = Map::new();
            $(
                m.insert($key.to_string(), JsonVal::from($value));
            )+
            m
        }
     };
);

#[derive(Debug, Deserialize, Serialize)]
pub struct Query {
    selects: Vec<Expr>,
    from: String,
}

impl Query {
    pub fn from(selects: Vec<Expr>, from: String) -> Self {
        Self { selects, from }
    }

    pub fn exec(&self, db: &Database) -> Res<Vec<Row>> {
        let tbl = db.find_table(&self.from).ok_or("cannot find table")?;
        let mut rows = eval_rows(&self.selects, tbl)?;
        if rows.is_empty() {
            let mut row = Map::new();
            eval_aggregations(&self.selects, &mut row, tbl.rows())?;
            rows.push(row);
        } else {
            eval_aggregations(&self.selects, &mut rows[0], tbl.rows())?;
        }

        Ok(rows)
    }
}

fn eval_aggregations(selects: &[Expr], out: &mut Row, rows: &[Row]) -> Res<()> {
    for select in selects {
        match select {
            Expr::Sum(box Expr::Get(key)) => { let mut total = 0.0;
               for row in rows {
                   if let Some(JsonVal::Number(val)) = row.get(key) {
                       if let Some(val) = val.as_f64() {
                           total += val;
                       }
                   }
               }
                out.insert(select.to_string(), JsonVal::from(total));
            }
            _ => continue,
        }
    }
    Ok(())
}

fn eval_rows(selects: &[Expr], tbl: &Table) -> Res<Vec<Row>> {
    let mut rows = Vec::new();
    for row in tbl.rows() {
        let row = eval_row(selects, row)?;
        if !row.is_empty() {
            rows.push(row);
        }
    }
    Ok(rows)
}

fn eval_row(selects: &[Expr], row: &Row) -> Res<Row> {
    let mut obj = Map::new();
    for select in selects {
        match select {
            Expr::Get(key) => {
                if let Some(val) = row.get(key) {
                    obj.insert(select.to_string(), val.clone());
                }
            }
            _ => continue,
        }
    }
    Ok(obj)
}

fn eval_sum(tbl: &Table, key: &str) -> Res<JsonVal> {
    let mut sum = 0.0;
    // TODO(jaupe) parallelize this
    for row in tbl.rows() {
        if let Some(val) = json_f64(row, key) {
            sum += val;
        }
    }
    Ok(JsonVal::from(sum))
}

fn json_f64(row: &Row, key: &str) -> Option<f64> {
    if let Some(JsonVal::Number(num)) = row.get(key) {
        num.as_f64()
    } else {
        None
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Expr {
    Get(String),
    Sum(Box<Expr>),
    Max(Box<Expr>),
    Min(Box<Expr>),
}

// FIXME(jaupe) add patterns for the rest of expressions
impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Get(ref name) => write!(f, "{}", name),
            Expr::Sum(box arg) => {
                write!(f, "sum(")?;
                arg.fmt(f)?;
                write!(f, ")")
            },
            _ => unimplemented!()
        }
    }
}


#[cfg(test)]
mod tests {
    use std::fs::remove_file;

    use serde_json::Map;

    use super::*;
    use crate::Row;
    use crate::obj;
    use crate::db::Cmd;

    fn add<S: Into<String>>(x: S, y: S) -> String {
        "{\"+\":[".to_string() + &x.into() + "," + &y.into() + "]}"
    }

    fn sub<S: Into<String>>(x: S, y: S) -> String {
        "{\"-\":[".to_string() + &x.into() + "," + &y.into() + "]}"
    }

    fn mul<S: Into<String>>(x: S, y: S) -> String {
        "{\"*\":[".to_string() + &x.into() + "," + &y.into() + "]}"
    }

    fn div<S: Into<String>>(x: S, y: S) -> String {
        "{\"/\":[".to_string() + &x.into() + "," + &y.into() + "]}"
    }

    fn get<S: Into<String>>(arg: S) -> String {
        "{\"get\":".to_string() + "\"" + &arg.into() + "\"" + "}"
    }

    fn first<S: Into<String>>(arg: S) -> String {
        json_fn("first", arg)
    }

    fn last<S: Into<String>>(arg: S) -> String {
        json_fn("last", arg)
    }

    fn max<S: Into<String>>(arg: S) -> String {
        json_fn("max", arg)
    }

    fn min<S: Into<String>>(arg: S) -> String {
        json_fn("min", arg)
    }

    fn avg<S: Into<String>>(arg: S) -> String {
        json_fn("avg", arg)
    }

    fn var<S: Into<String>>(arg: S) -> String {
        json_fn("var", arg)
    }

    fn dev<S: Into<String>>(arg: S) -> String {
        json_fn("dev", arg)
    }

    fn json_fn<S: Into<String>>(f: &str, arg: S) -> String {
        "{\"".to_string() + f + "\":" + &arg.into() + "}"
    }

    fn test_db() -> Database {
        unimplemented!()
    }

    fn json_f64(v: &JsonVal) -> f64 {
        v.as_f64().unwrap()
    }

    fn eval<'a, S: Into<String>>(db: &'a mut Database, line: S) -> Res<JsonVal> {
        db.eval(line)
    }

    fn db_get(db: &mut Database, key: &str) -> Res<JsonVal> {
        db.eval(get(key))
    }

    fn bad_type() -> Res<JsonVal> {
        Err("bad type")
    }


    #[test]
    fn select_sum_ok() {
        let mut db = Database::open("./", "t5").unwrap();
        // create table
        let cmd = Cmd::Insert(
            "p".to_string(),
            vec![row! {"price" => 1}, row! {"price" => 2}, row! {"price" => 3}],
        );
        db.eval_cmd(cmd).unwrap();
        let expr = Expr::Sum(Box::new(Expr::Get("price".to_string())));
        let qry = Query::from(vec![expr], "p".to_string());
        let res = qry.exec(&db).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0], obj! {"sum(price)" => 6.0});

        remove_file("./t5.db").unwrap();
        remove_file("./p.table").unwrap();
    }

    #[test]
    fn select_get_ok() {
        let mut db = Database::open("./", "t6").unwrap();
        // create table
        let cmd = Cmd::Insert(
            "prices".to_string(),
            vec![row! {"price" => 1}, row! {"price" => 2}, row! {"price" => 3}],
        );
        db.eval_cmd(cmd).unwrap();
        let expr = Expr::Get("price".to_string());
        let qry = Query::from(vec![expr], "prices".to_string());
        let res = qry.exec(&db).unwrap();
        assert_eq!(res.len(), 3);
        assert_eq!(res[0], obj! {"price" => 1});
        assert_eq!(res[1], obj! {"price" => 2});
        assert_eq!(res[2], obj! {"price" => 3});

        remove_file("./t6.db").unwrap();
        remove_file("./prices.table").unwrap();
    }
}
