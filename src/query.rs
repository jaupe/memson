use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonVal};

use crate::db::{Database, Table};
use crate::{Res, Row};
use std::fmt;
use serde::export::Formatter;

macro_rules! map (
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
        let val = eval_expr(&self.selects[0], tbl)?;
        Ok(vec![map! {self.selects[0] => val}])
    }
}

fn eval_expr(expr: &Expr, tbl: &Table) -> Res<JsonVal> {
    match expr {
        Expr::Sum(box arg) => {
            match arg {
                Expr::Get(key) => eval_sum(tbl, key),
                _ => unimplemented!(),
            }
        }
        _ => unimplemented!(),
    }
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