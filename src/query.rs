use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Query {
    selects: Vec<Expr>,
    from: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Expr {
    Get(String),
    Sum(Box<Expr>),
    Max(Box<Expr>),
    Min(Box<Expr>),
}
