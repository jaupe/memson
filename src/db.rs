use crate::json::*;
use crate::log::*;
use crate::query::Query;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonVal;
use std::collections::BTreeMap;
use std::fs;
use std::io::{self};
use std::path::Path;
use std::path::PathBuf;

#[derive(Debug, Deserialize, Serialize)]
pub enum Cmd {
    Insert(String, Vec<JsonVal>),
    Delete(String),
    Query(Query),
}

#[derive(Debug)]
pub struct Table {
    name: String,
    rows: Vec<JsonVal>,
    log: ReplayLog,
}

impl Table {
    pub fn new<S: Into<String>, P: AsRef<Path>>(name: S, path: P, rows: Vec<JsonVal>) -> io::Result<Self> {
        let mut path_buf = PathBuf::new();
        path_buf.push(path);
        let name = name.into();
        path_buf.push(name.clone() + ".table");
        let log = ReplayLog::new(path_buf, &rows)?;
        Ok(Table {
            name: name.into(),
            rows,
            log,
        })
    }

    pub fn open<S: Into<String>, P: AsRef<Path>>(name: S, path: P) -> Res<Self> {
        let mut log = ReplayLog::open(path).map_err(|_| "cannot open replay log")?;
        let rows = log.replay()?;
        Ok(Table {
            name: name.into(),
            rows,
            log,
        })
    }

    pub fn from<S: Into<String>>(name: S, rows: Vec<JsonVal>, log: ReplayLog) -> Self {
        Self {
            name: name.into(),
            rows,
            log,
        }
    }

    pub fn insert(&mut self, rows: Vec<JsonVal>) -> io::Result<()> {
        self.log.insert(&rows)?;
        self.rows.extend(rows);
        Ok(())
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }
}

// Type wrapper
pub type Cache = BTreeMap<String, Table>;

/// The in-memory database shared amongst all clients.
///
/// This database will be shared via `Arc`, so to mutate the internal map we're
/// going to use a `Mutex` for interior mutability.
#[derive(Debug)]
pub struct Database {
    root_path: PathBuf,
    tables: Vec<Table>,
    log: DbConfig,
}

impl Database {
    pub fn open<P: AsRef<Path>, S: Into<String>>(path: P, name: S) -> Res<Database> {
        let mut root_path = PathBuf::new();
        root_path.push(path);
        let mut log = DbConfig::open(&root_path, name).map_err(|_| "cannot open db config file")?;
        let tables = log.load()?;
        Ok(Database {
            root_path,
            tables,
            log,
        })
    }

    pub fn insert(&mut self, table: Table) -> io::Result<()> {
        self.log.insert(table.name())?;
        self.tables.push(table);
        Ok(())
    }

    pub fn delete_table(&mut self, tbl_name: &str) -> io::Result<bool> {
        self.log.remove_table(tbl_name)?;
        let found = self.tables.iter().position(|t| t.name() == tbl_name);
        Ok(match found {
            Some(index) => {
                self.tables.remove(index);
                let mut path = self.root_path.clone();
                path.push(tbl_name.to_string() + ".table");
                fs::remove_file(path)?;
                true
            }
            None => false,
        })
    }

    pub fn eval_cmd(&mut self, cmd: Cmd) -> Res<()> {
        match cmd {
            Cmd::Insert(name, rows) => {
                self.insert_table(name, rows).map_err(|_| "cannot insert")?;
                Ok(())
            }
            Cmd::Delete(name) => {
                self.delete_table(&name).map_err(|_| "cannot delete table")?;
                Ok(())
            }
            _ => unimplemented!(),
        }
    }

    pub fn eval<S: Into<String>>(&mut self, line: S) -> Res<JsonVal> {
        let line = line.into();
        unimplemented!()
    }

    pub fn find_table(&mut self, name: &str) -> Option<&mut Table> {
        self.tables.iter_mut().find(|x| x.name() == name)
    }

    pub fn insert_table(&mut self, name: String, rows: Vec<JsonVal>) -> io::Result<()> {
        let r = self.find_table(&name);
        match r {
            Some(tbl) => {
                tbl.insert(rows)
            }
            None => {
                let tbl = Table::new(name, self.root_path.clone(), rows)?;
                self.tables.push(tbl);
                Ok(())
            }
        }
    }

    pub fn table_exits(&self, name: &str) -> Option<usize> {
        self.tables.iter().position(|t| t.name() == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::remove_file;

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
    fn insert_new_table_ok() {
        let mut db = Database::open("./", "test").unwrap();
        let cmd = Cmd::Insert(
            "t".to_string(),
            vec![JsonVal::from(1), JsonVal::from(2.1), JsonVal::from("s")],
        );
        db.eval_cmd(cmd).unwrap();
        assert_eq!(db.tables.len(), 1);
        let tbl = &db.tables[0];
        assert_eq!(tbl.name(), "t");
        assert_eq!(tbl.len(), 3);
        assert_eq!(tbl.rows[0], JsonVal::from(1));
        assert_eq!(tbl.rows[1], JsonVal::from(2.1));
        assert_eq!(tbl.rows[2], JsonVal::from("s"));

        remove_file("./test.db").unwrap();
        remove_file("./t.table").unwrap();
    }

    #[test]
    fn delete_table_ok() {
        // populate db with test table
        let mut db = Database::open("./", "test3").unwrap();
        db.eval_cmd(Cmd::Insert(
            "foo".to_string(),
            vec![JsonVal::from(1), JsonVal::from(2.1), JsonVal::from("s")],
        ))
        .unwrap();

        assert_eq!(db.tables.len(), 1);
        // delete table
        db.eval_cmd(Cmd::Delete("foo".to_string())).unwrap();
        // assert state
        assert_eq!(db.tables.len(), 0);
        remove_file("./test3.db").unwrap();
    }

    #[test]
    fn append_to_table_ok() {
        let mut db = Database::open("./", "append").unwrap();
        // create table
        let cmd = Cmd::Insert(
            "append".to_string(),
            vec![JsonVal::from(1), JsonVal::from(2.1), JsonVal::from("s")],
        );
        db.eval_cmd(cmd).unwrap();
        // append data to table
        let cmd = Cmd::Insert(
            "append".to_string(),
            vec![JsonVal::from(2), JsonVal::from(3.1), JsonVal::from("t")],
        );
        db.eval_cmd(cmd).unwrap();
        assert_eq!(db.tables.len(), 1);
        let tbl = &db.tables[0];
        assert_eq!(tbl.name(), "append");
        assert_eq!(tbl.len(), 6);
        assert_eq!(tbl.rows[0], JsonVal::from(1));
        assert_eq!(tbl.rows[1], JsonVal::from(2.1));
        assert_eq!(tbl.rows[2], JsonVal::from("s"));
        assert_eq!(tbl.rows[3], JsonVal::from(2));
        assert_eq!(tbl.rows[4], JsonVal::from(3.1));
        assert_eq!(tbl.rows[5], JsonVal::from("t"));

        remove_file("./append.db").unwrap();
        remove_file("./append.table").unwrap();
    }

    /*
    #[test]
    fn open_db() {
        let mut db = test_db();
        assert_eq!(10, db.cache.len());
        assert_eq!(db_get(&mut db, "b"), Ok(JsonVal::Bool(true)));
        assert_eq!(
            db_get(&mut db, "ia"),
            Ok(JsonVal::Array(vec![
                JsonVal::from(1),
                JsonVal::from(2),
                JsonVal::from(3),
                JsonVal::from(4),
                JsonVal::from(5),
            ]))
        );
        assert_eq!(db_get(&mut db, "i"), Ok(JsonVal::from(3)));
        assert_eq!(db_get(&mut db, "f"), Ok(JsonVal::from(3.3)));
        assert_eq!(
            db_get(&mut db, "fa"),
            Ok(JsonVal::Array(vec![
                JsonVal::from(1.0),
                JsonVal::from(2.0),
                JsonVal::from(3.0),
                JsonVal::from(4.0),
                JsonVal::from(5.0),
            ]))
        );
        assert_eq!(db_get(&mut db, "f"), Ok(JsonVal::from(3.3)));
        assert_eq!(db_get(&mut db, "s"), Ok(JsonVal::from("hello")));
        assert_eq!(
            db_get(&mut db, "sa"),
            Ok(JsonVal::Array(vec![
                JsonVal::from("a"),
                JsonVal::from("b"),
                JsonVal::from("c"),
                JsonVal::from("d"),
            ]))
        );
        assert_eq!(db_get(&mut db, "z"), Ok(JsonVal::from(2.0)));
    }
    */

    /*
    #[test]
    fn test_get() {
        let mut db = test_db();
        let val = eval(&mut db, get("b"));
        assert_eq!(Ok(JsonVal::Bool(true)), val);

        let val = eval(&mut db, get("b"));
        assert_eq!(Ok(JsonVal::Bool(true)), val);

        let val = db.eval(get("ia"));
        assert_eq!(
            Ok(JsonVal::Array(vec![
                JsonVal::from(1),
                JsonVal::from(2),
                JsonVal::from(3),
                JsonVal::from(4),
                JsonVal::from(5)
            ])),
            val
        );

        let val = db.eval(get("i"));
        assert_eq!(Ok(JsonVal::from(3)), val);
    }
    */

    /*
    #[test]
    fn test_first() {
        let mut db = test_db();
        assert_eq!(Ok(JsonVal::Bool(true)), eval(&mut db, first(get("b"))));
        let val = db.eval(first(get("b")));
        assert_eq!(Ok(JsonVal::Bool(true)), val);
        let val = db.eval(first(get("f")));
        assert_eq!(Ok(JsonVal::from(3.3)), val);
        let val = db.eval(first(get("i")));
        assert_eq!(Ok(JsonVal::from(3)), val);
        let val = db.eval(first(get("fa")));
        assert_eq!(Ok(JsonVal::from(1.0)), val);
        let val = db.eval(first(get("ia")));
        assert_eq!(Ok(JsonVal::from(1)), val);
    }
    */

    /*
    #[test]
    fn test_last() {
        let mut db = test_db();
        assert_eq!(Ok(JsonVal::from(true)), eval(&mut db, last(get("b"))));
        let val = db.eval(last(get("b")));
        assert_eq!(Ok(JsonVal::from(true)), val);
        let val = db.eval(last(get("f")));
        assert_eq!(Ok(JsonVal::from(3.3)), val);
        let val = db.eval(last(get("i")));
        assert_eq!(Ok(JsonVal::from(3)), val);
        let val = db.eval(last(get("fa")));
        assert_eq!(Ok(JsonVal::from(5.0)), val);
        let val = db.eval(last(get("ia")));
        assert_eq!(Ok(JsonVal::from(5)), val);
    }
    */

    /*
    #[test]
    fn test_max() {
        let mut db = test_db();
        let val = eval(&mut db, max(get("b")));
        assert_eq!(Ok(JsonVal::Bool(true)), val);
        let val = eval(&mut db, max(get("b")));
        assert_eq!(Ok(JsonVal::Bool(true)), val);
        let val = eval(&mut db, max(get("i")));
        assert_eq!(Ok(JsonVal::from(3)), val);
        let val = eval(&mut db, max(get("f")));
        assert_eq!(Ok(JsonVal::from(3.3)), val);
        let val = eval(&mut db, max(get("ia")));
        assert_eq!(Ok(JsonVal::from(5.0)), val);
        let val = eval(&mut db, max(get("fa")));
        assert_eq!(Ok(JsonVal::from(5.0)), val);
    }

    #[test]
    fn test_min() {
        let mut db = test_db();
        let val = eval(&mut db, min(get("b")));
        assert_eq!(Ok(JsonVal::Bool(true)), val);
        let val = eval(&mut db, min(get("i")));
        assert_eq!(Ok(JsonVal::from(3)), val);
        let val = eval(&mut db, min(get("f")));
        assert_eq!(Ok(JsonVal::from(3.3)), val);
        let val = eval(&mut db, min(get("fa")));
        assert_eq!(Ok(JsonVal::from(1.0)), val);
        let val = eval(&mut db, min(get("ia")));
        assert_eq!(Ok(JsonVal::from(1.0)), val);
    }

    #[test]
    fn test_avg() {
        let mut db = test_db();
        let val = eval(&mut db, avg(get("f")));
        assert_eq!(Ok(JsonVal::from(3.3)), val);
        let val = db.eval(&json_fn("avg", get("i")));
        assert_eq!(Ok(JsonVal::from(3)), val);
        let val = db.eval(&json_fn("avg", get("fa")));
        assert_eq!(Ok(JsonVal::from(3.0)), val);
        let val = db.eval(&json_fn("avg", get("ia")));
        assert_eq!(Ok(JsonVal::from(3.0)), val);
    }

    #[test]
    fn test_var() {
        let mut db = test_db();
        let val = eval(&mut db, var(get("f")));
        assert_eq!(Ok(JsonVal::from(3.3)), val);
        let val = db.eval(var(get("i")));
        assert_eq!(Ok(JsonVal::from(3)), val);
        let val = db.eval(var(get("fa"))).unwrap();
        assert_approx_eq!(2.56, json_f64(&val), 0.0249f64);
        let val = db.eval(var(get("ia"))).unwrap();
        assert_approx_eq!(2.56, json_f64(&val), 0.0249f64);
    }
    #[test]
    fn test_dev() {
        let mut db = test_db();
        let val = eval(&mut db, dev(get("f")));
        assert_eq!(Ok(JsonVal::from(3.3)), val);
        let val = eval(&mut db, dev(get("i")));
        assert_eq!(Ok(JsonVal::from(3)), val);
        let val = eval(&mut db, dev(get("fa"))).unwrap();
        assert_approx_eq!(1.4, json_f64(&val), 0.0249f64);
        let val = eval(&mut db, dev(get("ia"))).unwrap();
        assert_approx_eq!(1.4, json_f64(&val), 0.0249f64);
    }

    #[test]
    fn test_add() {
        let mut db = test_db();
        assert_eq!(
            Ok(JsonVal::from(9.0)),
            eval(&mut db, add(get("x"), get("y")))
        );
        assert_eq!(
            Ok(JsonVal::from(vec![
                JsonVal::from(5.0),
                JsonVal::from(6.0),
                JsonVal::from(7.0),
                JsonVal::from(8.0),
                JsonVal::from(9.0),
            ])),
            eval(&mut db, add(get("x"), get("ia")))
        );

        assert_eq!(
            Ok(JsonVal::from(vec![
                JsonVal::from(6.0),
                JsonVal::from(7.0),
                JsonVal::from(8.0),
                JsonVal::from(9.0),
                JsonVal::from(10.0),
            ])),
            eval(&mut db, add(get("ia"), get("y")))
        );

        assert_eq!(
            Ok(JsonVal::from(vec![
                JsonVal::from(2.0),
                JsonVal::from(4.0),
                JsonVal::from(6.0),
                JsonVal::from(8.0),
                JsonVal::from(10.0),
            ])),
            eval(&mut db, add(get("ia"), get("ia")))
        );
        assert_eq!(
            Ok(JsonVal::Array(vec![
                JsonVal::from("ahello"),
                JsonVal::from("bhello"),
                JsonVal::from("chello"),
                JsonVal::from("dhello"),
            ])),
            db.eval(add(get("sa"), get("s")))
        );
        assert_eq!(
            Ok(JsonVal::Array(vec![
                JsonVal::from("helloa"),
                JsonVal::from("hellob"),
                JsonVal::from("helloc"),
                JsonVal::from("hellod"),
            ])),
            db.eval(add(get("s"), get("sa")))
        );
        assert_eq!(
            Ok(JsonVal::from("hellohello")),
            db.eval(add(get("s"), get("s")))
        );
        assert_eq!(bad_type(), db.eval(add(get("s"), get("f"))));
        assert_eq!(bad_type(), db.eval(add(get("f"), get("s"))));
        assert_eq!(bad_type(), db.eval(add(get("i"), get("s"))));
        assert_eq!(bad_type(), db.eval(add(get("s"), get("i"))));
    }

    #[test]
    fn test_sub() {
        let mut db = test_db();
        assert_eq!(Ok(JsonVal::from(-1.0)), db.eval(sub(get("x"), get("y"))));
        assert_eq!(Ok(JsonVal::from(1.0)), db.eval(sub(get("y"), get("x"))));
        assert_eq!(
            Ok(JsonVal::from(vec![
                JsonVal::from(3.0),
                JsonVal::from(2.0),
                JsonVal::from(1.0),
                JsonVal::from(0.0),
                JsonVal::from(-1.0),
            ])),
            db.eval(sub(get("x"), get("ia")))
        );

        assert_eq!(
            Ok(JsonVal::from(vec![
                JsonVal::from(-4.0),
                JsonVal::from(-3.0),
                JsonVal::from(-2.0),
                JsonVal::from(-1.0),
                JsonVal::from(0.0),
            ])),
            db.eval(sub(get("ia"), get("y")))
        );

        assert_eq!(
            Ok(JsonVal::from(vec![
                JsonVal::from(0.0),
                JsonVal::from(0.0),
                JsonVal::from(0.0),
                JsonVal::from(0.0),
                JsonVal::from(0.0),
            ])),
            db.eval(sub(get("ia"), get("ia")))
        );

        assert_eq!(bad_type(), db.eval(sub(get("s"), get("s"))));
        assert_eq!(bad_type(), db.eval(sub(get("sa"), get("s"))));
        assert_eq!(bad_type(), db.eval(sub(get("s"), get("sa"))));
        assert_eq!(bad_type(), db.eval(sub(get("i"), get("s"))));
        assert_eq!(bad_type(), db.eval(sub(get("s"), get("i"))));
    }

    #[test]
    fn json_mul() {
        let mut db = test_db();
        assert_eq!(Ok(JsonVal::from(20.0)), db.eval(mul(get("x"), get("y"))));
        assert_eq!(Ok(JsonVal::from(16.0)), db.eval(mul(get("x"), get("x"))));
        let arr = vec![
            JsonVal::from(5.0),
            JsonVal::from(10.0),
            JsonVal::from(15.0),
            JsonVal::from(20.0),
            JsonVal::from(25.0),
        ];
        assert_eq!(
            Ok(JsonVal::from(arr.clone())),
            db.eval(mul(get("ia"), get("y")))
        );
        assert_eq!(Ok(JsonVal::from(arr)), db.eval(mul(get("y"), get("ia"))));
        assert_eq!(
            Ok(JsonVal::from(vec![
                JsonVal::from(1.0),
                JsonVal::from(4.0),
                JsonVal::from(9.0),
                JsonVal::from(16.0),
                JsonVal::from(25.0),
            ])),
            db.eval(mul(get("ia"), get("ia")))
        );
        assert_eq!(bad_type(), db.eval(mul(get("s"), get("s"))));
        assert_eq!(bad_type(), db.eval(mul(get("sa"), get("s"))));
        assert_eq!(bad_type(), db.eval(mul(get("s"), get("sa"))));
        assert_eq!(bad_type(), db.eval(mul(get("i"), get("s"))));
        assert_eq!(bad_type(), db.eval(mul(get("s"), get("i"))));
    }

    #[test]
    fn json_div() {
        let mut db = test_db();
        assert_eq!(Ok(JsonVal::from(1.0)), db.eval(div(get("x"), get("x"))));
        assert_eq!(Ok(JsonVal::from(1.0)), db.eval(div(get("y"), get("y"))));
        assert_eq!(
            Ok(JsonVal::from(vec![
                JsonVal::from(1.0),
                JsonVal::from(1.0),
                JsonVal::from(1.0),
                JsonVal::from(1.0),
                JsonVal::from(1.0),
            ])),
            db.eval(div(get("ia"), get("ia")))
        );
        assert_eq!(
            Ok(JsonVal::from(vec![
                JsonVal::from(0.5),
                JsonVal::from(1.0),
                JsonVal::from(1.5),
                JsonVal::from(2.0),
                JsonVal::from(2.5),
            ])),
            db.eval(div(get("ia"), get("z")))
        );
        assert_eq!(
            Ok(JsonVal::from(vec![
                JsonVal::from(2.0),
                JsonVal::from(1.0),
                JsonVal::from(0.6666666666666666),
                JsonVal::from(0.5),
                JsonVal::from(0.4),
            ])),
            db.eval(div(get("z"), get("ia")))
        );

        assert_eq!(bad_type(), db.eval(div(get("s"), get("s"))));
        assert_eq!(bad_type(), db.eval(div(get("sa"), get("s"))));
        assert_eq!(bad_type(), db.eval(div(get("s"), get("sa"))));
        assert_eq!(bad_type(), db.eval(div(get("i"), get("s"))));
        assert_eq!(bad_type(), db.eval(div(get("s"), get("i"))));
    }
    */
}
