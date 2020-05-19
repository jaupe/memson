use std::collections::BTreeMap;
use std::fs;
use std::io::{self};
use std::path::Path;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonVal};

use crate::json::*;
use crate::log::*;
use crate::query::{Expr, Query};

use crate::Row;

#[derive(Debug, Deserialize, Serialize)]
pub enum Cmd {
    Insert(String, Vec<Row>),
    Delete(String),
    Query(Query),
}

#[derive(Debug)]
pub struct Table {
    name: String,
    rows: Vec<Row>,
    log: ReplayLog,
}

impl Table {
    pub fn new<S: Into<String>, P: AsRef<Path>>(name: S, path: P, rows: Vec<Row>) -> io::Result<Self> {
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

    pub fn from<S: Into<String>>(name: S, rows: Vec<Row>, log: ReplayLog) -> Self {
        Self {
            name: name.into(),
            rows,
            log,
        }
    }

    pub fn insert(&mut self, rows: Vec<Row>) -> io::Result<()> {
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

    pub fn rows(&self) -> &[Row] {
        &self.rows
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

    pub fn find_table(&self, name: &str) -> Option<&Table> {
        self.tables.iter().find(|x| x.name() == name)
    }

    pub fn find_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.tables.iter_mut().find(|x| x.name() == name)
    }

    pub fn insert_table(&mut self, name: String, rows: Vec<Row>) -> io::Result<()> {
        let r = self.find_table_mut(&name);
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
    use std::fs::remove_file;

    use serde_json::Map;

    use super::*;
    use crate::Row;
    use crate::obj;

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
            vec![obj! {"x" => 1}, obj! {"x" => 2.1}, obj! {"x" => "s"}],
        );
        db.eval_cmd(cmd).unwrap();
        assert_eq!(db.tables.len(), 1);
        let tbl = &db.tables[0];
        assert_eq!(tbl.name(), "t");
        assert_eq!(tbl.len(), 3);
        assert_eq!(tbl.rows[0], obj! {"x" => 1});
        assert_eq!(tbl.rows[1], obj! {"x" => 2.1});
        assert_eq!(tbl.rows[2], obj! {"x" => "s"});

        remove_file("./test.db").unwrap();
        remove_file("./t.table").unwrap();
    }

    #[test]
    fn delete_table_ok() {
        // populate db with test table
        let mut db = Database::open("./", "test3").unwrap();
        db.eval_cmd(Cmd::Insert(
            "foo".to_string(),
            vec![obj! {"x" => 1}, obj! {"x" => 2.1}, obj! {"x" => "s"}],
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
            vec![obj! {"x" => 1}, obj! {"x" => 2.1}, obj! {"x" => "s"}],
        );
        db.eval_cmd(cmd).unwrap();
        // append data to table
        let cmd = Cmd::Insert(
            "append".to_string(),
            vec![obj! {"x" => 2}, obj! {"x"=>3.1}, obj! {"x"=>"t"}],
        );
        db.eval_cmd(cmd).unwrap();
        assert_eq!(db.tables.len(), 1);
        let tbl = &db.tables[0];
        assert_eq!(tbl.name(), "append");
        assert_eq!(tbl.len(), 6);
        assert_eq!(tbl.rows[0], obj! {"x" => 1});
        assert_eq!(tbl.rows[1], obj! {"x" => 2.1});
        assert_eq!(tbl.rows[2], obj! {"x" => "s"});
        assert_eq!(tbl.rows[3], obj! {"x" => 2});
        assert_eq!(tbl.rows[4], obj! {"x" => 3.1});
        assert_eq!(tbl.rows[5], obj! {"x" => "t"});

        remove_file("./append.db").unwrap();
        remove_file("./append.table").unwrap();
    }

}
