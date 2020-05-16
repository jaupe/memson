use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonVal;

use crate::db::Table;
use crate::Res;

fn open_file<P: AsRef<Path>>(path: P) -> io::Result<File> {
    OpenOptions::new()
        .truncate(false)
        .read(true)
        .write(true)
        .create(true)
        .open(path)
}

#[derive(Debug)]
pub struct DbConfig {
    root_path: PathBuf,
    file: File,
}

#[derive(Debug, Serialize, Deserialize)]
struct TableConfig {
    key: String,
    val: PathBuf,
}

impl DbConfig {
    pub fn open<P: AsRef<Path>, S: Into<String>>(root: P, name: S) -> io::Result<Self> {
        let mut root_path = PathBuf::new();
        root_path.push(root);
        let mut path = root_path.clone();
        path.push(name.into() + ".db");
        let file = open_file(path)?;
        Ok(Self { root_path, file })
    }

    pub fn insert<S: Into<String>>(&mut self, table: S) -> io::Result<()> {
        let mut path = self.root_path.clone();
        let name = table.into();
        path.push(name.to_string() + ".table");
        let tbl_config = TableConfig {
            key: name,
            val: path,
        };
        let line = serde_json::to_string(&tbl_config).unwrap() + "\n";
        self.file.write_all(line.as_bytes())
    }

    pub fn load(&mut self) -> Res<Vec<Table>> {
        let buf = Box::new(BufReader::new(&mut self.file));
        let mut tables = Vec::new();
        //TODO parallelize this
        for line in buf.lines() {
            let line = line.map_err(|_| "cannot read db config line")?;
            let kv: TableConfig =
                serde_json::from_str(&line).map_err(|_| "cannot deserialize table config")?;
            let table = Table::open(kv.key, kv.val).map_err(|_| "")?;
            tables.push(table);
        }
        Ok(tables)
    }

    pub fn remove<S: Into<String>>(&mut self, table: S) -> io::Result<Vec<Table>> {
        unimplemented!()
    }
}

/// The replay log that records all mututations
///
///
#[derive(Debug)]
pub struct ReplayLog {
    file: File,
}

impl ReplayLog {
    pub fn new<P: AsRef<Path>>(path: P, rows: &[JsonVal]) -> io::Result<Self> {
        let mut log = Self::open(path)?;
        for row in rows {
            log.write(row)?;
        }
        Ok(log)
    }

    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = open_file(path)?;
        Ok(Self { file })
    }

    pub fn insert(&mut self, vals: &[JsonVal]) -> io::Result<()> {
        //TODO can this be done more efficiently to remove intermiedia?
        for val in vals {
            self.write(val)?;
        }
        Ok(())
    }

    fn write(&mut self, val: &JsonVal) -> io::Result<()> {
        let row = val.to_string() + "\n";
        self.file.write_all(row.as_bytes())
    }

    pub fn replay(&mut self) -> Res<Vec<JsonVal>> {
        let buf = Box::new(BufReader::new(&mut self.file));
        let mut rows = Vec::new();
        //TODO parallelize this
        for line in buf.lines() {
            let line = line.map_err(|err| {
                eprintln!("{:?}", err);
                "bad line"
            })?;
            let val: JsonVal = serde_json::from_str(&line).map_err(|err| {
                println!("{:?}", err);
                "bad json"
            })?;
            rows.push(val);
        }
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::remove_file;
    use std::io::Seek;

    use futures::io::SeekFrom;

    use super::*;

    #[test]
    fn dbconfig_load() {
        let mut log = DbConfig::open("./", "test2").unwrap();
        log.insert("a").unwrap();
        log.insert("b").unwrap();
        log.file.seek(SeekFrom::Start(0)).unwrap();
        let tables = log.load().unwrap();
        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].name(), "a");
        assert_eq!(tables[0].len(), 0);
        assert_eq!(tables[1].name(), "b");
        assert_eq!(tables[1].len(), 0);
        remove_file("./test2.db").unwrap();
        remove_file("./a.table").unwrap();
        remove_file("./b.table").unwrap();
    }

    #[test]
    fn replaylog_load() {
        let mut log = ReplayLog::open("./c.table").unwrap();
        log.insert(&vec![JsonVal::from(1), JsonVal::from("a")])
            .unwrap();
        log.insert(&vec![JsonVal::from(2), JsonVal::from("b")])
            .unwrap();
        log.file.seek(SeekFrom::Start(0)).unwrap();

        assert_eq!(
            log.replay().unwrap(),
            vec![
                JsonVal::from(1),
                JsonVal::from("a"),
                JsonVal::from(2),
                JsonVal::from("b")
            ]
        );

        remove_file("./c.table").unwrap();
    }
}
