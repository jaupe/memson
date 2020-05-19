use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonVal};

use crate::db::Table;
use crate::{Res, Row};

fn open_file<P: AsRef<Path>>(path: P) -> io::Result<File> {
    OpenOptions::new()
        .truncate(false)
        .read(true)
        .write(true)
        .create(true)
        .open(path)
}

#[derive(Debug, Serialize, Deserialize)]
struct TableConfig {
    table: String,
    path: PathBuf,
}

#[derive(Debug)]
pub struct DbConfig {
    name: String,
    root_path: PathBuf,
    file: File,
}

impl DbConfig {
    pub fn open<P: AsRef<Path>, S: Into<String>>(root: P, name: S) -> io::Result<Self> {
        let mut root_path = PathBuf::new();
        root_path.push(root);
        let mut path = root_path.clone();
        let name = name.into();
        let test_db = name.clone() + ".db";
        path.push(test_db);
        let file = open_file(path)?;
        Ok(Self {
            name,
            root_path,
            file,
        })
    }

    pub fn insert<S: Into<String>>(&mut self, table: S) -> io::Result<()> {
        let mut path = self.root_path.clone();
        let name = table.into();
        path.push(name.to_string() + ".table");
        let tbl_config = TableConfig { table: name, path };
        let line = serde_json::to_string(&tbl_config).unwrap() + "\n";
        self.file.write_all(line.as_bytes())
    }

    pub fn load(&mut self) -> Res<Vec<Table>> {
        let buf = Box::new(BufReader::new(&mut self.file));
        let mut tables = Vec::new();
        //TODO parallelize this
        for line in buf.lines() {
            let line = line.map_err(|_| "cannot read db config line")?;
            let config: TableConfig =
                serde_json::from_str(&line).map_err(|_| "cannot deserialize table config")?;
            let table = Table::open(config.table, config.path).map_err(|_| "")?;
            tables.push(table);
        }
        Ok(tables)
    }

    pub fn remove_table<S: Into<String>>(&mut self, tbl_name: S) -> io::Result<()> {
        // create new meta file
        let mut path_buf = self.root_path.clone();
        let tbl_name = tbl_name.into();
        let tmp_path = tbl_name.clone() + ".copy.table";
        path_buf.push(&tmp_path);
        let mut file = open_file(path_buf)?;
        // read old meta file and write to new one minus the removed table
        self.file.seek(SeekFrom::Start(0))?;
        let buf = Box::new(BufReader::new(&mut self.file));
        for line in buf.lines() {
            let line = line?;
            let config: TableConfig = serde_json::from_str(&line)?;
            if config.table != tbl_name {
                let json = serde_json::to_string(&config)? + "\n";
                file.write_all(json.as_bytes())?;
            }
        }
        // replace old meta file with new one
        let mut old_path = self.root_path.clone();
        old_path.push(tbl_name + ".table");
        fs::copy(&tmp_path, old_path)?;
        fs::remove_file(&tmp_path)?;
        Ok(())
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
    pub fn new<P: AsRef<Path>>(path: P, rows: &[Map<String, JsonVal>]) -> io::Result<Self> {
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

    pub fn insert(&mut self, vals: &[Map<String, JsonVal>]) -> io::Result<()> {
        //TODO can this be done more efficiently to remove intermiedia?
        for val in vals {
            self.write(val)?;
        }
        Ok(())
    }

    fn write(&mut self, val: &Map<String, JsonVal>) -> io::Result<()> {
        let row = serde_json::to_string(val).unwrap() + "\n";
        self.file.write_all(row.as_bytes())
    }

    pub fn replay(&mut self) -> Res<Vec<Row>> {
        let buf = Box::new(BufReader::new(&mut self.file));
        let mut rows = Vec::new();
        //TODO parallelize this
        for line in buf.lines() {
            let line = line.map_err(|err| {
                eprintln!("{:?}", err);
                "bad line"
            })?;
            let row: Row = serde_json::from_str(&line).map_err(|err| {
                println!("{:?}", err);
                "bad json"
            })?;
            rows.push(row);
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
        log.insert(&vec![obj!{"x"=> 1}, obj!{"x"=>"a"}])
            .unwrap();
        log.insert(&vec![obj!{"x"=>2}, obj!{"x"=>"b"}].as_ref())
            .unwrap();
        log.file.seek(SeekFrom::Start(0)).unwrap();

        assert_eq!(
            log.replay().unwrap(),
            vec![
                obj!{"x"=>1},
                obj!{"x"=>"a"},
                obj!{"x"=>2},
                obj!{"x"=>"b"},
            ]
        );

        remove_file("./c.table").unwrap();
    }
}
