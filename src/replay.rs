use std::collections::BTreeMap;
use std::fs::{File,OpenOptions};
use std::path::Path;
use std::io::{self, BufRead, BufReader, Write};
use serde_json::Value as JsonVal;
/// The replay log that records all mututations
/// 
/// 
/// 
pub struct ReplayLog {
    file: File,
}

impl ReplayLog {
    pub fn open<P:AsRef<Path>>(path: P) -> io::Result<ReplayLog> {
        let file = OpenOptions::new()
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .create(true)                    
                    .open(path)?;
        Ok(ReplayLog{ file })
    }

    pub fn write(&mut self, key: &str, val: &JsonVal) -> io::Result<()> {
        let line = key.to_string() + "=" + &val.to_string() + "\n";
        self.file.write_all(line.as_bytes())?;
        Ok(())
    }

    pub fn replay<'a>(&'a mut self) -> BTreeMap<String, JsonVal> {
        let buf = Box::new(BufReader::new(&mut self.file));
        let mut cache = BTreeMap::new();
        for line in buf.lines() {
            println!("line={:?}", line);
            let s = line.unwrap();

            let mut it = s.split_terminator('=');
            let key = it.next().unwrap();
            let val_str = it.next().unwrap();
            let val: JsonVal = serde_json::from_str(&val_str).unwrap();
            cache.insert(key.to_string(), val);
        }
        cache
    }   
}