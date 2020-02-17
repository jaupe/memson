//! Memson is an in-memory JSON key/value cache.
//!
//! JSON structures are stored by a string key.
//!


#![warn(rust_2018_idioms)]

use clap::{App, Arg};
use tokio::net::TcpListener;
use tokio_util::codec::{Framed, LinesCodec};

use db::*;
use either::Either;
use futures::{SinkExt, StreamExt};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonVal;
use std::error::Error;
use std::sync::{Arc, Mutex};

mod db;
mod json;
mod parse;
mod replay;

type Res<T> = Result<T, &'static str>;

/// Possible requests our clients can send us
#[derive(Debug, Deserialize, Serialize)]
enum Request {
    #[serde(rename = "get")]
    Get(String),
    #[serde(rename = "set")]
    Set(String, JsonVal),
    #[serde(rename = "sum")]
    Sum(Box<Request>),
    #[serde(rename = "max")]
    Max(Box<Request>),
    #[serde(rename = "min")]
    Min(Box<Request>),
    #[serde(rename = "first")]
    First(Box<Request>),
    #[serde(rename = "last")]
    Last(Box<Request>),
    #[serde(rename = "+")]
    Add(Box<Request>, Option<Box<Request>>),
}

/// Responses to the `Request` commands above
enum Response {
    Value { value: JsonVal },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let matches = App::new("memson")
        .version("1.0")
        .about("In-memory JSON Cache")
        .author("jaupe")
        .arg(
            Arg::with_name("log")
                .short("l")
                .long("log")
                .value_name("FILE")
                .help("Sets a custom config file")
                .takes_value(false),
        )
        .arg(
            Arg::with_name("host")
                .short("h")
                .long("host")
                .value_name("IP")
                .help("Sets the IP address to listen on")
                .takes_value(false),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .value_name("port")
                .help("Sets the port number to listen on")
                .takes_value(false),
        )
        .get_matches();

    let log = matches.value_of("log").unwrap_or("memson.log");
    // Parse the address we're going to run this server on
    // and set up our TCP listener to accept connections.
    let host = matches.value_of("host").unwrap_or("0.0.0.0");
    let port = matches.value_of("port").unwrap_or("8000");
    let addr = host.to_string() + ":" + port;
    println!("replaying write log: {:?}", log);
    println!("listening on: {:?}", addr);
    let mut listener = TcpListener::bind(&addr).await?;

    // Create the shared state of this server that will be shared amongst all
    // clients. We populate the initial database and then create the `Database`
    // structure. Note the usage of `Arc` here which will be used to ensure that
    // each independently spawned client will have a reference to the in-memory
    // database.

    let db: Database = Database::open(&addr).unwrap();
    let db: Arc<Mutex<Database>> = Arc::new(Mutex::new(db));
    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                // After getting a new connection first we see a clone of the database
                // being created, which is creating a new reference for this connected
                // client to use.
                let dbase = db.clone();

                // Like with other small servers, we'll `spawn` this client to ensure it
                // runs concurrently with all other clients. The `move` keyword is used
                // here to move ownership of our db handle into the async closure.
                tokio::spawn(async move {
                    // Since our protocol is line-based we use `tokio_codecs`'s `LineCodec`
                    // to convert our stream of bytes, `socket`, into a `Stream` of lines
                    // as well as convert our line based responses into a stream of bytes.
                    let mut lines = Framed::new(socket, LinesCodec::new());

                    // Here for every line we get back from the `Framed` decoder,
                    // we parse the request, and if it's valid we generate a response
                    // based on the values in the database.
                    while let Some(result) = lines.next().await {
                        match result {
                            Ok(line) => {
                                let response = match handle_request(&line, &dbase) {
                                    Ok(r) => r,
                                    Err(err) => {
                                        eprintln!("{:?}", err);
                                        continue;
                                    }
                                };

                                let response = response.serialize();

                                if let Err(e) = lines.send(response).await {
                                    println!("error on sending response; error = {:?}", e);
                                }
                            }
                            Err(e) => {
                                println!("error on decoding from socket; error = {:?}", e);
                            }
                        }
                    }

                    // The connection will be closed at this point as `lines.next()` has returned `None`.
                });
            }
            Err(e) => println!("error accepting socket; error = {:?}", e),
        }
    }
}

fn handle_request(line: &str, db_lock: &Arc<Mutex<Database>>) -> Res<Response> {
    let mut db = db_lock.lock().unwrap();
    let val = db.eval(line);
    println!("{:?}", val);
    let val = match val {
        Ok(Either::Left(lhs)) => Response::Value {
            value: lhs,
        },
        Ok(Either::Right(rhs)) => Response::Value {
            value: rhs.clone(),
        },
        Err(msg) => {
            eprintln!("error: {}", msg);
            Response::Value {
                value: JsonVal::Null,
            }
        }
    };
    Ok(val)
}

impl Response {
    fn serialize(&self) -> String {
        match self {
            Response::Value { value: val, .. } => format!("{}", val),
        }
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    use assert_approx_eq::assert_approx_eq;
    
    fn json_fn(f: &str, key: &str) -> String {
        "{\"".to_string() + f + "\":\"" + key + "\"}"  
    }

    fn json_fn_get(f: &str, key: &str) -> String {
        "{\"".to_string() + f + "\": {\"get\":\"" + key + "\"}]"  
    }

    fn test_db() -> Database {
        Database::open("test.log").unwrap()
    }

    fn json_f64(v: &JsonVal) -> f64 {
        v.as_f64().unwrap()
    }

    fn val_f64<'a>(v: Either<JsonVal, &'a JsonVal>) -> f64 {
        match v {
            Either::Left(ref val) => json_f64(val),
            Either::Right(val) => json_f64(val),
        }
    }

    #[test]
    fn open_db() {
        let db = test_db();
        assert_eq!(7, db.len());
    }

    #[test]
    fn get() {
        let mut db = test_db();
        let val = db.eval(&json_fn("get", "b")).unwrap();
        assert_eq!(Either::Right(&JsonVal::Bool(true)), val);

        let val = db.eval("\"b\"").unwrap();
        assert_eq!(Either::Right(&JsonVal::Bool(true)), val);

        let val = db.eval(&json_fn("get", "ia")).unwrap();
        assert_eq!(
            Either::Right(&JsonVal::Array(vec![
                JsonVal::from(1),
                JsonVal::from(2),
                JsonVal::from(3),
                JsonVal::from(4),
                JsonVal::from(5)
            ])),
            val
        );

        let val = db.eval(&json_fn("get", "i")).unwrap();
        assert_eq!(
            Either::Right(&JsonVal::from(3)),
            val
        );        
    }

    #[test]
    fn first() {
        let mut db = test_db();
        let val = db.eval(r#"{"first": "b"}"#).unwrap();
        assert_eq!(Either::Left(JsonVal::Bool(true)), val);
        let val = db.eval(r#"{"first": {"get": "b"}}"#).unwrap();
        assert_eq!(Either::Left(JsonVal::Bool(true)), val);
        let val = db.eval(&json_fn("first", "f")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3.3)), val);
        let val = db.eval(&json_fn("first", "i")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3)), val);
        let val = db.eval(&json_fn("first", "fa")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(1.0)), val);
        let val = db.eval(&json_fn("first", "ia")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(1)), val);        
    }

    #[test]
    fn last() {
        let mut db = test_db();
        let val = db.eval(r#"{"last": "b"}"#).unwrap();
        assert_eq!(Either::Left(JsonVal::from(true)), val);
        let val = db.eval(r#"{"last": {"get": "b"}}"#).unwrap();
        assert_eq!(Either::Left(JsonVal::from(true)), val);
        let val = db.eval(&json_fn("last", "f")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3.3)), val);
        let val = db.eval(&json_fn("last", "i")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3)), val);
        let val = db.eval(&json_fn("last", "fa")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(5.0)), val);
        let val = db.eval(&json_fn("last", "ia")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(5)), val);        
    }

    #[test]
    fn max() {
        let mut db = test_db();
        let val = db.eval(r#"{"max": "b"}"#).unwrap();
        assert_eq!(Either::Left(JsonVal::Bool(true)), val);
        let val = db.eval(r#"{"max": {"get": "i"}}"#).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3)), val);
        let val = db.eval(r#"{"max": {"get": "f"}}"#).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3.3)), val);
        let val = db.eval(r#"{"max": {"get": "ia"}}"#).unwrap();
        assert_eq!(Either::Left(JsonVal::from(5.0)), val);
        let val = db.eval(r#"{"max": {"get": "fa"}}"#).unwrap();
        assert_eq!(Either::Left(JsonVal::from(5.0)), val);
    }
    #[test]
    fn min() {
        let mut db = test_db();
        let val = db.eval(&json_fn("min", "b")).unwrap();
        assert_eq!(Either::Left(JsonVal::Bool(true)), val);
        let val = db.eval(&json_fn("min", "i")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3)), val);
        let val = db.eval(&json_fn("min", "f")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3.3)), val);
        let val = db.eval(&json_fn("min", "fa")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(1.0)), val);
        let val = db.eval(&json_fn("min", "ia")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(1.0)), val);
    }

    #[test]
    fn avg() {
        let mut db = test_db();
        let val = db.eval(&json_fn("avg", "f")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3.3)), val);
        let val = db.eval(&json_fn("avg", "i")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3)), val);
        let val = db.eval(&json_fn("avg", "fa")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3.0)), val);
        let val = db.eval(&json_fn("avg", "ia")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3.0)), val);        
    }

    #[test]
    fn var() {
        let mut db = test_db();
        let val = db.eval(&json_fn("var", "f")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3.3)), val);
        let val = db.eval(&json_fn("var", "i")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3)), val);
        let val = db.eval(&json_fn("var", "fa")).unwrap();
        assert_approx_eq!(2.56, val_f64(val), 0.0249f64);
        let val = db.eval(&json_fn("var", "ia")).unwrap();
        assert_approx_eq!(2.56, val_f64(val), 0.0249f64);        
    } 
    
    #[test]
    fn dev() {
        let mut db = test_db();
        let val = db.eval(&json_fn("dev", "f")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3.3)), val);
        let val = db.eval(&json_fn("dev", "i")).unwrap();
        assert_eq!(Either::Left(JsonVal::from(3)), val);
        let val = db.eval(&json_fn("dev", "fa")).unwrap();
        assert_approx_eq!(1.4, val_f64(val), 0.0249f64);
        let val = db.eval(&json_fn("dev", "ia")).unwrap();
        assert_approx_eq!(1.4, val_f64(val), 0.0249f64);        
    }     
}
