//! A "tiny database" and accompanying protocol
//!
//! This example shows the usage of shared state amongst all connected clients,
//! namely a database of key/value pairs. Each connected client can send a
//! series of GET/SET commands to query the current value of a key or set the
//! value of a key.
//!
//! This example has a simple protocol you can use to interact with the server.
//! To run, first run this in one terminal window:
//!
//!     cargo run --example tinydb
//!
//! and next in another windows run:
//!
//!     cargo run --example connect 127.0.0.1:8080
//!
//! In the `connect` window you can type in commands where when you hit enter
//! you'll get a response from the server for that command. An example session
//! is:
//!
//!
//!     $ cargo run --example connect 127.0.0.1:8080
//!     GET foo
//!     foo = bar
//!     GET FOOBAR
//!     error: no key FOOBAR
//!     SET FOOBAR my awesome string
//!     set FOOBAR = `my awesome string`, previous: None
//!     SET foo tokio
//!     set foo = `tokio`, previous: Some("bar")
//!     GET foo
//!     foo = tokio
//!
//! Namely you can issue two forms of commands:
//!
//! * `GET $key` - this will fetch the value of `$key` from the database and
//!   return it. The server's database is initially populated with the key `foo`
//!   set to the value `bar`
//! * `SET $key $value` - this will set the value of `$key` to `$value`,
//!   returning the previous value, if any.

#![warn(rust_2018_idioms)]

use std::io::{self, Lines, Write, BufReader};

use tokio::net::TcpListener;
use tokio_util::codec::{Framed, LinesCodec};

use futures::{SinkExt, StreamExt};
use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::sync::{Arc, Mutex, RwLock};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonVal;
use json::*;
use either::Either;
use std::path::Path;
use std::fs::{File,OpenOptions};
use replay::ReplayLog;
use db::*;

mod db;
mod json;
mod replay;

/// Possible requests our clients can send us
#[derive(Debug,Deserialize,Serialize)]
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

impl Request {
    fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    /*
    fn eval(self, cache: &mut Cache) -> Response {
        match self {
            Request::Get(key) => {
                match cache.get(&key) {
                    Some(val) => Response::Value{key, value: val.clone() },
                    None => Response::Error{msg: "not found".to_string()},
                }
            }
            Request::Set(key, value) => {
                let previous = cache.insert(key.clone(), value.clone());
                Response::Set{key, value, previous}
            }
            Request::Sum(arg) => {
                match arg.eval(cache) {
                    Response::Error{msg} => Response::Error{msg},
                    Response::Value{key, value } => unimplemented!(),
                    _ => unimplemented!()
                }
            }
            Request::First(arg) => {
                match arg.eval(cache) {
                    Response::Error{msg} => Response::Error{msg},
                    Response::Value{key, value} => {
                        match json_first(&value) {
                            Some(value) => Response::Value{key, value},
                            None => Response::Value{key, value: JsonVal::Null}
                        }
                    }
                    _ => unimplemented!(),
                }
            }
            Request::Last(arg) => {
                match arg.eval(cache) {
                    Response::Error{msg} => Response::Error{msg},
                    Response::Value{key, value} => {
                        match json_last(&value) {
                            Some(value) => Response::Value{key, value},
                            None => Response::Value{key, value: JsonVal::Null}
                        }
                    }
                    _ => unimplemented!(),
                }
            }
            _ => unimplemented!(),
        }
    }
    */
}

/// Responses to the `Request` commands above
enum Response {
    Value {
        key: String,
        value: JsonVal,
    },
    Error {
        msg: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Parse the address we're going to run this server on
    // and set up our TCP listener to accept connections.
    let addr = env::args().nth(1).unwrap_or("0.0.0.0:8080".to_string());

    let mut listener = TcpListener::bind(&addr).await?;
    println!("Listening on: {}", addr);

    // Create the shared state of this server that will be shared amongst all
    // clients. We populate the initial database and then create the `Database`
    // structure. Note the usage of `Arc` here which will be used to ensure that
    // each independently spawned client will have a reference to the in-memory
    // database.
    let log_path = "memson.log";
    println!("replaying write log: {:?}", log_path);
    let db: Database = Database::open(log_path).unwrap();
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
                                let response = handle_request(&line, &dbase);

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
            },
            Err(e) => println!("error accepting socket; error = {:?}", e),
        }
    }
}

fn handle_request(line: &str, db: &Arc<Mutex<Database>>) -> Response {
    let json_val: JsonVal = match serde_json::from_str(line) {
        Ok(val) => val,
        Err(_) => return Response::Error { msg: "bad json".to_string() },
    };
    
    let mut db = db.lock().unwrap();
    match eval_json(&json_val, &mut db) {
        Some(Either::Left(lhs)) => Response::Value{ key: line.to_string(), value: lhs },
        Some(Either::Right(rhs)) => Response::Value{ key: line.to_string(), value: rhs.clone() },
        None => Response::Value{key: line.to_string(), value: JsonVal::Null},
    }
}

impl Request {
    fn parse(input: &str) -> Result<Request, String> {
        serde_json::from_str(input).map_err(|_| "cannot parse".to_string())
    }
}

impl Response {
    fn serialize(&self) -> String {
        match *self {
            Response::Value { ref key, ref value } => format!("{} = {}", key, value),
            Response::Error { ref msg } => format!("error: {}", msg),
        }
    }
}
