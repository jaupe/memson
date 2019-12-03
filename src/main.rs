
/*
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonVal;
use std::collections::BTreeMap;

use futures::future::Future;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

type Res<T> = Result<T, &'static str>;

const BAD_TYPE: &str = "bad type";
const BAD_JSON: &str = "bad json";

fn json_sum(val: &JsonVal) -> Res<JsonVal> {
    let mut sum = 0.0;
    match val {
        JsonVal::Array(arr) => {
            for val in arr {
                sum += val.as_f64().ok_or(BAD_TYPE)?;
            }
            Ok(JsonVal::from(sum))
        }
        JsonVal::Bool(val) => Ok(if *val {
            JsonVal::from(1)
        } else {
            JsonVal::from(0)
        }),
        JsonVal::String(_) => Err(BAD_TYPE),
        JsonVal::Null => Err(BAD_TYPE),
        JsonVal::Object(_) => Err(BAD_TYPE),
        JsonVal::Number(num) => Ok(JsonVal::Number(num.clone())),
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
enum Cmd {
    Get(String),
    Put(String, JsonVal),
    Sum(Box<Cmd>),
    Max(Box<Cmd>),
    Min(Box<Cmd>),
    Avg(Box<Cmd>),
    Dev(Box<Cmd>),
    Add(Box<Cmd>, Box<Cmd>),
    Sub(Box<Cmd>, Box<Cmd>),
    Mul(Box<Cmd>, Box<Cmd>),
    Div(Box<Cmd>, Box<Cmd>),
}

impl Cmd {
    fn parse(s: &str) -> Res<Cmd> {
        let cmd: Cmd = serde_json::from_str(s).unwrap();
        Ok(cmd)
    }
}

type Cache = BTreeMap<String, JsonVal>;

#[derive(Debug)]
struct Db {
    cache: Cache,
}

#[derive(Debug, PartialEq)]
enum Json<'a> {
    Val(JsonVal),
    Ref(&'a JsonVal),
}

impl<'a> Json<'a> {
    fn sum(&self) -> Res<JsonVal> {
        match self {
            Json::Val(ref val) => json_sum(val),
            Json::Ref(val) => json_sum(val),
        }
    }

    fn to_string(&self) -> String {
        match self {
            Json::Val(val) => val.to_string(),
            Json::Ref(val) => val.to_string(),
        }
    }
}

impl Db {
    fn new() -> Self {
        Db {
            cache: BTreeMap::new(),
        }
    }

    fn eval<'a>(&'a self, cmd: Cmd) -> Res<Option<Json<'a>>> {
        match cmd {
            Cmd::Get(ref key) => match self.cache.get(key) {
                Some(val) => Ok(Some(Json::Ref(val))),
                None => Ok(None),
            },
            Cmd::Put(key, val) => Err("bad cmd"),
            Cmd::Sum(arg) => match self.eval(*arg)? {
                Some(val) => Ok(Some(val.sum().map(Json::Val)?)),
                None => Ok(None),
            },
            _ => unimplemented!(),
        }
    }

    fn put<S: Into<String>>(&mut self, key: S, val: JsonVal) -> Option<Json> {
        self.cache.insert(key.into(), val).map(Json::Val)
    }
}

enum Response<'a> {
    Json(Json<'a>),
    Error(&'static str),
}

fn main() {
    Server::start();
}

impl<'a> Response<'a> {
    fn serialize(&self) -> String {
        match self {
            Response::Json(json) => json.to_string(),
            Response::Error(err) => err.to_string(),
        }
    }
}

struct Server {}

impl Server {
    fn start() {
        let addr = "127.0.0.1:6142".parse().unwrap();
        println!("creating stream");
        let client = TcpStream::connect(&addr)
            .and_then(|stream| {
                println!("created stream");
                io::write_all(stream, "hello world\n").then(|result| {
                    println!("wrote to stream; success={:?}", result.is_ok());
                    Ok(())
                })
            })
            .map_err(|err| {
                // All tasks must have an `Error` type of `()`. This forces error
                // handling and helps avoid silencing failures.
                //
                // In our example, we are only going to log the error to STDOUT.
                println!("connection error = {:?}", err);
            });

        println!("About to create the stream and write to it...");
        tokio::run(client);
        println!("Stream has been created and written to.");
    }
}

#[test]
fn db_eval_get() {
    let mut db = Db::new();

    let key = "k1";
    let val = JsonVal::Array(
        vec![1, 2, 3, 4, 5]
            .into_iter()
            .map(|x| JsonVal::from(x))
            .collect(),
    );
    db.put(key, val.clone());
    let get = Cmd::Get(key.to_string());
    let exp = Ok(Some(Json::Ref(&val)));
    assert_eq!(exp, db.eval(get))
}

#[test]
fn db_eval_put_get() {
    let mut db = Db::new();

    let key = "k1";
    let val = JsonVal::from(1.0);

    db.eval(Cmd::Put(key.to_string(), val.clone()));
    assert_eq!(
        Ok(Some(Json::Ref(&val))),
        db.eval(Cmd::Get(key.to_string()))
    );
}

#[test]
fn db_eval_put_replace() {
    let mut db = Db::new();

    let key = "k1";
    let val = JsonVal::from(1.0);

    assert_eq!(Ok(None), db.eval(Cmd::Put(key.to_string(), val.clone())));
    assert_eq!(
        Ok(Some(Json::Ref(&val))),
        db.eval(Cmd::Get(key.to_string()))
    );
    assert_eq!(
        Ok(Some(Json::Val(val))),
        db.eval(Cmd::Put(key.to_string(), JsonVal::from("hello")))
    );
    let val = JsonVal::from("hello");
    assert_eq!(
        Ok(Some(Json::Ref(&val))),
        db.eval(Cmd::Get(key.to_string()))
    );
}

#[test]
fn db_eval_sum() {
    let mut db = Db::new();
    db.put(
        "k1",
        JsonVal::Array(vec![JsonVal::from(1), JsonVal::from(2), JsonVal::from(3)]),
    );
    let res = db.eval(Cmd::Sum(Box::new(Cmd::Get("k1".to_string()))));
    assert_eq!(Ok(Some(Json::Val(JsonVal::from(6.0)))), res);
}

#[test]
fn json_sum_array() {
    let val = JsonVal::Array(
        vec![1, 2, 3, 4, 5]
            .into_iter()
            .map(|x| JsonVal::from(x))
            .collect(),
    );
    assert_eq!(Ok(JsonVal::from(15.0)), json_sum(&val));
}

#[test]
fn json_sum_str() {
    let val = JsonVal::String("hello".to_string());
    assert_eq!(Err("bad json type"), json_sum(&val));
}

#[test]
fn json_sum_homovec() {
    let e1 = JsonVal::String("hello".to_string());
    let e2 = JsonVal::from(2);
    let e3 = JsonVal::from(3.1);
    let val = JsonVal::Array(vec![e1, e2, e3]);
    assert_eq!(Err("bad json type"), json_sum(&val));
}

#[test]
fn json_sum_numvec() {
    let e1 = JsonVal::from(1.1);
    let e2 = JsonVal::from(2);
    let e3 = JsonVal::from(3.1);
    let val = JsonVal::Array(vec![e1, e2, e3]);
    assert_eq!(Ok(JsonVal::from(6.2)), json_sum(&val));
}

#[test]
fn eval_sum_arr() {
    let mut db = Db::new();
    db.put(
        "k1",
        JsonVal::Array(vec![JsonVal::from(1), JsonVal::from(2), JsonVal::from(3)]),
    );
    let exp = Json::Val(JsonVal::from(6.0));
    assert_eq!(
        Ok(Some(exp)),
        db.eval(Cmd::Sum(Box::new(Cmd::Get("k1".to_string()))))
    )
}

#[test]
fn eval_sum_arr_err() {
    let mut db = Db::new();
    db.put(
        "k1",
        JsonVal::Array(vec![
            JsonVal::from(1),
            JsonVal::String("s".to_string()),
            JsonVal::from(3),
        ]),
    );
    assert_eq!(
        Err(BAD_TYPE),
        db.eval(Cmd::Sum(Box::new(Cmd::Get("k1".to_string()))))
    )
}
*/

mod db;

use std::collections::HashMap;
use std::env;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::io::{lines, write_all};
use tokio::net::TcpListener;
use tokio::prelude::*;

/// The in-memory database shared amongst all clients.
///
/// This database will be shared via `Arc`, so to mutate the internal map we're
/// going to use a `Mutex` for interior mutability.
struct Database {
    map: Mutex<HashMap<String, String>>,
}

/// Possible requests our clients can send us
enum Request {
    Get { key: String },
    Set { key: String, value: String },
}

/// Responses to the `Request` commands above
enum Response {
    Value {
        key: String,
        value: String,
    },
    Set {
        key: String,
        value: String,
        previous: Option<String>,
    },
    Error {
        msg: String,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse the address we're going to run this server on
    // and set up our TCP listener to accept connections.
    let addr = env::args().nth(1).unwrap_or("127.0.0.1:8080".to_string());
    let addr = addr.parse::<SocketAddr>()?;
    let listener = TcpListener::bind(&addr).map_err(|_| "failed to bind")?;
    println!("Listening on: {}", addr);

    // Create the shared state of this server that will be shared amongst all
    // clients. We populate the initial database and then create the `Database`
    // structure. Note the usage of `Arc` here which will be used to ensure that
    // each independently spawned client will have a reference to the in-memory
    // database.
    let mut initial_db = HashMap::new();
    initial_db.insert("foo".to_string(), "bar".to_string());
    let db = Arc::new(Database {
        map: Mutex::new(initial_db),
    });

    let done = listener
        .incoming()
        .map_err(|e| println!("error accepting socket; error = {:?}", e))
        .for_each(move |socket| {
            // As with many other small examples, the first thing we'll do is
            // *split* this TCP stream into two separately owned halves. This'll
            // allow us to work with the read and write halves independently.
            let (reader, writer) = socket.split();

            // Since our protocol is line-based we use `tokio_io`'s `lines` utility
            // to convert our stream of bytes, `reader`, into a `Stream` of lines.
            let lines = lines(BufReader::new(reader));

            // Here's where the meat of the processing in this server happens. First
            // we see a clone of the database being created, which is creating a
            // new reference for this connected client to use. Also note the `move`
            // keyword on the closure here which moves ownership of the reference
            // into the closure, which we'll need for spawning the client below.
            //
            // The `map` function here means that we'll run some code for all
            // requests (lines) we receive from the client. The actual handling here
            // is pretty simple, first we parse the request and if it's valid we
            // generate a response based on the values in the database.
            let db = db.clone();
            let responses = lines.map(move |line| {
                let request = match Request::parse(&line) {
                    Ok(req) => req,
                    Err(e) => return Response::Error { msg: e },
                };

                let mut db = db.map.lock().unwrap();
                match request {
                    Request::Get { key } => match db.get(&key) {
                        Some(value) => Response::Value {
                            key,
                            value: value.clone(),
                        },
                        None => Response::Error {
                            msg: format!("no key {}", key),
                        },
                    },
                    Request::Set { key, value } => {
                        let previous = db.insert(key.clone(), value.clone());
                        Response::Set {
                            key,
                            value,
                            previous,
                        }
                    }
                }
            });

            // At this point `responses` is a stream of `Response` types which we
            // now want to write back out to the client. To do that we use
            // `Stream::fold` to perform a loop here, serializing each response and
            // then writing it out to the client.
            let writes = responses.fold(writer, |writer, response| {
                let mut response = response.serialize();
                response.push('\n');
                write_all(writer, response.into_bytes()).map(|(w, _)| w)
            });

            // Like with other small servers, we'll `spawn` this client to ensure it
            // runs concurrently with all other clients, for now ignoring any errors
            // that we see.
            let msg = writes.then(move |_| Ok(()));

            tokio::spawn(msg)
        });

    tokio::run(done);
    Ok(())
}

impl Request {
    fn parse(input: &str) -> Result<Request, String> {
        let mut parts = input.splitn(3, " ");
        match parts.next() {
            Some("GET") => {
                let key = match parts.next() {
                    Some(key) => key,
                    None => return Err(format!("GET must be followed by a key")),
                };
                if parts.next().is_some() {
                    return Err(format!("GET's key must not be followed by anything"));
                }
                Ok(Request::Get {
                    key: key.to_string(),
                })
            }
            Some("SET") => {
                let key = match parts.next() {
                    Some(key) => key,
                    None => return Err(format!("SET must be followed by a key")),
                };
                let value = match parts.next() {
                    Some(value) => value,
                    None => return Err(format!("SET needs a value")),
                };
                Ok(Request::Set {
                    key: key.to_string(),
                    value: value.to_string(),
                })
            }
            Some(cmd) => Err(format!("unknown command: {}", cmd)),
            None => Err(format!("empty input")),
        }
    }
}

impl Response {
    fn serialize(&self) -> String {
        match *self {
            Response::Value { ref key, ref value } => format!("{} = {}", key, value),
            Response::Set {
                ref key,
                ref value,
                ref previous,
            } => format!("set {} = `{}`, previous: {:?}", key, value, previous),
            Response::Error { ref msg } => format!("error: {}", msg),
        }
    }
}