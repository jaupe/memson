# memson

memson is an in-memory key-value cache to store, retrieve and aggregate JSON data. 

It is similar to Reddis but it treats JSON as a first class citizen and treats JSON as code.

## Tutorial

1. **Install memson binary to your path**

``` shell
git clone https://github.com/jaupe/memson
cd memson
cargo install
```

2. **Run memson server instance**

``` shell
memson --port 8000
```

This will by default bind to port 8000 but made it explicity in the example to show how to change it. It will write the persistence log to $HOME/memson/memson.log

3. **Connect a local client to the memson server instance**

``` shell 
nc -v 0.0.0.0 8000
```

4. **Run memson commands from local client**

``` json
{"set": ["k1", [1,2,3,4]]} //sets JSON array to key "k1"
null // it is null as it's a new entry; otherwise it would return the previous value
{"get": "k1"}
[1,2,3,4]
{"first": "k1"}
1
{"last": "k1"}
4
```


## Frequently Asked Questions

* Why use Memson over Reddis?

Redis treats JSON as strings so Redis functions cannot be used against JSON. Memson treats JSON as a first class citizen so you can use aggregation functions against JSON. Memson also has more sophisticated aggregation functions that can be together.

* Does it offer persistence if restarted?

## Cookbook

* storing JSON value by key

``` json
// sets an array to the key "foo"
{"set": ["john", "mayer"]}
```

* retrieving JSON value by key

``` json
// get key using JSON object
{"get": "foo"}
```

## Functions

* max

returns the highest value in the JSON key/val

``` json
{"max": {"get": "foo"}}
```

* min

returns the lowest value in the JSON key/val

``` json
{"min": {"get": "foo"}}
```

* avg

returns the arithemtic mean of the JSON key/val (if applicable)

``` json
{"avg": {"get": "foo"}}
```

* var

returns the variance of the JSON key/val (if applicable)

``` json
{"var": {"get": "foo"}}
```

* dev

returns the standard deviation of the JSON key/val (if applicable)

``` json
{"dev": {"get": "foo"}}
```

* sum

returns the summation of the JSON key/val (if applicable)

``` json
{"sum": {"get": "foo"}}
```

* first 

returns the first element of the JSON key/val

``` json
{"first": {"get": "foo"}}
```

* last 

returns the last element of the JSON key/val

``` json
{"first": {"get": "last"}}
```