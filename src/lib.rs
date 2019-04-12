extern crate reqwest;
extern crate rocksdb;
// extern crate sha3;
extern crate serde;
extern crate chrono;
// extern crate hex;
extern crate ethabi;
extern crate futures;

pub mod eth;
pub mod actors;
pub mod db;
pub mod web;

#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate uuid;
#[macro_use]
extern crate actix;
extern crate tiny_keccak;
extern crate rustc_hex;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate actix_lua;
#[macro_use]
extern crate lazy_static;
extern crate actix_web;
extern crate docopt;