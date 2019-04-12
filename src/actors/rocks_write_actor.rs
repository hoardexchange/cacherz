extern crate actix;
use actix::{Actor, Addr, Context, Handler, Message, AsyncContext};
use rocksdb::{DB, Writable};
use chrono::prelude::*;
use db::cachedb::CacheDB;

#[derive(Debug)]
pub enum MsgType {
  Event,
  Aggregation,
  Log,
  Filter,
  Setting,
  Stat
}

/// Enum used for sending information into RocksWriteActor
#[derive(Debug, Clone)]
pub enum MsgContentType {
  PureString(String),
  Int32(i32),
  Int64(i64),
  UInt32(u32),
  UInt64(u64),
}

/// Struct responsible for holding all necessary data for sending message to WriteActor
#[derive(Debug)]
pub struct WriteMsg {
  pub msg_content: (String, MsgContentType),
  pub msg_type: MsgType
}

impl Message for WriteMsg {
  type Result = Result<String, String>;
}

/// WriteActor state declaration
#[derive(Debug, Clone)]
pub struct RocksWriteActor {
  pub id: i64,                              // id of actor
  pub last_write: Option<String>,           // last actor write to the db
  pub last_timestamp: Option<u16>,          // last actor timestamp
  pub db: CacheDB,                          // db
  pub addr: Option<Addr<RocksWriteActor>>,  // self addr
}

impl RocksWriteActor {
  pub fn create_new_with_db(db: CacheDB) -> Result<RocksWriteActor, String> {
    let id: i64 = Utc::now().timestamp_nanos();
    return Ok(RocksWriteActor {id: id, last_write: None, last_timestamp: None, db: db, addr: None});
  }

  pub fn get_addr(self) -> Option<Addr<RocksWriteActor>> {
    self.addr
  }
}

impl Handler<WriteMsg> for RocksWriteActor {
  type Result = Result<String, String>;

  fn handle(&mut self, msg: WriteMsg, ctx: &mut Context<RocksWriteActor>) -> Result<String, String> {
    let cf = match msg.msg_type {
      MsgType::Event => "events",
      MsgType::Aggregation => "aggregations",
      MsgType::Log => "logs",
      MsgType::Filter => "filters",
      MsgType::Setting => "settings",
      MsgType::Stat => "stats"
    };
    match self.db.db.cf_handle(cf) {
      Some(cf_handle) => {
        let put_result = match msg.msg_content.clone() {
          (key, MsgContentType::PureString(msg_string)) => {
            self.db.db.put_cf(cf_handle, key.as_bytes(), msg_string.as_bytes())
            },
          (key, MsgContentType::Int32(msg_int_i32)) => self.db.db.put_cf(cf_handle, key.as_bytes(), msg_int_i32.to_string().as_bytes()),
          (key, MsgContentType::Int64(msg_int_i64)) => self.db.db.put_cf(cf_handle, key.as_bytes(), msg_int_i64.to_string().as_bytes()),
          (key, MsgContentType::UInt32(msg_int_u32)) => self.db.db.put_cf(cf_handle, key.as_bytes(), msg_int_u32.to_string().as_bytes()),
          (key, MsgContentType::UInt64(msg_int_u64)) => self.db.db.put_cf(cf_handle, key.as_bytes(), msg_int_u64.to_string().as_bytes()),
        };
        match put_result {
          Ok(()) => {
            info!("Message for column '{}' has been writen. Content: {}: {:?}", cf, msg.msg_content.0, msg.msg_content.1);
            Ok(format!("Message: {} {:?} has been written",  msg.msg_content.0, msg.msg_content.1))
          },
          Err(error_put_result) => {
            error!("Can not save message for column {}. Message content:  {}: {:?}. Error: {}", cf, msg.msg_content.0, msg.msg_content.1, error_put_result);
            Err(error_put_result)
          }
        }
      },
      None => {
        let _err_msg = format!("There is no column family as: {} for message: {:?}", cf, msg);
        error!("There is no column family as: {} for message: {:?}", cf, msg);
        Err(_err_msg)
      }
    }
  }
}

impl Actor for RocksWriteActor {
  type Context = Context<RocksWriteActor>;

  fn started(&mut self, ctx: &mut Self::Context) {
    self.addr = Some(ctx.address());
    println!("I am RocksWriterActor {} and I am alive!", self.id);
  }
}