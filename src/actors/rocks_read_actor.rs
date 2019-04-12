extern crate actix;
use actix::{Actor, Addr, Context, Handler, Message, AsyncContext};
use rocksdb::{SeekKey, DBVector};
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

pub fn msgtype_from_string(msg_type: String) -> Result<MsgType, String> {
    match msg_type.as_ref() {
        "events" => Ok(MsgType::Event),
        "aggregations" => Ok(MsgType::Aggregation),
        "logs" => Ok(MsgType::Log),
        "filters" => Ok(MsgType::Filter),
        "settings" => Ok(MsgType::Setting),
        "stats" => Ok(MsgType::Stat),
        _ => Err(format!("Cannot convert {} into msgtype", msg_type))
    }
}

#[derive(Debug, Clone)]
pub enum MsgContentType {
    Prefix(String, usize), // Prefix query will invoke iterator
    Key(String), // Key query will should return only one result
}

#[derive(Debug)]
pub struct MsgRead {
    pub msg_content: MsgContentType,
    pub msg_type: MsgType
}

impl Message for MsgRead {
    type Result = Result<Vec<(Vec<u8>, Vec<u8>)>, String>;
}

#[derive(Debug, Clone)]
pub struct RocksReadActor {
    pub id: i64,
    pub db: CacheDB,
    pub addr: Option<Addr<RocksReadActor>>,
}

impl RocksReadActor {
    pub fn create_new_with_db(db: CacheDB) -> Result<RocksReadActor, String> {
        let id: i64 = Utc::now().timestamp_nanos();
        return Ok(RocksReadActor {id: id, db: db, addr: None});
    }

    pub fn get_addr(self) -> Option<Addr<RocksReadActor>> {
        self.addr
    }
}

impl Handler<MsgRead> for RocksReadActor {
    type Result = Result<Vec<(Vec<u8>, Vec<u8>)>, String>;

    fn handle(&mut self, msg: MsgRead, ctx: &mut Context<RocksReadActor>) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let cf = match msg.msg_type {
            MsgType::Event => "events",
            MsgType::Aggregation => "aggregations",
            MsgType::Log => "logs",
            MsgType::Filter => "filters",
            MsgType::Setting => "settings",
            MsgType::Stat => "stats"
        };
        let result = match self.db.db.cf_handle(cf) {
            Some(cf_handle) => {
                let get_result: Vec<(Vec<u8>, Vec<u8>)> = match msg.msg_content.clone() {
                    MsgContentType::Prefix(prefix, size) => {
                        let mut return_msg: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
                        let mut iter = self.db.db.iter_cf(cf_handle);
                        iter.seek(SeekKey::Key(prefix.as_bytes()));
                        let mut key_count = 0;
                        while iter.valid() && size > key_count {
                            key_count = key_count + 1;
                            return_msg.push(iter.kv().unwrap());
                            iter.next();
                        };
                        return_msg
                    },
                    MsgContentType::Key(key)=> {
                        let get_result: Option<DBVector> =  self.db.db.get_cf(cf_handle, key.as_bytes())
                            .expect(&format!("Cannot get such key: {} from cf: {}", key, cf));
                        let db_vector = get_result.expect(&format!("Cannot get such key: {} from cf: {}", key, cf));
                        let db_vector_str = db_vector.to_utf8().expect(&format!("Cannot convert vector value to utf8"));
                        let db_vector_as_vec_bytes = db_vector_str.as_bytes().to_vec();
                      vec!((key.as_bytes().to_vec(), db_vector_as_vec_bytes))
                    },

                };
                Ok(get_result)
            },
            None => {
                error!("There is no column family as: {} for message: {:?}", cf, msg);
                Err("".to_string())
                
            },
        };
        result
    }
}

impl Actor for RocksReadActor {
    type Context = Context<RocksReadActor>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.addr = Some(ctx.address());
        println!("I am RocksReadActor {} and I am alive!", self.id);
    }
}