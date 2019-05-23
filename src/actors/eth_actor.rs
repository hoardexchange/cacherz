extern crate actix;
// extern crate rustc_hex;
use actix::{Actor, Addr, Context, Handler, Message, AsyncContext};
use eth::eth_json_rpc;
use std::time::Duration;
use chrono::prelude::*;
use reqwest::Client;
use ethabi::{Event, EventParam, Error};
use std::collections::HashMap;
use actors::structs::settings::Settings;
use actors::traits::setupable::Setupable;
use actors::rocks_write_actor::{RocksWriteActor, WriteMsg, MsgContentType, MsgType};
use db::cachedb::CacheDB;
use db::reader::Event as DbEvent;
use eth::structs::eventprefix::EventPrefix;
use eth::structs::eventprefixparam::EventPrefixParam;
use db::reader::{get_by_key_with_default, get_by_key};
use std::str::from_utf8;
use std::time::{SystemTime, UNIX_EPOCH};
use serde_json;


#[derive(Debug, Message)]
enum Ping {
  IsAlive(u64),
  LastEvent,
  FilterId(String),
  LastTimestamp
}

enum QueryType {
  Prefix(String, usize),
  Key(String)
}

#[derive(Message)]
pub struct GetEvents;

#[derive(Debug, Clone)]
pub struct EthActor {
  pub id: i64, // Actor inner id
  pub event: Event, // Actor handled event
  pub last_event: Option<String>, // Actor last handled event
  pub filter_id: Option<String>, // Actor connected filter_id
  pub last_timestamp: Option<u64>, // Actor last timestamp
  pub addr: Option<Addr<EthActor>>, // Actor addres necessary for messages
  pub addr_writer: Option<Addr<RocksWriteActor>>, // RockDB writer actor address
  pub db: Option<CacheDB>,
  pub last_block_log: Option<String>, // last block log for a given event filter.
  pub settings: Option<HashMap<String, Settings>>, // Actor settings
}

impl Setupable for EthActor {
  fn get_settings(&self) -> HashMap<String, Settings> {
    let settings = self.settings.clone();
    match settings {
      Some(settings_hashmap) => settings_hashmap,
      None => HashMap::new()
    }
  }
}

impl EthActor {
  pub fn create_new(event: Event, settings: HashMap<String, Settings>) -> EthActor {
    let generated_id: i64 = Utc::now().timestamp_nanos();
    return EthActor{event: event, last_event: None, filter_id: None, last_timestamp: None,
      id: generated_id, addr: None, settings: Some(settings), addr_writer: None, db: None, 
      last_block_log: None};
  }

  pub fn get_filter_id_default(&self, default: &str) -> String {
    let filter_id = self.filter_id.clone();
    match filter_id {
      Some (filter_id_data) => filter_id_data.to_string(),
      None => default.to_string()
    }
  }

  pub fn set_addr_writer(mut self, addr_writer: Addr<RocksWriteActor>)  {
    self.addr_writer = Some(addr_writer);
  }

  pub fn get_addr(&self) -> Option<Addr<EthActor>> {
    self.addr.clone()
  }

  pub fn get_event_inputs(&self) -> Vec<EventParam> {
    self.event.inputs.clone()
  }

  pub fn get_settings_host_and_port(&self) -> (String, String) {
    let host_enum = self.get_settings_data_default("host", Settings::PureString("localhost".to_string()));
    let host = if let Settings::PureString(host_str) =  host_enum {
      host_str.clone()
      } else {
        "".to_string()
      };
      let port_enum = self.get_settings_data_default("port", Settings::PureString("8545".to_string()));
      let port = if let Settings::PureString(port_str) =  port_enum {
          port_str.clone()
      } else {
        "".to_string()
      };
      (host, port)
  }

  pub fn get_prefix(&self) -> usize {
    let mut prefix: usize = 30;
    if let Some(Settings::USize(p)) = self.get_settings().get("prefix") {
      prefix = *p;
    };
    prefix
  }

  pub fn send_to_write(&self, msg_content: (String, String), msg_type: MsgType) {
    match self.addr_writer.clone() {
      Some(w_addr) => {
        let _msg_content = msg_content.clone();
        let prefix: String = generate_prefix_for_query(_msg_content.0, 30);
        let writer_msg = WriteMsg{msg_content: (prefix, MsgContentType::PureString(_msg_content.1)), msg_type: msg_type};
        match w_addr.try_send(writer_msg) {
          Ok(()) => {
            info!("Message has been send to RocksDBWriteAgent. Msg key: {}", msg_content.0);
          },
          _ => {
            error!("Cannot send message to RocksDBWriteAgent. Msg key: {}", msg_content.0);
          }
        }
      }, 
      None => {
        error!("Cannot send message to RocksDBWriteAgent. RocksDBAgent is not initialized");
      }
    }
  }

  pub fn send_to_webHook(&self, web_hook_url: String, msg: (String, String)) -> Result<(), String> {
    let http_client = Client::new();
    let (event_key, event_body) = msg;
    let mut response_hashmap = HashMap::new();
    response_hashmap.insert("key", event_key);
    response_hashmap.insert("params", serde_json::to_string(&event_body).unwrap());
    let response = http_client.post(&web_hook_url)
      .json(&response_hashmap)
      .send().map_err(| err | {
        format!("ERROR: {:?}", err)
      });
    match response {
      Ok(valid_response) => {
        info!("Response from webhook: {:?}", valid_response);
        Ok(())
      },
      Err(error_webhook) => {
        error!("WebHook error: {:?}", error_webhook);
        Err(format!("Cannot make post into {}", web_hook_url))
      }
    }
  }
  
  fn get_new_filter(&mut self ,host: String, port: String, prefix: usize) -> Option<String> {
    let event_name = self.clone().event.name;
    let last_event_result = self.clone().db.clone()
      .ok_or(String::from("There is no database atached to eth actor"))
      .and_then(| db | get_by_key(db, String::from("aggregations"), event_name.clone()))
      .and_then(| return_object | { serde_json::from_str(&return_object).map_err(| err | format!("Can not convert string into Value. Error: {}", err)) })
      .and_then(| json_map : serde_json::Value | { json_map.as_object().cloned().ok_or(String::from("Can not cast json_value into object")) })
      .and_then(| json_object | { json_object.get("last_block").cloned().ok_or(String::from("There is no last_block in json_object")) })
      .and_then(| val | { val.as_str().and_then(| s | Some(s.to_string())).ok_or(String::from("Cannot cast json_object to string")) })
      .map_err(| err | {
        warn!("There was an problem with getting new filter. {}", err);
        String::from("0x0-0x0")
      });
    let last_event = match last_event_result {
      Ok(l_e) => l_e,
      Err(err) => {
        err
      }
     };

    match eth_json_rpc::create_new_filter(host, port, format!("{:x}", self.event.signature()), self.id, last_event) {
      Ok(event_id) => {
        let filter_prefix = EventPrefix{params: vec![(EventPrefixParam::PureString(event_name.clone()), prefix)]};
        let prefix: String = generate_prefix_for_query(filter_prefix.generate_key(), 30);
        self.send_to_write((prefix, event_id.clone()), MsgType::Filter);
        Some(event_id)
      },
      Err(error) => {
        match self.db.clone() {
          Some(db) => {
            match get_by_key(db, String::from("filters"), event_name.clone()) {
              Ok(value) => {
                Some(value)
              },
              Err(err) => {
                error!("There was an error on getting value from db. Error: {}", err);
                None
              }
            }
          },
          None => {
            error!("Database is not initialized in Eth actor: {}. Error: {}", event_name, error);
            None
          }

        }
      }
    }
  }
}

impl Actor for EthActor {
  type Context = Context<EthActor>;

  fn started(&mut self, ctx: &mut Self::Context) {
    let (host, port) = self.get_settings_host_and_port();
    let prefix: usize = self.get_prefix();
    self.filter_id = self.get_new_filter(host, port, prefix);
    self.addr = Some(ctx.address());
    match self.db.clone() {
      Some(db) => {
        let last_block_log_from_db: String = get_by_key_with_default(db, String::from("aggregations"), self.event.name.clone(), String::from("0x0-0x0"));
        self.last_block_log = Some(last_block_log_from_db);
      }, 
      None => {
        error!("There is no db attached to Eth Actor: {}", self.event.name);
      }
    };
    info!("I am EthEventActor {} and I am alive! Context: {:?}", self.id.to_string(), ctx.address());
    ctx.address().do_send(GetEvents{});
  }
}

fn generate_prefix_for_query(query: String, prefix_size: usize) -> String {
  if query.clone().len() >= prefix_size {
    query
  } else {
    let mut vec_query = Vec::from(query.clone());
    let extend_query_size = prefix_size - query.clone().len();
    vec_query.extend(&vec![b'-'; extend_query_size]);
    from_utf8(&vec_query).unwrap().to_string()
  }
}


impl Handler<Ping> for EthActor {
  type Result = ();

  fn handle(&mut self, msg: Ping, ctx: &mut Context<EthActor>) {
    match msg {
      Ping::FilterId(new_filter_id) => {
        self.filter_id = Some(new_filter_id);
      },
      _ => ()
    };
  }
}

impl Handler<GetEvents> for EthActor{
  type Result = ();

  fn handle(&mut self, msg: GetEvents, ctx: &mut Context<EthActor>) {
    info!("Event {} hanlde GetEvents for event: {}", self.id, self.event.name);
    let (host, port) = self.get_settings_host_and_port();
    let actor_addr = match self.get_addr() {
      Some(addr) => addr,
      None => return (),
    };
    match self.filter_id.clone() {
      Some(_filter_id) => {
        let new_events = eth_json_rpc::get_new_events(host, port, _filter_id, self.id);
        let _decode_result: Vec<(EventPrefix, Result<HashMap<String, String>, Error>)> = match new_events {
          Ok(events) => events.result.into_iter().map(|event| {
            let mut event_params: Vec<(EventPrefixParam, usize)> = Vec::new();
            let _event = event.clone();
            let block_log = format!("{}-{}", _event.blockNumber, _event.logIndex);
            self.last_block_log = Some(block_log);
            event_params.push((EventPrefixParam::PureString(_event.blockNumber), 0));
            event_params.push((EventPrefixParam::PureString(_event.logIndex), 0));
            (EventPrefix{params: event_params}, event.decode_hashmap(self.get_event_inputs()))
            }).collect::<Vec<(EventPrefix, Result<HashMap<String, String>, Error>)>>(),
          Err(err_get_new_events) => {
            error!("Actor {:?} cannot get new events {:?}", self.id.to_string() ,err_get_new_events);
            Vec::new()
          },
        };
        _decode_result.iter().for_each(|(event_prefix, decode_result)| {
          match decode_result {
            Ok(d_result) => {
              let mut event_params: Vec<(EventPrefixParam, usize)> = Vec::new();
              event_params.push((EventPrefixParam::PureString(self.event.clone().name), 30));
              let mut _event_prefix = event_prefix.clone();
              _event_prefix.append_at_beggining(event_params);
              let json_value = serde_json::to_string(&d_result);
              match json_value {
                Ok(j_val) => {
                  let msg_content = (_event_prefix.generate_key(), j_val);
                  self.send_to_write(msg_content.clone(), MsgType::Event);
                  // send to webHook
                  let web_hook_settings = self.get_settings_data_default("webHook", Settings::Empty);
                  if let Settings::PureString(web_hook_url) = web_hook_settings {
                    let result = self.send_to_webHook(web_hook_url, msg_content);
                    match result {
                      Ok(_) => {
                        info!("Message for event {} has been sended to webHook.", self.event.name);
                      },
                      Err(err) => {
                        error!("Cannot send message into webhook for event {}, REASON: {:?}", self.event.name, err);
                      }
                    };
                  };
                },
                Err(error_convert_json_to_string) => {
                  error!("json: {:?} to string. Error: {}", d_result, error_convert_json_to_string);
                }
              };
            }, 
            Err(error_msg) => {
              error!("{}", error_msg);
            }
          }
      });
      let last_block_log_prefix = self.clone().last_block_log.unwrap();
      let since_the_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
      let ts: u128 = since_the_epoch.as_secs() as u128 * 1000 + since_the_epoch.subsec_millis() as u128;
      let msg: String = format!("{{\"last_block\": \"{}\", \"ts\": {}}}", last_block_log_prefix, ts);
      self.send_to_write((self.clone().event.name, msg), MsgType::Aggregation);
      },
      None => {
        let prefix: usize = self.get_prefix();
        self.filter_id = self.get_new_filter(host, port, prefix);
        error!("There is no available filter id for actor: {}. Please check your connection with blockchain", self.id);
      }
    };

    ctx.run_later(Duration::new(0, 1000000000), move |_, _| {
      actor_addr.do_send(GetEvents{});
      });
  } 
}