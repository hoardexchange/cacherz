extern crate lib;
extern crate tempdir;
extern crate actix;
extern crate futures;
extern crate serde_json;
use std::collections::HashMap;
use serde_json::{Value, Map};
use lib::actors::structs::settings::Settings;
use lib::actors::main_actor::MainActor;
use lib::actors::rocks_write_actor::{MsgContentType, WriteMsg, MsgType};
use lib::actors::rocks_read_actor as RRA;
use lib::actors::main_actor::{MsgCreateReadWriteActor, MsgGetDB};
use tempdir::TempDir;
use actix::{Actor, System, AsyncContext, Arbiter, Handler, Message};
use futures::{future, Future};
use lib::db::reader::{get_by_key, get_event_by_key, get_by_key_with_default, get_events_by_prefix};

#[test]
fn test_reader_db() {
  let path = TempDir::new("_rust_rocksdb_reader").expect("");
  let path_str = path.path().to_str().unwrap();
  let mut _settings: HashMap<String, Settings> = HashMap::new();
  _settings.insert("host".to_string(), Settings::PureString("localhost".to_string()));
  _settings.insert("port".to_string(), Settings::PureString("8545".to_string()));
  let mut settings: HashMap<String, Settings> = HashMap::new();

  let settings_column_families: Vec<&'static str> = vec!("events", "aggregations", "stats", "settings", "filters", "log");
  let settings_host: String = "localhost".to_string();
  let settings_port: String = "8545".to_string();
  let settings_db_path: String = path_str.to_string();

  settings.insert("column_families".to_string(), Settings::VecStr(settings_column_families));
  settings.insert("host".to_string(), Settings::PureString(settings_host));
  settings.insert("port".to_string(), Settings::PureString(settings_port));
  settings.insert("db_path".to_string(), Settings::PureString(settings_db_path));
  settings.insert("prefix".to_string(), Settings::USize(15));
  
  let m_actor: MainActor = MainActor{system_name: "Test2".to_string(), eth_actors: Vec::new(), write_actor: None, read_actor: None, settings: Some(settings), addr: None, db: None};
  let m_actor_addr  = m_actor.clone().start();
  System::run(move || {
    let m_actor_req = m_actor_addr.clone().send(MsgCreateReadWriteActor{});
    Arbiter::spawn(m_actor_req.then(move |res| {
      match res {
        Ok(Some((_m_r_a, m_w_a, _))) => {
          {
            println!("Test write async event into Event database...");
            let write_response_1 = m_w_a.clone().send(WriteMsg{msg_content: ("Trade----------UserAddr-------0x18-0x12".to_string(), MsgContentType::PureString("{\"test\": 123}".to_string())), msg_type: MsgType::Event});
            Arbiter::spawn(write_response_1.then(move |res| {
              let unwraped_res = res.unwrap();
              assert_eq!(unwraped_res, Ok(String::from("Message: Trade----------UserAddr-------0x18-0x12 PureString(\"{\\\"test\\\": 123}\") has been written")));
              future::result(Ok(()))
            }));
          }
          {
            println!("Test read sync event from Events database...");
            let m_actor_req_db = m_actor_addr.clone().send(MsgGetDB{});
            Arbiter::spawn(m_actor_req_db.then(move |res| {
              let unwraped_res_db = res.unwrap();
              let response_db = get_by_key(unwraped_res_db.unwrap(), String::from("events"), String::from("Trade----------UserAddr-------0x18-0x12")).unwrap();
              assert_eq!(response_db, "{\"test\": 123}");
              future::result(Ok(()))
            }));
          }
          {
            println!("Test async write event into Aggregations database...");
            let write_response_2 = m_w_a.clone().send(WriteMsg{msg_content: ("Trade----------".to_string(), MsgContentType::PureString("{\"test\": \"0x18-0x14\"}".to_string())), msg_type: MsgType::Aggregation});
            Arbiter::spawn(write_response_2.then(move |res| {
              let unwraped_res = res.unwrap();
              assert_eq!(unwraped_res, Ok(String::from("Message: Trade---------- PureString(\"{\\\"test\\\": \\\"0x18-0x14\\\"}\") has been written")));
              future::result(Ok(()))
            }));
          }
          {
            println!("Test read sync event from Aggregations database...");
            let m_actor_req_db = m_actor_addr.clone().send(MsgGetDB{});
            Arbiter::spawn(m_actor_req_db.then(move |res| {
              let unwraped_res_db = res.unwrap();
              let response_db = get_by_key(unwraped_res_db.unwrap(), String::from("aggregations"), String::from("Trade----------")).unwrap();
              assert_eq!(response_db, String::from("{\"test\": \"0x18-0x14\"}"));
              future::result(Ok(()))
            }));
          }
          {
            println!("Test get event by key from Aggregations database...");
            let m_actor_req_db = m_actor_addr.clone().send(MsgGetDB{});
            Arbiter::spawn(m_actor_req_db.then(move |res| {
              let unwraped_res_db = res.unwrap();
              let response_db = get_event_by_key(unwraped_res_db.unwrap(), String::from("aggregations"), String::from("Trade----------")).unwrap();
              let response: String = String::from("0x18-0x14");
              assert_eq!(response_db.params.as_object().unwrap().get("test").unwrap().as_str().unwrap(), &response);
              future::result(Ok(()))
            }));
          }
          {
            println!("Test get event by key from Aggregations database with default value ...");
            let m_actor_req_db = m_actor_addr.clone().send(MsgGetDB{});
            Arbiter::spawn(m_actor_req_db.then(move |res| {
              let unwraped_res_db = res.unwrap();
              let response_db = get_by_key_with_default(unwraped_res_db.unwrap(), String::from("aggregations"), String::from("Tradee"), String::from("123"));
              assert_eq!(response_db, String::from("123"));
              future::result(Ok(()))
            }));
          }
          {
            println!("Test write async event into Event database...");
            let write_response_1 = m_w_a.clone().send(WriteMsg{msg_content: ("Trade----------UserAddr-------0x18-0x20".to_string(), MsgContentType::PureString("{\"test\": \"123\"}".to_string())), msg_type: MsgType::Event});
            Arbiter::spawn(write_response_1.then(move |res| {
              let unwraped_res = res.unwrap();
              assert_eq!(unwraped_res, Ok(String::from("Message: Trade----------UserAddr-------0x18-0x20 PureString(\"{\\\"test\\\": \\\"123\\\"}\") has been written")));
              future::result(Ok(()))
            }));
          }
          {
            println!("Test get event by prefix from Events database where searched value is inside prefix search.");
            let m_actor_req_db = m_actor_addr.clone().send(MsgGetDB{});
            Arbiter::spawn(m_actor_req_db.then(move |res| {
              let unwraped_res_db = res.unwrap();
              let response_db = get_events_by_prefix(unwraped_res_db.unwrap(), String::from("events"), String::from("Trade----------UserAddr-------0x18-0x12"), 10, true, 15).unwrap();
              assert_eq!(response_db.len(), 2);
              future::result(Ok(()))
            }));
          }
          assert_eq!(true, true);
        },
        Ok(None) => {
          assert_eq!(true, true);
        }
        Err(error) => {
          println!("There was an error: {:?}", error);
          assert_eq!(true, false);
        }
      };
      System::current().stop();
      future::result(Ok(()))
    }));
  });
}