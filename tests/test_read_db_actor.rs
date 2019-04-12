extern crate lib;
extern crate tempdir;
extern crate actix;
extern crate futures;
use std::collections::HashMap;
use lib::actors::structs::settings::Settings;
use lib::actors::main_actor::MainActor;
use lib::actors::rocks_write_actor::{MsgContentType, WriteMsg, MsgType};
use lib::actors::rocks_read_actor as RRA;
use lib::actors::main_actor::MsgCreateReadWriteActor;
use tempdir::TempDir;
use actix::{Actor, System, AsyncContext, Arbiter, Handler, Message};
use futures::{future, Future};

#[test]
fn test_read_db_actor() {
  let path = TempDir::new("_rust_rocksdb_read_db_test1").expect("");
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
  let m_actor: MainActor = MainActor{system_name: "Test".to_string(), eth_actors: Vec::new(), write_actor: None, read_actor: None, settings: Some(settings), addr: None, db: None};
  let m_actor_addr  = m_actor.clone().start();
  System::run(move || {
    let m_actor_req = m_actor_addr.clone().send(MsgCreateReadWriteActor{});
    Arbiter::spawn(m_actor_req.then(|res| {
      match res {
        Ok(Some((m_r_a, m_w_a, _))) => {
          {
          let m_r_a_1 = m_r_a.clone();
            let write_response_1 = m_w_a.clone().send(WriteMsg{msg_content: ("Trade----------UserAddr-------0x18-0x12".to_string(), MsgContentType::PureString("123".to_string())), msg_type: MsgType::Event});
            Arbiter::spawn(write_response_1.then(move |res| {
              match res {
                Ok(write_result) => {
                  let read_actor_reasponse_1 = m_r_a_1.clone().send(RRA::MsgRead{msg_content: RRA::MsgContentType::Prefix("Trade----------UserAddr-------0x18-0x12".to_string(), 1), msg_type: RRA::MsgType::Event});
                  
                  Arbiter::spawn(read_actor_reasponse_1.then(|res| {
                    match res {
                      Ok(response) => {
                        let parsed_response = response.unwrap()
                          .into_iter()
                          .map(|(key_vec, val_vec)| {
                            (String::from_utf8(key_vec).unwrap(), String::from_utf8(val_vec).unwrap())
                          })
                          .collect::<Vec<(String, String)>>();
                        let first_response = parsed_response.get(0).unwrap();
                        assert_eq!(first_response.0, "Trade----------UserAddr-------0x18-0x12".to_string());
                        assert_eq!(first_response.1, "123".to_string());
                        println!("Test prefix search on empty database...OK");
                      },
                      Err(error_read_response_1) => {
                        println!("READ ERROR, {}", error_read_response_1);
                        assert_eq!(true, false);
                      }
                    };
                    future::result(Ok(()))
                  }));
                },
                Err(_) => {
                  println!("Cannot write message");
                  assert_eq!(true, false);
                }
              };
              future::result(Ok(()))
            }));
          }
          {
            let _m_r_a = m_r_a.clone();
            let write_response_2 = m_w_a.send(WriteMsg{msg_content: ("Trade----------AddrUser-------0x19-0x01".to_string(), MsgContentType::PureString("123".to_string())), msg_type: MsgType::Event});
            Arbiter::spawn(write_response_2.then(move |res| {
              match res {
                Ok(write_result) => {
                  let read_actor_reasponse_2 = _m_r_a.clone().send(RRA::MsgRead{msg_content: RRA::MsgContentType::Key("Trade----------AddrUser-------0x19-0x01".to_string()), msg_type: RRA::MsgType::Event});
                  
                  Arbiter::spawn(read_actor_reasponse_2.then(|res| {
                    match res {
                      Ok(response) => {
                        let parsed_response = response.unwrap()
                          .into_iter()
                          .map(|(key_vec, val_vec)| {
                            (String::from_utf8(key_vec).unwrap(), String::from_utf8(val_vec).unwrap())
                          })
                          .collect::<Vec<(String, String)>>();
                        let first_response = parsed_response.get(0).unwrap();
                        assert_eq!(first_response.0, "Trade----------AddrUser-------0x19-0x01".to_string());
                        assert_eq!(first_response.1, "123".to_string());
                        println!("Test key search...OK");
                      },
                      Err(error_read_response_1) => {
                        println!("READ ERROR, {}", error_read_response_1);
                        assert_eq!(true, false);
                      }
                    };
                    future::result(Ok(()))
                  }));
                },
                Err(_) => {
                  println!("Cannot write message");
                  assert_eq!(true, false);
                }
              };
              future::result(Ok(()))
            }));
          }
          {
            let _m_r_a = m_r_a.clone();
            let write_response_1 = m_w_a.clone().send(WriteMsg{msg_content: ("Trade----------UserAddr-------0x18-0x10".to_string(), MsgContentType::PureString("123".to_string())), msg_type: MsgType::Event});
            Arbiter::spawn(write_response_1.then(move |res| {
              match res {
                Ok(write_result) => {
                  let read_actor_reasponse_1 = _m_r_a.clone().send(RRA::MsgRead{msg_content: RRA::MsgContentType::Prefix("Trade----------UserAddr-------0x18-0x10".to_string(), 2), msg_type: RRA::MsgType::Event});
                  
                  Arbiter::spawn(read_actor_reasponse_1.then(|res| {
                    match res {
                      Ok(response) => {
                        let parsed_response = response.unwrap()
                          .into_iter()
                          .map(|(key_vec, val_vec)| {
                            (String::from_utf8(key_vec).unwrap(), String::from_utf8(val_vec).unwrap())
                          })
                          .collect::<Vec<(String, String)>>();
                        let first_response = parsed_response.get(0).unwrap();
                        let second_response = parsed_response.get(1).unwrap();
                        assert_eq!(first_response.0, "Trade----------UserAddr-------0x18-0x10".to_string());
                        assert_eq!(first_response.1, "123".to_string());
                        assert_eq!(second_response.0, "Trade----------UserAddr-------0x18-0x12".to_string());
                        assert_eq!(second_response.1, "123".to_string());
                        println!("Test prefix search from given key...OK");
                      },
                      Err(error_read_response_1) => {
                        println!("READ ERROR, {}", error_read_response_1);
                        assert_eq!(true, false);
                      }
                    };
                    future::result(Ok(()))
                  }));
                },
                Err(_) => {
                  println!("Cannot write message");
                  assert_eq!(true, false);
                }
              };
              future::result(Ok(()))
            }));
          }
          {
            let _m_r_a = m_r_a.clone();
            let write_response_1 = m_w_a.clone().send(WriteMsg{msg_content: ("Trade----------UserAddr-------0x18-0x09".to_string(), MsgContentType::PureString("123".to_string())), msg_type: MsgType::Event});
            Arbiter::spawn(write_response_1.then(move |res| {
              match res {
                Ok(write_result) => {
                  println!("Message has been writen.");
                  let read_actor_reasponse_1 = _m_r_a.clone().send(RRA::MsgRead{msg_content: RRA::MsgContentType::Prefix("Trade----------UserAddr-------0x18-0x09".to_string(), 2), msg_type: RRA::MsgType::Event});
                  
                  Arbiter::spawn(read_actor_reasponse_1.then(|res| {
                    match res {
                      Ok(response) => {
                        let parsed_response = response.unwrap()
                          .into_iter()
                          .map(|(key_vec, val_vec)| {
                            (String::from_utf8(key_vec).unwrap(), String::from_utf8(val_vec).unwrap())
                          })
                          .collect::<Vec<(String, String)>>();
                        let first_response = parsed_response.get(0).unwrap();
                        let second_response = parsed_response.get(1).unwrap();
                        assert_eq!(parsed_response.len(), 2);
                        assert_eq!(first_response.0, "Trade----------UserAddr-------0x18-0x09".to_string());
                        assert_eq!(first_response.1, "123".to_string());
                        assert_eq!(second_response.0, "Trade----------UserAddr-------0x18-0x10".to_string());
                        assert_eq!(second_response.1, "123".to_string());
                        println!("Test subset of prefix search");
                      },
                      Err(error_read_response_1) => {
                        println!("READ ERROR, {}", error_read_response_1);
                        assert_eq!(true, false);
                      }
                    };
                    future::result(Ok(()))
                  }));
                },
                Err(_) => {
                  println!("Cannot write message");
                  assert_eq!(true, false);
                }
              };
              System::current().stop();
              future::result(Ok(()))
            }));
          }
            },
            Ok(None) => {
              println!("Something went wrong");
            },
            Err(_) => {
              println!("There is no write actor at the moment");
            }
          };
    future::result(Ok(()))
    }));

  });
}