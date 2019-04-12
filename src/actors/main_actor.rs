extern crate actix;
extern crate futures;
use actix::{Actor, Addr, Context, System, AsyncContext, Arbiter, Handler, Message};
use actors::eth_actor::EthActor;
use actors::rocks_write_actor::RocksWriteActor;
use actors::rocks_read_actor::RocksReadActor;
use std::collections::HashMap;
use db::cachedb::CacheDB;
use actors::structs::settings::Settings;
use actors::traits::setupable::Setupable;
use futures::{future, Future};
use web::web;

/// Struct with description of Main actor state
#[derive(Clone, Debug)]
pub struct MainActor {
  pub system_name: String,                          // Main system name. This name is connected to actix backend.
  pub eth_actors: Vec<EthActor>,                    // Vector of EthActors. TODO: Change to vector of ethActor addresses
  pub write_actor: Option<Addr<RocksWriteActor>>,   // Address of RocksDB writer actor
  pub read_actor: Option<Addr<RocksReadActor>>,     // Address of RocksDB reader actor
  pub settings: Option<HashMap<String, Settings>>,  // Settings contained settings for whole system
  pub addr: Option<Addr<MainActor>>,                // Self adress, necessary for return messages.
  pub db: Option<CacheDB>,                          // Optional DB handler
}

/// Message responsible for creating WriteActor. It is a self message. 
#[derive(Debug)]
pub struct MsgCreateReadWriteActor{}
impl Message for MsgCreateReadWriteActor {
  type Result = Option<(Addr<RocksReadActor>, Addr<RocksWriteActor>, Option<CacheDB>)>;
}


/// Message responsible for creating web service
#[derive(Debug)]
pub struct MsgCreateWebService{}
impl Message for MsgCreateWebService {
  type Result = ();
}

/// Message responsible for getting db
#[derive(Debug)]
pub struct MsgGetDB{}
impl Message for MsgGetDB {
  type Result = Option<CacheDB>;
}

impl<'a> Setupable for MainActor{
  fn get_settings(&self) -> HashMap<String, Settings> {
    let settings = self.settings.clone();
    match settings {
      Some(settings_hashmap) => settings_hashmap,
      None => HashMap::new()
    }
  }
}

impl<'a> MainActor {
  pub fn run(self) {
    let main_addr = self.clone().start();
    let system_name = self.system_name.clone();
    let system = System::new(system_name.to_string());
    let create_read_write_actor = main_addr.send(MsgCreateReadWriteActor{});
    let create_web_service = main_addr.send(MsgCreateWebService{});
    Arbiter::spawn(create_read_write_actor.then(move |res| {
      match res {
        Ok(Some((read_actor_addr, write_actor_addr, db))) => {
          match self.run_event_actors(write_actor_addr.clone(), read_actor_addr.clone(), db.clone()) {
            Ok(run_event_msg) => info!("{}", run_event_msg),
            Err(err_event_msg) => error!("{}", err_event_msg)
          };
        },
        _ => println!("Something wrong"),
      }
      Arbiter::spawn(create_web_service.then(move |res| {
        match res {
          Ok(()) => info!("Web service has been created!"),
          Err(error_web_service_creation) => error!("Cannot create web service. Reason: {}", error_web_service_creation)
        }
        future::result(Ok(()))
      }));
      future::result(Ok(()))
    }));
      
    system.run();
  }

  /// Function responsible for runing all MainActor event actors holded as a "eth_actor" field
  pub fn run_event_actors(self, write_actor_addr: Addr<RocksWriteActor>, read_actor_addr: Addr<RocksReadActor>, db: Option<CacheDB>) -> Result<String, String> {
    for mut actor in self.eth_actors {
      info!("{:?}", actor);
      actor.addr_writer =  Some(write_actor_addr.clone());
      actor.db = db.clone();
      let started_actor = actor.start();
      info!("{:?}", started_actor);
    }
    Ok("All Event Actors have been run".to_string())
  }

  /// Function responsible for adding new event actor into state
  pub fn add_event_actor(mut self, new_actor: EthActor) {
    &self.eth_actors.push(new_actor);
  }

  fn create_db(&mut self) -> Result<CacheDB, String> {
    match self.db.clone() {
      Some(_db) => Ok(_db),
      None => {
        let db_path_enum = self.get_settings_data_default("db_path", Settings::PureString("".to_string()));
        let prefix_length = match  self.get_settings_data_default("prefix", Settings::USize(30)) {
          Settings::USize(prefix) => prefix,
          _ => {
            error!("Cannot get proper prefix value from Settings");
            return Err(String::from("Cannot get proper prefix value from Settings"));
          }
        };
        let db_path = if let Settings::PureString(db_path_str) =  db_path_enum {
          db_path_str.clone()
        } else {
          "".to_string()
        };
        let column_families_enum = self.get_settings_data_default("column_families", Settings::VecStr(Vec::new()));
        let column_families =  if let Settings::VecStr(column_fam_vecstr) = column_families_enum {
          column_fam_vecstr.clone()
        } else {
          let empty_column_fam: Vec<&str> = Vec::new();
          empty_column_fam
        };
        let db = CacheDB::create(db_path, column_families, prefix_length).map_err(|create_db_error| {
          System::current().stop();
          format!("Cannot create database. Reason: {}", create_db_error)
        });
        match db {
          Ok(created_database) => {
            self.db = Some(created_database.clone());
            Ok(created_database)
          },
          Err(error_created_database) => {
            error!("Cannot create database. Reason: {}", error_created_database);
            Err(error_created_database)
          }
        }   
      }
    }
  }

  /// Function resposible for creating write actor
  pub fn create_write_actor(&mut self) -> Addr<RocksWriteActor> {
    let db = self.create_db().unwrap();
    let write_actor = RocksWriteActor::create_new_with_db(db).unwrap();
    let write_actor_addr = write_actor.start();
    self.write_actor = Some(write_actor_addr.clone());
    write_actor_addr
  }

  /// Function responsible for creating web service
  pub fn create_web_service(&mut self) {
    let db = match self.db.clone() {
      Some(_db) => _db,
      None => {
        self.create_db().unwrap()   
      }
    };
    let prefix_length = match  self.get_settings_data_default("prefix", Settings::USize(30)) {
          Settings::USize(prefix) => prefix,
          _ => {
            error!("Cannot get proper prefix value from Settings");
            30
          }
    };
    web::run("localhost".to_string(), "8080".to_string(), db, prefix_length);
  }

  /// Function responsible for crating read actor
  pub fn create_read_actor(&mut self) -> Addr<RocksReadActor> {
    let db = self.create_db().unwrap();    
    let read_actor = RocksReadActor::create_new_with_db(db).unwrap();
    let read_actor_addr = read_actor.start();
    self.read_actor = Some(read_actor_addr.clone());
    read_actor_addr
  }
}

impl Handler<MsgGetDB> for MainActor {
  type Result = Option<CacheDB>;

  fn handle(&mut self, msg: MsgGetDB, ctx: &mut Context<MainActor>) -> Option<CacheDB> {
    self.clone().db
  }
}

impl Handler<MsgCreateWebService> for MainActor {
  type Result = ();

  fn handle(&mut self, msg: MsgCreateWebService, ctx: &mut Context<MainActor>) {
    self.create_web_service();
  }
}

impl Handler<MsgCreateReadWriteActor> for MainActor {
  type Result = Option<(Addr<RocksReadActor>, Addr<RocksWriteActor>, Option<CacheDB>)>;

  fn handle(&mut self, msg: MsgCreateReadWriteActor, ctx: &mut Context<MainActor>) -> Option<(Addr<RocksReadActor>, Addr<RocksWriteActor>, Option<CacheDB>)> {
    match (self.read_actor.clone(), self.write_actor.clone()) {
      (Some(read_actor_addr), Some(write_actor_addr)) => {
        info!("{}", "RockDBWriterActor and RockDBReaderActor are alive and are connected to MainActor");
        Some((read_actor_addr, write_actor_addr, self.db.clone()))
      },
      (None, None) => {
        let write_actor_addr = self.create_write_actor();
        let read_actor_addr = self.create_read_actor();
        Some((read_actor_addr, write_actor_addr, self.db.clone()))
      },
      (Some(read_actor_addr), None) => Some((read_actor_addr, self.create_write_actor(), self.db.clone())),
      (None, Some(write_actor_addr)) => Some((self.create_read_actor(), write_actor_addr, self.db.clone()))
    }
  }
}

impl Actor for MainActor {
  type Context = Context<MainActor>;

  fn started(&mut self, ctx: &mut Self::Context) {
    self.addr = Some(ctx.address());
    println!("I am MainActor and I am alive!");
  }
}