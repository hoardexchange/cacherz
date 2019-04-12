use actix_web::{
    http, middleware, server, App, HttpResponse, State, Error, Query
};
use db::cachedb::CacheDB;
use db::reader::{get_event_by_key, get_events_by_prefix, Event};
use std::collections::HashMap;
use std::str::from_utf8;
use serde_json;

struct WebActor {
    db: CacheDB,
    prefix: usize
}
#[derive(Serialize, Deserialize)]
struct JsonWebResponse {
  status: String,
  data: Vec<Event>,
  msg: Option<String>
}

enum ContentType {
  JsonApplication
}

enum QueryStringType {
  I32(i32),
  PureString(String)
} 

fn parse_string_by_prefix(string_key: String, prefix_size: usize) -> String {
  if string_key.clone().len() >= prefix_size {
    from_utf8(string_key[..prefix_size].as_bytes()).unwrap().to_string()
  } else {
    let mut vec_query = Vec::from(string_key.clone());
    let extend_query_size = prefix_size - string_key.clone().len();
    vec_query.extend(&vec![b'-'; extend_query_size]);
    from_utf8(&vec_query).unwrap().to_string()
  }
}

fn build_query(key: String, block_nr: String, log_nr: String, prefix_size: usize) -> String {
  let parsed_key: String = parse_string_by_prefix(key, prefix_size);
  let parsed_blocknr: String = parse_string_by_prefix(block_nr, 15);
  let parsed_lognr: String = parse_string_by_prefix(log_nr, 15);
  format!("{}{}{}", parsed_key, parsed_blocknr, parsed_lognr)
}

fn parse_query_string(query: Query<HashMap<String, String>>, query_name: &str, query_string_type: QueryStringType) -> Result<QueryStringType, String> {
  let query_string: String = query.get(query_name).expect(&format!("There is no requested param: {}", query_name)).to_string();
  match query_string_type {
    QueryStringType::I32(_) => {
      let i_32: Result<QueryStringType, String> = query_string.parse()
        .and_then(|i| {
          Ok(QueryStringType::I32(i))
        })
        .map_err( |_| {
          format!("Can not parse param: {} into integer", query_string)}
        );
      i_32
    },
    QueryStringType::PureString(_) => {
      Ok(QueryStringType::PureString(query_string.to_string()))
    }
  }
}

fn err_msg(msg: String) -> String {
  let mut _hm: HashMap<String, String> = HashMap::new();
  _hm.insert("status".to_string(), "error".to_string());
  _hm.insert("message".to_string(), msg);
  serde_json::to_string(&_hm).unwrap()
}

fn get_last_event_from_db((state, query_string): (State<WebActor>, Query<HashMap<String, String>>)) -> Result<HttpResponse, Error> {
  let event_name: String = match query_string.get("event_name") {
    Some(e_n) => e_n.to_string(),
    None => return Ok(HttpResponse::Ok().content_type("json/application").body("There is no requested param event_name".to_string()))
  };
  let event_name_prefixed: String = parse_string_by_prefix(event_name, state.prefix);
  let last_event_block_log = get_event_by_key(state.db.clone(), String::from("aggregations"), event_name_prefixed);
  match last_event_block_log {
    Ok(last_event) => {
      Ok(HttpResponse::Ok().content_type("application/json").body(serde_json::to_string(&JsonWebResponse{status: String::from("ok"), data: vec![last_event], msg: Some(String::from(""))}).unwrap()))
    },
    Err(error) => {
      Ok(HttpResponse::Ok().content_type("application/json").body(serde_json::to_string(&JsonWebResponse{status: String::from("error"), data: Vec::new(), msg: Some(error)}).unwrap()))
    }
  }
}

fn get_events((state, query_string): (State<WebActor>, Query<HashMap<String, String>>)) -> Result<HttpResponse, Error> {
  let column_family: String = match query_string.get("column_family") {
    Some(cf) => cf.to_string(),
    None => return Ok(HttpResponse::Ok().content_type("json/application").body("There is no requested param column_family".to_string()))
  };
  let key: String = match query_string.get("key") {
    Some(q) => q.to_string(),
    None => return Ok(HttpResponse::Ok().content_type("json/application").body(err_msg("There is no requested param key".to_string())))
  };
  let block_nr: String = match query_string.get("block") {
    Some(q) => q.to_string(),
    None => return Ok(HttpResponse::Ok().content_type("json/application").body(err_msg("There is no requested param block".to_string())))
  };
  let log_nr: String = match query_string.get("log") {
    Some(q) => q.to_string(),
    None => return Ok(HttpResponse::Ok().content_type("json/application").body(err_msg("There is no requested param log".to_string())))
  };
  let size: i32 = match query_string.get("size") {
    Some(s) => s.parse().unwrap(),
    None => return Ok(HttpResponse::Ok().content_type("json/application").body(err_msg("There is no requested param size".to_string())))
  };
  let method: String = match query_string.get("method") {
    Some(m) => m.to_string(),
    None => return Ok(HttpResponse::Ok().content_type("json/application").body(err_msg("There is no requested param method".to_string())))
  };
  let query: String = build_query(key, block_nr, log_nr, state.prefix);
  let result: JsonWebResponse = match method.as_ref() {
      "prefix" => {
        let get_result: Result<Vec<Event>, String> = get_events_by_prefix(state.db.clone(), column_family, query, size, true, state.prefix);
        match get_result {
          Ok(event_results) => JsonWebResponse{status: String::from("ok"), data: event_results, msg: None},
          Err(error_msg) => JsonWebResponse{status: String::from("error"), data: vec![], msg: Some(error_msg)}
        }
      },
      "key" => {
        let get_result: Result<Event, String> = get_event_by_key(state.db.clone(), column_family, query);
        match get_result {
          Ok(event_result) => JsonWebResponse{status: String::from("ok"), data: vec![event_result], msg: None},
          Err(error_msg) => JsonWebResponse{status: String::from("error"), data: vec![], msg: Some(error_msg)}
        }
      },
      &_ => {
        JsonWebResponse{status: String::from("error"), data: vec![], msg: Some(String::from("Wrong method. Method of quering should be prefix of key"))}
      }
  };
  let result_hm = json!(result);
  Ok(HttpResponse::Ok().content_type("application/json").body(serde_json::to_string(&result_hm).unwrap()))
}

pub fn run(host: String, port: String, db: CacheDB, prefix: usize) {
  server::new(move || {
        App::with_state(WebActor{db: db.clone(), prefix: prefix})
            // enable logger
            .middleware(middleware::Logger::default())
            .resource("/get_events/", |r| r.method(http::Method::GET).with(get_events))
            .resource("/last_event/", |r| r.method(http::Method::GET).with(get_last_event_from_db))
    }).bind(format!("{}:{}", host, port))
        .unwrap()
        .start();
}