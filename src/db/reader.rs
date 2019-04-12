use rocksdb::{SeekKey, DBVector};
use db::cachedb::CacheDB;
use std::str::from_utf8;

#[derive(Serialize, Deserialize, Debug)]
pub struct Event {
  pub key: String,
  pub params: serde_json::Value
}

#[derive(Serialize, Deserialize)]
pub struct Filter {
  pub name: String,
  pub id: String
}

pub fn get_prefix_from_query(query: String, prefix_size: usize) -> Vec<u8> {
  let prefix: Vec<u8> = if query.clone().len() >= prefix_size {
    Vec::from(query.clone()[..prefix_size].to_string())
  } else {
    let mut vec_query = Vec::from(query.clone());
    let extend_query_size = query.clone().len() - prefix_size;
    vec_query.extend(&vec![b'-'; extend_query_size]);
    vec_query
  };
  return prefix;
}

pub fn get_events_by_prefix(db: CacheDB, column_family: String, query: String, size: i32, is_forward: bool, prefix_size: usize) -> Result<Vec<Event>, String> {
  let prefix = get_prefix_from_query(query.clone(), prefix_size);
  let query_vec_u8 = query.clone().into_bytes();
  match db.db.cf_handle(&column_family) {
    Some(cf_handle) => {
      let mut return_msg: Vec<Event> = Vec::new();
      let mut iter = db.db.iter_cf(cf_handle);
      // You can iterate db in both directions
      if is_forward == true {
        iter.seek(SeekKey::Key(query.clone().as_bytes()));
      } else {
        iter.seek_for_prev(SeekKey::Key(query.clone().as_bytes()));
      };
      let valid = iter.valid();
      if valid == true {
        let mut key_count = 0;
        let iter_key_string: String = from_utf8(iter.key()).expect(&format!("Cannot change {:?} into string", iter.key())).to_string();
        let mut key_prefix: Vec<u8> = get_prefix_from_query(iter_key_string, prefix_size);
        let mut key: Vec<u8> = Vec::from(iter.key());
        while iter.valid() && size > key_count && key_prefix == prefix && key >= query_vec_u8 {
          key_count = key_count + 1;
          match iter.kv() {
            Some((k, v)) => {
              let _k = from_utf8(&k).expect(&format!("Cannot change {:?} into string", k)).to_string();
              let _v = from_utf8(&v).expect(&format!("Cannot change {:?} into string", v));
              let event = Event{key: _k, params: serde_json::from_str(_v).map_err(|_| {String::from("Cannot decode database content to json")})?};
              return_msg.push(event);
            },
            None => {()}
          };
          key_prefix = get_prefix_from_query(from_utf8(iter.key()).expect(&format!("Cannot change {:?} into string", iter.key())).to_string(), prefix_size);
          key = Vec::from(iter.key());
          iter.next();
        };
      }
      return Ok(return_msg);
    },
    None => {
      return Err(String::from("Cannot parse your query."));
    }
  };
}

pub fn get_by_key(db: CacheDB, column_family: String, query: String) -> Result<String, String> {
  match db.db.cf_handle(&column_family) {
    Some(cf_handle) => {
      let get_result: Option<DBVector> = db.db.get_cf(cf_handle, query.as_bytes())
        .expect(&format!("Cannot get such key: {} from cf: {}", query, column_family));
      match get_result {
        Some(db_v) => {
          let db_vector_str = db_v.to_utf8().expect(&format!("Cannot convert vector value to utf8"));
          let db_vector_as_vec_string = db_vector_str.to_string();
          Ok(db_vector_as_vec_string)
        },
        None => {
          Err(format!("There is no such key as {}", query))
        }
      }
    },
    None => {Err(String::from("Cannot parse your query."))}
  }
}

pub fn get_by_key_with_default(db: CacheDB, column_family: String, query: String, default: String) -> String {
  let key_from_db: Result<String, String> = get_by_key(db, column_family, query.clone());
  match key_from_db {
    Ok(param) => param,
    Err(err_param) => {
      warn!("There is no such value as: {}. Error: {}", query, err_param);
      default
    }
  }
}

pub fn get_event_by_key(db: CacheDB,  column_family: String, query: String) -> Result<Event, String> {
  let key_from_db : Result<String, String> = get_by_key(db, column_family, query.clone());
  match key_from_db {
    Ok(param) => {
      let event = Event{key: query, params: serde_json::from_str(&param).map_err(|_| {String::from("Cannot decode database content to json")})?};
      Ok(event)
      },
    Err(error) => {
      Err(error)
    }
  }
}

pub fn get_filter_by_name(db: CacheDB, name: String) -> Result<Filter, String> {
  let column_family = String::from("filters");
  let key_from_db : Result<String, String> = get_by_key(db, column_family, name.clone());
  match key_from_db {
    Ok(param) => {
      let filter = Filter{name: name, id: param};
      Ok(filter)
      },
    Err(error) => {
      Err(error)
    }
  }
}
