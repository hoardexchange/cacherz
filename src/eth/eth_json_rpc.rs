use std::io::Read;
use serde_json;
use reqwest;
use eth::structs::eventchanges::EventChanges;

pub fn create_new_filter(eth_addr: String, eth_port: String, eth_event_hex: String, id: i64, from_block: String) -> Result<String, String> {
  let client = reqwest::Client::new();
  let eth_address: String = format!("http://{}:{}", eth_addr, eth_port);
    let json = json!({
      "jsonrpc": "2.0",
      "method": "eth_newFilter",
      "id": id,
      "params": [{
          "fromBlock": from_block,
          "topics": [format!("0x{}", eth_event_hex)]
      }
      ]
    });
    let filter_result = client.post(&eth_address).json(&json).send();
    match filter_result {
      Ok(mut result) => {
        let mut buf = String::new();
        result
          .read_to_string(&mut buf)
          .expect("Failed to read response");
        let filter_id: String =
          get_filter_id(buf.clone()).expect(&format!("Can not receive id from {}", buf));
        Ok(filter_id)
      }
      Err(error) => {
        error!("Error: {:?}", error);
        Err(error.to_string())
      },
    }
}

fn get_filter_id(filter_str: String) -> Result<String, String> {
  let filter_json: serde_json::Value =
    serde_json::from_str(&filter_str).expect("Can not convert string into json");
  let filter_json_result = filter_json["result"].as_str().expect(&format!(
    "Can not convert result: {:?} into string",
    filter_json
  ));
  return Ok(filter_json_result.to_string());
}

pub fn get_new_events(eth_addr: String, eth_port: String, filter_id: String, id: i64) -> Result<EventChanges, String> {
  let client = reqwest::Client::new();
  let eth_address: String = format!("http://{}:{}", eth_addr, eth_port);
  let json = json!({
    "jsonrpc": "2.0",
    "method": "eth_getFilterChanges",
    "id": id,
    "params": [filter_id]
  });
  let filter_result = client.post(&eth_address).json(&json).send();
  match filter_result {
    Ok(mut result) => {
      let mut buf = String::new();
      result
        .read_to_string(&mut buf)
        .expect("Failed to read response");
      let event_result: EventChanges = convert_event(buf.clone())?;
      Ok(event_result)
    }
    Err(error) => {
      error!("Error: {:?}", error);
      Err(error.to_string())
      },
  }
}

fn convert_event(event_string: String) -> Result<EventChanges, String> {
  let event_result = serde_json::from_str::<EventChanges>(&event_string);
  match event_result {
    Ok(e_r) => Ok(e_r),
    Err(error) => Err(error.to_string()),
  }
}