use serde_json::{from_str, Error, Value};
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use ethabi;

pub fn get_abi(file_path: String) -> Result<ethabi::Contract, String> {
  let result = 
    read_abi_file(file_path)
    .and_then(|abi_file_content| convert_from_json(abi_file_content))
    .and_then(|json_pure_value| search_abi(json_pure_value))
    .and_then(|json_value| convert_json_to_string(json_value))
    .and_then(|json_string| convert_to_contract(json_string))
    .map_err(|err| err);
  return result;
}

fn convert_json_to_string(json_value: Value) ->  Result<String, String> {
  let json_string = json_value.to_string();
  return Ok(json_string);
    
}

fn search_abi(json_value: Value) -> Result<Value, String> {
  return Ok(json_value["abi"].clone());
}

fn convert_to_contract(json_string: String) -> Result<ethabi::Contract, String> {
  let ethapi_contract_result = ethabi::Contract::load(json_string.as_bytes());
  match ethapi_contract_result {
    Ok(ethapi_contract) => Ok(ethapi_contract),
    Err(error) => Err(error.to_string()),
  }
}

fn read_abi_file(file_path: String) -> Result<String, String> {
  let path = Path::new(&file_path);
  let file = File::open(path);
  match file {
    Ok(mut file_handler) => {
      let mut contents = String::new();
      file_handler.read_to_string(&mut contents).unwrap();
      return Ok(contents);
    }
    Err(msg) => return Err(msg.to_string()),
  }
}

fn convert_from_json(json_content: String) -> Result<Value, String> {
  let json_value: Result<Value, Error> = from_str(&String::from(json_content));
  match json_value {
    Ok(js_val) => return Ok(js_val),
    Err(msg) => return Err(msg.to_string()),
  }
}