use ethabi::{decode, Error, EventParam, ParamType, Token};
use rustc_hex::FromHex;
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EventResult {
  pub logIndex: String,
  pub blockNumber: String,
  pub blockHash: String,
  pub transactionHash: String,
  pub transactionIndex: String,
  pub address: String,
  pub data: String,
  pub topics: Vec<String>,
}

impl EventResult {
  fn prepare_params(self, inputs: Vec<EventParam>) -> (Vec<ParamType>, Vec<u8>) {
    let mut event_data_cleared = self.data[2..].to_string().clone();
    let mut event_topics_clone = self.topics.split_at(1).1;
    let event_sorted_results: String = inputs
      .clone()
      .into_iter()
      .map(|event_type| {
        // We have to concat data by param type (indexed params are topics, other params are data)
        if event_type.indexed == true {
          match event_topics_clone.split_first() {
            Some((first_topic, rest_of_topics)) => {
              event_topics_clone = rest_of_topics;
              first_topic[2..].to_string()
            }
            None => "".to_string(),
          }
        } else {
          // Quite complicated part. We have to split event data by 64
          // to separate each of the event param (stored in data)
          let splited_event_data = event_data_cleared.split_off(64);
          let _chunk = event_data_cleared.clone();
          event_data_cleared = splited_event_data;
          _chunk
        }
      })
      .fold("".to_string(), |acc, elem| format!("{}{}", acc, elem));

      let event_params = inputs
        .clone()
        .into_iter()
        .map(|event_type| event_type.kind)
        .collect::<Vec<ParamType>>();

      let event_sorted_results_hex = match event_sorted_results.from_hex() {
        Ok(event_hex_slice) => event_hex_slice,
        Err(err_event_hex_slice) => {
          error!(
            "Cannot convert Eth message into hex format{:?}",
            err_event_hex_slice
          );
          Vec::new()
        }
      };

    (event_params, event_sorted_results_hex)
  }

  pub fn decode_hashmap(self, inputs: Vec<EventParam>) -> Result<HashMap<String, String>, Error> {
    let (event_params, event_sorted_results_hex) = self.prepare_params(inputs.clone());
    match decode(event_params.as_slice(), event_sorted_results_hex.as_slice()) {
      Ok(decoded_params) => {
        let mut _hash_map = HashMap::new();
        let _d_params = decoded_params
          .into_iter()
          .zip(inputs.into_iter())
          .map(|(zip_param, zip_input)| {
            let zip_param_string = format!("{}", zip_param);
            let _hash_map_insert =
              _hash_map.insert(zip_input.name.clone(), zip_param_string.clone());
            (zip_input.name, zip_param_string)
          })
          .collect::<HashMap<String, String>>();
        Ok(_d_params)
      }
      Err(error_decode) => Err(error_decode),
    }
  }

  pub fn decode_vector(self, inputs: Vec<EventParam>) -> Result<Vec<(String, String)>, Error> {
    let (event_params, event_sorted_results_hex) = self.prepare_params(inputs.clone());
    match decode(event_params.as_slice(), event_sorted_results_hex.as_slice()) {
      Ok(decoded_params) => {
        let _d_params = decoded_params
          .into_iter()
          .zip(inputs.into_iter())
          .map(|(zip_param, zip_input)| {
              let zip_param_string = format!("{}", zip_param);
              (zip_input.name, zip_param_string)
          })
          .collect::<Vec<(String, String)>>();
        Ok(_d_params)
      }
      Err(error_decode) => Err(error_decode),
    }
  }
}
