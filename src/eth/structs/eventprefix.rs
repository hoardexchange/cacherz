use eth::structs::eventprefixparam::EventPrefixParam;

#[derive(Clone, Debug)]
pub struct EventPrefix {
  pub params: Vec<(EventPrefixParam, usize)>
}

impl EventPrefix {
  pub fn generate_key(self) -> String {
    self.params
      .into_iter()
      .map(| (param, param_len) | {
        let _param_string = match param {
          EventPrefixParam::PureString(p_s) => p_s,
          EventPrefixParam::Int32(i_32) => i_32.to_string(),
          EventPrefixParam::Int64(i_64) => i_64.to_string(),
          EventPrefixParam::UInt32(u_32) => u_32.to_string(),
          EventPrefixParam::UInt64(u_64) => u_64.to_string()
        };
        let mut _empty_param_with_cap: String = String::with_capacity(param_len);
        for _ in 0..(param_len - _param_string.len()) {
          _empty_param_with_cap.push('-');
        };
        format!("{}{}", _param_string, _empty_param_with_cap)
        })
      .fold("".to_string(), |acc, elem| format!("{}{}", acc, elem))
  }

  pub fn append_param(&mut self, params: Vec<(EventPrefixParam, usize)>) {
    self.params.append(&mut params.clone());
  }

  pub fn append_at_beggining(&mut self, params: Vec<(EventPrefixParam, usize)>) {
    let mut _self_params = self.params.clone();
    let mut _params = params.clone();
    _params.append(&mut _self_params);
    self.params = _params;
  }

  pub fn insert_param(&mut self, param: (EventPrefixParam, usize), index: usize) {
    if self.params.len() > index {
      self.params.insert(index, param);
    } 
  }
 }