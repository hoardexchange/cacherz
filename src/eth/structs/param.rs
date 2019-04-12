// use ethabi::ParamType

#[derive(Debug)]
pub struct Param {
  pub name: String,
  pub sol_type: String,
  pub is_indexed: bool
}