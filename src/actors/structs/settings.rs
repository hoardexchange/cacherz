#[derive(Clone, Debug)]
pub enum Settings {
  PureString(String),
  VecStr(Vec<&'static str>),
  VecString(Vec<String>),
  VecI64(Vec<i64>),
  VecI32(Vec<i32>),
  USize(usize),
  Empty
}