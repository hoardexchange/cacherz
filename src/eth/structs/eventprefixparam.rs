#[derive(Clone, Debug)]
pub enum EventPrefixParam {
  PureString(String),
  Int32(i32),
  Int64(i64),
  UInt32(u32),
  UInt64(u64)
}