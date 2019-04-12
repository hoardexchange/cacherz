/// # Module Eventchanges
use eth::structs::eventresult::EventResult;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EventChanges
{
  pub id: i64,
  pub jsonrpc: String,
  pub result: Vec<EventResult>
}