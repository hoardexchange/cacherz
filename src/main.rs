#[macro_use] extern crate serde_derive;
extern crate lib;
extern crate reqwest;
extern crate ethabi;
extern crate log;
extern crate docopt;

use docopt::Docopt;

use lib::eth::eth_contract_loader;
use lib::actors::main_actor::MainActor;
use lib::actors::eth_actor::EthActor;
use std::collections::HashMap;
use lib::actors::structs::settings::Settings;

const USAGE: &'static str = "
  Cacherz.

  Usage:
  cacherz --ethHost <ethHost> --ethPort <ethPort> --webHost <webHost> --webPort <webPort> --abiFilePath <abiFilePath> --rocksdbPath <rocksdbPath>
  cacherz --version

  Options:
  -h --help     Show this screen.
  --version     Show version.
  --ethHost=<ethHost>     Host of the ethereum node.
  --ethPort=<ethPort>     Port of the ethereum node.
  --webHost=<webHost>     Host of the web service.
  --webPort=<webPort>     Port of the web service.
  --abiFilePath=<abiFilePath>     Full path to abi file.
  --rocksdbPath=<rocksdbPath>     Full path to rocksdb main folder.
  --prefixSize=<prefixSize>       Size of a prefix
";

#[derive(Debug,Deserialize)]
struct Args {
  flag_ethHost: String,
  flag_ethPort: String,
  flag_webHost: String,
  flag_webPort: String,
  flag_abiFilePath: String,
  flag_rocksdbPath: String,
  flag_prefixSize: Option<usize>
}

fn main() {
  env_logger::init();
  let args: Args = Docopt::new(USAGE)
    .and_then(|d| d.deserialize())
    .unwrap_or_else(|e| e.exit());
  let mut eth_actor_settings: HashMap<String, Settings> = HashMap::new();
  eth_actor_settings.insert("host".to_string(), Settings::PureString(args.flag_ethHost.clone()));
  eth_actor_settings.insert("port".to_string(), Settings::PureString(args.flag_ethPort.clone()));
  let file_path = String::from(args.flag_abiFilePath);
  let eth_contract = eth_contract_loader::get_abi(file_path.clone())
    .expect(&format!("Can not get abi from: {}", file_path));
  let mut eth_actors : Vec<EthActor> = Vec::new();
  for event in eth_contract.events {
    eth_actors.push(EthActor::create_new(event.1, eth_actor_settings.clone()));
  }
  let mut settings: HashMap<String, Settings> = HashMap::new();

  let settings_column_families: Vec<&'static str> = vec!("events", "aggregations", "stats", "settings", "filters", "log");
  let settings_host: String = args.flag_ethHost.clone();
  let settings_port: String = args.flag_ethPort.clone();
  let settings_webPort: String = args.flag_webPort.clone();
  let settings_webHost: String = args.flag_webHost.clone();
  let settings_db_path: String = args.flag_rocksdbPath;
  let settings_prefix: Option<usize> = args.flag_prefixSize;

  let prefix = match settings_prefix {
    Some(prefix) => Settings::USize(prefix),
    None => Settings::USize(30)
  };

  settings.insert("column_families".to_string(), Settings::VecStr(settings_column_families));
  settings.insert("host".to_string(), Settings::PureString(settings_host));
  settings.insert("port".to_string(), Settings::PureString(settings_port));
  settings.insert("webPort".to_string(), Settings::PureString(settings_webPort));
  settings.insert("webHost".to_string(), Settings::PureString(settings_webHost));
  settings.insert("db_path".to_string(), Settings::PureString(settings_db_path));
  settings.insert("prefix".to_string(), prefix);
  let m_actor: MainActor = MainActor{system_name: "EventStreamer".to_string(), eth_actors: eth_actors, write_actor: None, read_actor: None, settings: Some(settings), addr: None, db: None};
  m_actor.run();
}