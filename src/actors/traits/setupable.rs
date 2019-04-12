use actors::structs::settings::Settings;
use std::collections::hash_map::HashMap;

pub trait Setupable {

  fn get_settings(&self) -> HashMap<String, Settings>;

  fn get_settings_data_default(&self, data: &str, default: Settings) -> Settings {
    let settings = self.get_settings();
    // let _temp_settings = settings.get(data).clone();
    match settings.get(data) {
        Some (settings_data) => {
            settings_data.clone()
        },
        None => default 
    }
  }
}