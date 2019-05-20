use rocksdb::{ColumnFamilyOptions, DBOptions, DB, SliceTransform, BlockBasedOptions, DBCompactionStyle};

use std::sync::Arc;
use std::str::from_utf8;


#[derive(Debug, Clone)]
pub struct CacheDB{
  pub db: Arc<rocksdb::DB>
}

struct FixedPrefixTransform {
    pub prefix_len: usize,
}

impl SliceTransform for FixedPrefixTransform {
    fn transform<'a>(&mut self, key: &'a [u8]) -> &'a [u8] {
        if self.prefix_len <= key.len() {
          &key[..self.prefix_len]
        } else {
          &key
        }
    }

    fn in_domain(&mut self, key: &[u8]) -> bool {
        key.len() >= self.prefix_len
    }
}

impl CacheDB{
  pub fn create(db_path: String, column_families: Vec<&str>, prefix_length: usize) -> Result<CacheDB, String> {
    let mut bbto = BlockBasedOptions::new();
    bbto.set_bloom_filter(10, false);
    bbto.set_whole_key_filtering(false);
    
    let mut opts = DBOptions::new();
    opts.create_if_missing(true);
    opts.set_use_fsync(false);
    opts.set_bytes_per_sync(8388608);
    opts.set_max_background_jobs(4);

    let mut cf_opts = ColumnFamilyOptions::new();
    cf_opts.set_block_based_table_factory(&bbto);
    cf_opts.set_block_cache_size_mb(1024);
    cf_opts.set_compaction_style(DBCompactionStyle::Universal);
    cf_opts.set_level_zero_stop_writes_trigger(2000);
    cf_opts.set_level_zero_slowdown_writes_trigger(0);
    cf_opts.set_disable_auto_compactions(true);
    cf_opts.set_min_write_buffer_number_to_merge(4);
    cf_opts.set_write_buffer_size(536870912);
    cf_opts.set_prefix_extractor(
      "FixedPrefixTransform",
      Box::new(FixedPrefixTransform { prefix_len: prefix_length }),
      ).expect("Cannot set prefix extractor for database");
    cf_opts.set_memtable_prefix_bloom_size_ratio(0.1 as f64);
    match DB::open_cf(opts.clone(), &db_path, vec![("default", cf_opts.clone())]) {
      Ok(mut db_handler) => {
        for cf in  column_families {
          match db_handler.create_cf(cf.clone()) {
            Ok(_) => info!("Column family: {} was created", cf),
            Err(error_cf) => {
              error!("Cannot create column family: {}. Reason: {}", cf, error_cf);
              return Err(error_cf);
              },
          };
        };
        Ok(CacheDB{db: Arc::new(db_handler)})},
      Err(error) => {
        error!("There was an error on opening default database in RocksWriteAcotor. Error: {:?}", error);
        let _generated_column_families_tuples = Self::generate_column_families_tuples(column_families.clone(), cf_opts.clone());
        match DB::open_cf(opts.clone(), &db_path, _generated_column_families_tuples) {
          Ok(db_handler_with_cf_group) => return Ok(CacheDB{db: Arc::new(db_handler_with_cf_group)}),
          Err(error_db_handler_with_cf_group) => {
            error!("Cannot open db with column families {:?}", column_families);
            return Err(error_db_handler_with_cf_group);
          }
        };
      }
    }
  }

  fn generate_column_families_tuples(column_families: Vec<&str>, cf_opts: ColumnFamilyOptions) -> Vec<(&str, ColumnFamilyOptions)> {
    column_families
      .into_iter()
      .map(|cf| {
        (cf, cf_opts.clone())
      }).collect::<Vec<(&str, ColumnFamilyOptions)>>()
  }
}