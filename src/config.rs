// Default config
pub const BACKGROUND_FLUSHES: i32 = 2;
pub const BACKGROUND_COMPACTIONS: i32 = 2;
pub const WRITE_BUFFER_SIZE: usize = 4 * 64 * 1024 * 1024;

/// RocksDB configuration
/// TODO https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
#[derive(Clone)]
pub struct Config {
    /// WAL
    pub wal: bool,
    /// Number of categorys
    pub category_num: Option<u32>,
    /// Number of open files
    pub max_open_files: i32,
    /// About compaction
    pub compaction: Compaction,
    /// Good value for total_threads is the number of cores.
    pub increase_parallelism: Option<i32>,
}

impl Config {
    /// Create new `Config` with default parameters and specified set of category.
    pub fn with_category_num(category_num: Option<u32>) -> Self {
        let mut config = Self::default();
        config.category_num = category_num;
        config
    }
}

impl Default for Config {
    fn default() -> Config {
        Config {
            wal: true,
            category_num: None,
            max_open_files: 512,
            compaction: Compaction::default(),
            increase_parallelism: None,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub struct Compaction {
    /// L0-L1 target file size
    pub target_file_size_base: u64,
    pub max_bytes_for_level_multiplier: Option<f64>,
    /// Sets the maximum number of concurrent background compaction jobs
    pub max_background_compactions: Option<i32>,
}

impl Default for Compaction {
    fn default() -> Compaction {
        Compaction {
            target_file_size_base: 64 * 1024 * 1024,
            max_bytes_for_level_multiplier: None,
            max_background_compactions: None,
        }
    }
}
