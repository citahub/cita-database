use crate::error::DatabaseError;
use rocksdb::DBIterator;
use std::result;

pub type Result<T> = result::Result<T, DatabaseError>;

/// Specify the category of data stored, and users can store the data in a
/// decentralized manner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataCategory {
    // State
    State,
    // Block headers
    Headers,
    // Block bodies
    Bodies,
    // Extras: Block hash, receipt, and so on
    Extra,
    // TBD. Traces
    Trace,
    // TBD. Empty accounts bloom filter
    AccountBloom,
    // Keep it for compatibility
    Other,
}

pub trait Database: Send + Sync {
    fn get(&self, category: Option<DataCategory>, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn get_batch(
        &self,
        category: Option<DataCategory>,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>>;

    fn insert(&self, category: Option<DataCategory>, key: Vec<u8>, value: Vec<u8>) -> Result<()>;

    fn insert_batch(
        &self,
        category: Option<DataCategory>,
        keys: Vec<Vec<u8>>,
        values: Vec<Vec<u8>>,
    ) -> Result<()>;

    fn contains(&self, category: Option<DataCategory>, key: &[u8]) -> Result<bool>;

    fn remove(&self, category: Option<DataCategory>, key: &[u8]) -> Result<()>;

    fn remove_batch(&self, category: Option<DataCategory>, keys: &[Vec<u8>]) -> Result<()>;

    fn restore(&mut self, new_db: &str) -> Result<()>;

    // TODO Replace the DBIterator
    fn iterator(&self, category: Option<DataCategory>) -> Option<DBIterator>;

    fn close(&mut self);
}
