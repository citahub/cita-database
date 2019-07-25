use crate::error::DatabaseError;
use rocksdb::DBIterator;

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
    fn get(
        &self,
        category: Option<DataCategory>,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, DatabaseError>;

    fn get_batch(
        &self,
        category: Option<DataCategory>,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, DatabaseError>;

    fn insert(
        &self,
        category: Option<DataCategory>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), DatabaseError>;

    fn insert_batch(
        &self,
        category: Option<DataCategory>,
        keys: Vec<Vec<u8>>,
        values: Vec<Vec<u8>>,
    ) -> Result<(), DatabaseError>;

    fn contains(&self, category: Option<DataCategory>, key: &[u8]) -> Result<bool, DatabaseError>;

    fn remove(&self, category: Option<DataCategory>, key: &[u8]) -> Result<(), DatabaseError>;

    fn remove_batch(
        &self,
        category: Option<DataCategory>,
        keys: &[Vec<u8>],
    ) -> Result<(), DatabaseError>;

    fn restore(&mut self, new_db: &str) -> Result<(), DatabaseError>;

    fn iterator(&self, category: Option<DataCategory>) -> Result<DBIterator, DatabaseError>;
}
