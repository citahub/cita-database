use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseError {
    NotFound,
    InvalidData,
    Internal(String),
}

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

impl Error for DatabaseError {}
impl fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let printable = match *self {
            DatabaseError::NotFound => "not found".to_owned(),
            DatabaseError::InvalidData => "invalid data".to_owned(),
            DatabaseError::Internal(ref err) => format!("internal error: {:?}", err),
        };
        write!(f, "{}", printable)
    }
}

pub trait Database: Send + Sync {
    fn get(&self, category: DataCategory, key: &[u8]) -> Result<Option<Vec<u8>>, DatabaseError>;

    fn get_batch(
        &self,
        category: DataCategory,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, DatabaseError>;

    fn insert(
        &self,
        category: DataCategory,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), DatabaseError>;

    fn insert_batch(
        &self,
        category: DataCategory,
        keys: Vec<Vec<u8>>,
        values: Vec<Vec<u8>>,
    ) -> Result<(), DatabaseError>;

    fn contains(&self, category: DataCategory, key: &[u8]) -> Result<bool, DatabaseError>;

    fn remove(&self, category: DataCategory, key: &[u8]) -> Result<(), DatabaseError>;

    fn remove_batch(&self, category: DataCategory, keys: &[Vec<u8>]) -> Result<(), DatabaseError>;
}
