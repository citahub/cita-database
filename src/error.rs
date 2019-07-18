use rocksdb::Error as RocksError;
use std::error::Error;
use std::fmt;
use std::io::Error as IOError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseError {
    NotFound,
    InvalidData,
    Internal(String),
}

impl From<IOError> for DatabaseError {
    fn from(err: IOError) -> Self {
        DatabaseError::Internal(err.to_string())
    }
}

impl From<RocksError> for DatabaseError {
    fn from(err: RocksError) -> Self {
        DatabaseError::Internal(err.to_string())
    }
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
