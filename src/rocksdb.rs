#![allow(dead_code)]
use std::default::Default;
use std::path::Path;
use std::sync::Arc;

use crate::config::Config;
use crate::database::{DataCategory, Database};
use crate::error::DatabaseError;
use parity_rocksdb::{
    DBIterator, ReadOptions,
    WriteOptions, DB,
};

pub struct RocksDB {
    db: Arc<DB>,
    pub config: Config,
    pub write_opts: WriteOptions,
    pub read_opts: ReadOptions,
}

// RocksDB guarantees synchronization
unsafe impl Sync for RocksDB {}
unsafe impl Send for RocksDB {}

impl RocksDB {
    /// Open a rocksDB with default config.
    pub fn open_default<P: AsRef<Path>>(path: P) -> Result<Self, DatabaseError> {
        Self::open(path, &Config::default())
    }

    /// Open rocksDB with config.
    pub fn open<P: AsRef<Path>>(_path: P, _config: &Config) -> Result<Self, DatabaseError> {
        unimplemented!()
    }

    /// Restore the database from a copy at given path.
    /// TODO Add path into RocksDB
    pub fn restore<P: AsRef<Path>>(&self, _new_db: P, _old_db: P) -> Result<(), DatabaseError> {
        unimplemented!()
    }

    // TODO Implement it.
    pub fn iterator(&self, _category: Option<DataCategory>) -> Result<DBIterator, DatabaseError> {
        unimplemented!();
    }
}

impl Database for RocksDB {
    fn get(
        &self,
        _category: Option<DataCategory>,
        _key: &[u8],
    ) -> Result<Option<Vec<u8>>, DatabaseError> {
        unimplemented!();
    }

    fn get_batch(
        &self,
        _category: Option<DataCategory>,
        _keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, DatabaseError> {
        unimplemented!();
    }

    fn insert(
        &self,
        _category: Option<DataCategory>,
        _key: Vec<u8>,
        _value: Vec<u8>,
    ) -> Result<(), DatabaseError> {
        unimplemented!();
    }

    fn insert_batch(
        &self,
        _category: Option<DataCategory>,
        _keys: Vec<Vec<u8>>,
        _values: Vec<Vec<u8>>,
    ) -> Result<(), DatabaseError> {
        unimplemented!();
    }

    fn contains(&self, _category: Option<DataCategory>, _key: &[u8]) -> Result<bool, DatabaseError> {
        unimplemented!();
    }

    fn remove(&self, _category: Option<DataCategory>, _key: &[u8]) -> Result<(), DatabaseError> {
        unimplemented!();
    }

    fn remove_batch(
        &self,
        _category: Option<DataCategory>,
        _keys: &[Vec<u8>],
    ) -> Result<(), DatabaseError> {
        unimplemented!();
    }

    fn restore<P: AsRef<Path>>(&self, new_db: P, old_db: P) -> Result<(), DatabaseError> {
        RocksDB::restore(self, new_db, old_db)
    }

    fn iterator(&self, category: Option<DataCategory>) -> Result<DBIterator, DatabaseError> {
        RocksDB::iterator(self, category)
    }
}
