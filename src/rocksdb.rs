use std::default::Default;
use std::path::Path;
use std::sync::Arc;

use crate::columns::map_columns;
use crate::config::{Config, BACKGROUND_COMPACTIONS, BACKGROUND_FLUSHES, WRITE_BUFFER_SIZE};
use crate::database::{DataCategory, Database};
use crate::error::DatabaseError;
use rocksdb::{
    BlockBasedOptions, ColumnFamily, DBCompactionStyle, DBIterator, IteratorMode, Options,
    ReadOptions, WriteBatch, WriteOptions, DB,
};
use std::fs;
use std::io::ErrorKind;

pub struct RocksDB {
    db: Arc<DB>,
    pub config: Config,
    pub write_opts: WriteOptions,
    pub read_opts: ReadOptions,
    path: String,
}

// RocksDB guarantees synchronization
unsafe impl Sync for RocksDB {}
unsafe impl Send for RocksDB {}

impl RocksDB {
    /// Open a rocksDB with default config.
    pub fn open_default(path: &str) -> Result<Self, DatabaseError> {
        Self::open(path, &Config::default())
    }

    /// Open rocksDB with config.
    pub fn open(path: &str, config: &Config) -> Result<Self, DatabaseError> {
        let mut opts = Options::default();
        opts.set_write_buffer_size(WRITE_BUFFER_SIZE);
        opts.set_max_background_flushes(BACKGROUND_FLUSHES);
        opts.set_max_background_compactions(BACKGROUND_COMPACTIONS);
        opts.create_if_missing(true);
        // If true, any column families that didn't exist when opening the database will be created.
        opts.create_missing_column_families(true);

        let block_opts = BlockBasedOptions::default();
        opts.set_block_based_table_factory(&block_opts);

        opts.set_max_open_files(config.max_open_files);
        opts.set_use_fsync(false);
        opts.set_compaction_style(DBCompactionStyle::Level);
        opts.set_target_file_size_base(config.compaction.target_file_size_base);
        if let Some(level_multiplier) = config.compaction.max_bytes_for_level_multiplier {
            opts.set_max_bytes_for_level_multiplier(level_multiplier);
        }
        if let Some(compactions) = config.compaction.max_background_compactions {
            opts.set_max_background_compactions(compactions);
        }

        let mut write_opts = WriteOptions::default();
        if !config.wal {
            write_opts.disable_wal(true);
        }

        let columns: Vec<_> = (0..config.category_num.unwrap_or(0))
            .map(|c| format!("col{}", c))
            .collect();
        let columns: Vec<&str> = columns.iter().map(|n| n as &str).collect();
        debug!("[database] Columns: {:?}", columns);

        let db = match config.category_num {
            Some(_) => DB::open_cf(&opts, path, columns.iter())
                .map_err(|e| DatabaseError::Internal(e.to_string()))?,
            None => DB::open(&opts, path).map_err(|e| DatabaseError::Internal(e.to_string()))?,
        };

        Ok(RocksDB {
            db: Arc::new(db),
            write_opts,
            read_opts: ReadOptions::default(),
            config: config.clone(),
            path: path.to_owned(),
        })
    }

    /// Restore the database from a copy at given path.
    pub fn restore(&mut self, new_db: &str) -> Result<(), DatabaseError> {
        // Close it first
        // https://github.com/facebook/rocksdb/wiki/Basic-Operations#closing-a-database
        // TODO Use Option<db> and set it to None.

        let backup_db = Path::new("backup_db");

        let existed = match fs::rename(&self.path, &backup_db) {
            Ok(_) => true,
            Err(e) => {
                if let ErrorKind::NotFound = e.kind() {
                    false
                } else {
                    return Err(DatabaseError::Internal(e.to_string()));
                }
            }
        };

        match fs::rename(&new_db, &self.path) {
            Ok(_) => {
                // Clean up the backup.
                if existed {
                    fs::remove_dir_all(&backup_db)?;
                }
            }
            Err(e) => {
                // Restore the backup.
                if existed {
                    fs::rename(&backup_db, &self.path)?;
                }
                return Err(DatabaseError::Internal(e.to_string()));
            }
        }

        // Reopen the database
        let new_db = Self::open(&self.path, &self.config).unwrap().db;
        *Arc::get_mut(&mut self.db).unwrap() = Arc::try_unwrap(new_db).unwrap();
        Ok(())
    }

    pub fn iterator(&self, category: Option<DataCategory>) -> Result<DBIterator, DatabaseError> {
        let iter = category.map_or_else(
            || self.db.iterator_opt(IteratorMode::Start, &self.read_opts),
            |col| {
                self.db
                    .iterator_cf_opt(
                        get_column(&self.db, col).unwrap(),
                        &self.read_opts,
                        IteratorMode::Start,
                    )
                    .expect("iterator params are valid;")
            },
        );
        Ok(iter)
    }

    #[cfg(test)]
    fn clean(&self) {
        let columns: Vec<_> = (0..self.config.category_num.unwrap_or(0))
            .map(|c| format!("col{}", c))
            .collect();
        let columns: Vec<&str> = columns.iter().map(|n| n as &str).collect();

        for col in columns.iter() {
            self.db.drop_cf(col).unwrap();
        }
    }
}

impl Database for RocksDB {
    fn get(
        &self,
        category: Option<DataCategory>,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, DatabaseError> {
        let db = Arc::clone(&self.db);
        let key = key.to_vec();

        let mut value = db.get(&key)?;
        if let Some(category) = category {
            let col = get_column(&db, category)?;
            value = db.get_cf(col, &key)?;
        }

        Ok(value.map(|v| v.to_vec()))
    }

    fn get_batch(
        &self,
        category: Option<DataCategory>,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, DatabaseError> {
        let db = Arc::clone(&self.db);
        let keys = keys.to_vec();

        let mut values = Vec::with_capacity(keys.len());

        for key in keys {
            let mut value = db.get(&key)?;
            if let Some(category) = category.clone() {
                let col = get_column(&db, category)?;
                value = db.get_cf(col, &key)?;
            }
            values.push(value.map(|v| v.to_vec()));
        }

        Ok(values)
    }

    fn insert(
        &self,
        category: Option<DataCategory>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), DatabaseError> {
        let db = Arc::clone(&self.db);

        match category {
            Some(category) => {
                let col = get_column(&db, category)?;
                db.put_cf(col, key, value)?;
            }
            None => db.put(key, value)?,
        }

        Ok(())
    }

    fn insert_batch(
        &self,
        category: Option<DataCategory>,
        keys: Vec<Vec<u8>>,
        values: Vec<Vec<u8>>,
    ) -> Result<(), DatabaseError> {
        let db = Arc::clone(&self.db);

        if keys.len() != values.len() {
            return Err(DatabaseError::InvalidData);
        }

        let mut batch = WriteBatch::default();

        for i in 0..keys.len() {
            match category.clone() {
                Some(category) => {
                    let col = get_column(&db, category)?;
                    batch.put_cf(col, &keys[i], &values[i])?;
                }
                None => batch.put(&keys[i], &values[i])?,
            }
        }
        db.write(batch)?;

        Ok(())
    }

    fn contains(&self, category: Option<DataCategory>, key: &[u8]) -> Result<bool, DatabaseError> {
        let db = Arc::clone(&self.db);
        let key = key.to_vec();

        let mut value = db.get(&key)?;
        if let Some(category) = category {
            let col = get_column(&db, category)?;
            value = db.get_cf(col, &key)?;
        }

        Ok(value.is_some())
    }

    fn remove(&self, category: Option<DataCategory>, key: &[u8]) -> Result<(), DatabaseError> {
        let db = Arc::clone(&self.db);
        let key = key.to_vec();

        match category {
            Some(category) => {
                let col = get_column(&db, category)?;
                db.delete_cf(col, key)?;
            }
            None => db.delete(key)?,
        }

        Ok(())
    }

    fn remove_batch(
        &self,
        category: Option<DataCategory>,
        keys: &[Vec<u8>],
    ) -> Result<(), DatabaseError> {
        let db = Arc::clone(&self.db);
        let keys = keys.to_vec();

        let mut batch = WriteBatch::default();

        for key in keys {
            match category.clone() {
                Some(category) => {
                    let col = get_column(&db, category)?;
                    batch.delete_cf(col, key)?;
                }
                None => db.delete(key)?,
            }
        }
        db.write(batch)?;

        Ok(())
    }

    fn restore(&mut self, new_db: &str) -> Result<(), DatabaseError> {
        RocksDB::restore(self, new_db)
    }

    fn iterator(&self, category: Option<DataCategory>) -> Result<DBIterator, DatabaseError> {
        RocksDB::iterator(self, category)
    }
}

fn get_column(db: &DB, category: DataCategory) -> Result<ColumnFamily, DatabaseError> {
    db.cf_handle(map_columns(category))
        .ok_or(DatabaseError::NotFound)
}

#[cfg(test)]
mod tests {
    use super::{Config, RocksDB};
    use crate::database::{DataCategory, Database};
    use crate::error::DatabaseError;
    use crate::test::{batch_op, insert_get_contains_remove};

    #[test]
    fn test_insert_get_contains_remove_with_category() {
        let cfg = Config::with_category_num(Some(1));
        let db = RocksDB::open(
            "rocksdb/test_get_insert_contains_remove_with_category",
            &cfg,
        )
        .unwrap();

        insert_get_contains_remove(&db, Some(DataCategory::State));

        db.clean();
    }

    #[test]
    fn test_insert_get_contains_remove() {
        let db = RocksDB::open_default("rocksdb/test_get_insert_contains_remove").unwrap();

        insert_get_contains_remove(&db, None);

        db.clean();
    }

    #[test]
    fn test_batch_op_with_category() {
        let cfg = Config::with_category_num(Some(1));
        let db = RocksDB::open("rocksdb/test_batch_op_with_category", &cfg).unwrap();

        batch_op(&db, Some(DataCategory::State));

        db.clean();
    }

    #[test]
    fn test_batch_op() {
        let db = RocksDB::open_default("rocksdb/test_batch_op").unwrap();

        batch_op(&db, None);

        db.clean();
    }

    #[test]
    fn test_insert_batch_error_with_category() {
        let cfg = Config::with_category_num(Some(1));
        let db = RocksDB::open("rocksdb/test_insert_batch_error_with_category", &cfg).unwrap();

        let data = b"test".to_vec();

        match db.insert_batch(Some(DataCategory::State), vec![data], vec![]) {
            Err(DatabaseError::InvalidData) => (), // pass
            _ => panic!("should return error DatabaseError::InvalidData"),
        }

        db.clean();
    }

    #[test]
    fn test_insert_batch_error() {
        let db = RocksDB::open_default("rocksdb/test_insert_batch_error").unwrap();

        let data = b"test".to_vec();

        match db.insert_batch(None, vec![data], vec![]) {
            Err(DatabaseError::InvalidData) => (), // pass
            _ => panic!("should return error DatabaseError::InvalidData"),
        }

        db.clean();
    }

    #[test]
    fn test_iterator_with_category() {
        let cfg = Config::with_category_num(Some(1));
        let db = RocksDB::open("rocksdb/test_iterator_with_category", &cfg).unwrap();

        let data1 = b"test1".to_vec();
        let data2 = b"test2".to_vec();

        db.insert_batch(
            Some(DataCategory::State),
            vec![data1.clone(), data2.clone()],
            vec![data1.clone(), data2.clone()],
        )
        .expect("Insert data ok.");

        let contents: Vec<_> = db
            .iterator(Some(DataCategory::State))
            .into_iter()
            .flat_map(|inner| inner)
            .collect();
        println!("contents: {:?}", contents);
        assert_eq!(contents.len(), 2);
        assert_eq!(&*contents[0].0, &*data1);
        assert_eq!(&*contents[0].1, &*data1);
        assert_eq!(&*contents[1].0, &*data2);
        assert_eq!(&*contents[1].1, &*data2);
    }

    #[test]
    fn test_iterator() {
        let db = RocksDB::open_default("rocksdb/test_iterator").unwrap();

        let data1 = b"test1".to_vec();
        let data2 = b"test2".to_vec();

        db.insert_batch(
            None,
            vec![data1.clone(), data2.clone()],
            vec![data1.clone(), data2.clone()],
        )
        .expect("Insert data ok.");

        let contents: Vec<_> = db
            .iterator(None)
            .into_iter()
            .flat_map(|inner| inner)
            .collect();
        assert_eq!(contents.len(), 2);
        assert_eq!(&*contents[0].0, &*data1);
        assert_eq!(&*contents[0].1, &*data1);
        assert_eq!(&*contents[1].0, &*data2);
        assert_eq!(&*contents[1].1, &*data2);
    }
}
