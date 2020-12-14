use std::default::Default;
use std::path::Path;
use std::sync::Arc;

use crate::columns::map_columns;
use crate::config::{Config, BACKGROUND_FLUSHES, WRITE_BUFFER_SIZE};
use crate::database::{DataCategory, Database, Result};
use crate::error::DatabaseError;
use rocksdb::{
    BlockBasedOptions, ColumnFamily, DBCompactionStyle, DBIterator, IteratorMode, Options,
    ReadOptions, WriteBatch, WriteOptions, DB,
};
use std::fs::{metadata, remove_dir_all, rename};

// The backup db path.
const BACKUP_PATH: &str = "backup_old_db";

// For the future: Add more info about db.
#[derive(Debug)]
struct DBInfo {
    db: DB,
}

pub struct RocksDB {
    db_info: Arc<Option<DBInfo>>,
    pub config: Config,
    pub write_opts: WriteOptions,
    path: String,
}

// RocksDB guarantees synchronization
unsafe impl Sync for RocksDB {}
unsafe impl Send for RocksDB {}

impl RocksDB {
    /// Open a rocksDB with default config.
    pub fn open_default(path: &str) -> Result<Self> {
        Self::open(path, &Config::default())
    }

    /// Open rocksDB with config.
    pub fn open(path: &str, config: &Config) -> Result<Self> {
        let mut opts = Options::default();
        opts.set_write_buffer_size(WRITE_BUFFER_SIZE);
        opts.set_max_background_jobs(BACKGROUND_FLUSHES);

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
            opts.set_max_background_jobs(compactions);
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
            db_info: Arc::new(Some(DBInfo { db })),
            write_opts,
            config: config.clone(),
            path: path.to_owned(),
        })
    }

    pub fn close(&mut self) {
        let new_db = Arc::new(None);
        *Arc::get_mut(&mut self.db_info).unwrap() = Arc::try_unwrap(new_db).unwrap();
    }

    /// Restore the database from a copy at given path.
    pub fn restore(&mut self, new_db_path: &str) -> Result<()> {
        // Close it first
        // https://github.com/facebook/rocksdb/wiki/Basic-Operations#closing-a-database
        self.close();

        // Backup if the backup_path does not exist.
        let backup = !path_exists(&BACKUP_PATH);

        // Backup the old db
        if backup {
            rename(&self.path, &BACKUP_PATH)?;
        }

        // Restore the new db.
        match rename(&new_db_path, &self.path) {
            Ok(_) => {
                // Clean up the backup db.
                if backup {
                    remove_dir_all(&BACKUP_PATH)?;
                }
            }
            Err(e) => {
                // Restore the backup db.
                if backup {
                    rename(&BACKUP_PATH, &self.path)?;
                }
                return Err(DatabaseError::Internal(e.to_string()));
            }
        }

        // Reopen the database.
        let new_db = Self::open(&self.path, &self.config).unwrap().db_info;
        *Arc::get_mut(&mut self.db_info).unwrap() = Arc::try_unwrap(new_db).unwrap();
        Ok(())
    }

    pub fn iterator(&self, category: Option<DataCategory>) -> Option<DBIterator> {
        match *self.db_info {
            Some(DBInfo { ref db }) => {
                let iter = {
                    if let Some(col) = category {
                        db.iterator_cf_opt(
                            get_column(&db, col).unwrap(),
                            ReadOptions::default(),
                            IteratorMode::Start,
                        )
                    } else {
                        db.iterator_opt(IteratorMode::Start, ReadOptions::default())
                    }
                };
                Some(iter)
            }
            None => None,
        }
    }

    #[cfg(test)]
    fn clean_cf(&self) {
        let columns: Vec<_> = (0..self.config.category_num.unwrap_or(0))
            .map(|c| format!("col{}", c))
            .collect();
        let columns: Vec<&str> = columns.iter().map(|n| n as &str).collect();

        for col in columns.iter() {
            if let Some(DBInfo { ref mut db }) = *self.db_info {
                db.drop_cf(col).unwrap();
            }
        }
    }

    #[cfg(test)]
    fn clean_db(&self) {
        if path_exists(&self.path) {
            remove_dir_all(&self.path).unwrap();
        }
    }
}

impl Database for RocksDB {
    fn get(&self, category: Option<DataCategory>, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match *self.db_info {
            Some(DBInfo { ref db }) => {
                // let db = Arc::clone(&self.db);
                let key = key.to_vec();

                let mut value = db.get(&key)?;
                if let Some(category) = category {
                    let col = get_column(&db, category)?;
                    value = db.get_cf(col, &key)?;
                }
                Ok(value.map(|v| v.to_vec()))
            }
            None => Ok(None),
        }
    }

    fn get_batch(
        &self,
        category: Option<DataCategory>,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>> {
        let mut values = Vec::with_capacity(keys.len());
        if let Some(DBInfo { ref db }) = *self.db_info {
            let keys = keys.to_vec();

            for key in keys {
                let mut value = db.get(&key)?;
                if let Some(category) = category.clone() {
                    let col = get_column(&db, category)?;
                    value = db.get_cf(col, &key)?;
                }
                values.push(value.map(|v| v.to_vec()));
            }
        }

        Ok(values)
    }

    fn insert(&self, category: Option<DataCategory>, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        if let Some(DBInfo { ref db }) = *self.db_info {
            match category {
                Some(category) => {
                    let col = get_column(&db, category)?;
                    db.put_cf(col, key, value)?;
                }
                None => db.put(key, value)?,
            }
        }

        Ok(())
    }

    fn insert_batch(
        &self,
        category: Option<DataCategory>,
        keys: Vec<Vec<u8>>,
        values: Vec<Vec<u8>>,
    ) -> Result<()> {
        if keys.len() != values.len() {
            return Err(DatabaseError::InvalidData);
        }

        if let Some(DBInfo { ref db }) = *self.db_info {
            let mut batch = WriteBatch::default();

            for i in 0..keys.len() {
                match category.clone() {
                    Some(category) => {
                        let col = get_column(&db, category)?;
                        batch.put_cf(col, &keys[i], &values[i]);
                    }
                    None => batch.put(&keys[i], &values[i]),
                }
            }
            db.write(batch)?;
        }

        Ok(())
    }

    fn contains(&self, category: Option<DataCategory>, key: &[u8]) -> Result<bool> {
        match *self.db_info {
            Some(DBInfo { ref db }) => {
                let key = key.to_vec();
                let mut value = db.get(&key)?;
                if let Some(category) = category {
                    let col = get_column(&db, category)?;
                    value = db.get_cf(col, &key)?;
                }

                Ok(value.is_some())
            }
            None => Ok(false),
        }
    }

    fn remove(&self, category: Option<DataCategory>, key: &[u8]) -> Result<()> {
        if let Some(DBInfo { ref db }) = *self.db_info {
            let key = key.to_vec();
            match category {
                Some(category) => {
                    let col = get_column(&db, category)?;
                    db.delete_cf(col, key)?;
                }
                None => db.delete(key)?,
            }
        }

        Ok(())
    }

    fn remove_batch(&self, category: Option<DataCategory>, keys: &[Vec<u8>]) -> Result<()> {
        if let Some(DBInfo { ref db }) = *self.db_info {
            let keys = keys.to_vec();
            let mut batch = WriteBatch::default();

            for key in keys {
                match category.clone() {
                    Some(category) => {
                        let col = get_column(&db, category)?;
                        batch.delete_cf(col, key);
                    }
                    None => db.delete(key)?,
                }
            }
            db.write(batch)?;
        }

        Ok(())
    }

    fn restore(&mut self, new_db: &str) -> Result<()> {
        RocksDB::restore(self, new_db)
    }

    fn iterator(&self, category: Option<DataCategory>) -> Option<DBIterator> {
        RocksDB::iterator(self, category)
    }

    fn close(&mut self) {
        RocksDB::close(self)
    }
}

// Get the column from the data category.
fn get_column(db: &DB, category: DataCategory) -> Result<&ColumnFamily> {
    db.cf_handle(map_columns(category))
        .ok_or(DatabaseError::NotFound)
}

// Check the path exists.
fn path_exists(path: &str) -> bool {
    metadata(Path::new(path)).is_ok()
}

#[cfg(test)]
mod tests {
    use super::{Config, RocksDB};
    use crate::database::{DataCategory, Database};
    use crate::error::DatabaseError;
    use crate::rocksdb::{path_exists, BACKUP_PATH};
    use crate::test::{batch_op, insert_get_contains_remove};
    use std::fs::{create_dir, remove_dir_all};

    #[test]
    fn test_insert_get_contains_remove_with_category() {
        let cfg = Config::with_category_num(Some(1));
        let db = RocksDB::open(
            "rocksdb_test/get_insert_contains_remove_with_category",
            &cfg,
        )
        .unwrap();

        insert_get_contains_remove(&db, Some(DataCategory::State));
        db.clean_cf();
        db.clean_db();
    }

    #[test]
    fn test_insert_get_contains_remove() {
        let db = RocksDB::open_default("rocksdb_test/get_insert_contains_remove").unwrap();

        insert_get_contains_remove(&db, None);
        db.clean_db();
    }

    #[test]
    fn test_batch_op_with_category() {
        let cfg = Config::with_category_num(Some(1));
        let db = RocksDB::open("rocksdb_test/batch_op_with_category", &cfg).unwrap();

        batch_op(&db, Some(DataCategory::State));

        db.clean_cf();
        db.clean_db();
    }

    #[test]
    fn test_batch_op() {
        let db = RocksDB::open_default("rocksdb_test/batch_op").unwrap();

        batch_op(&db, None);
        db.clean_db();
    }

    #[test]
    fn test_insert_batch_error_with_category() {
        let cfg = Config::with_category_num(Some(1));
        let db = RocksDB::open("rocksdb_test/insert_batch_error_with_category", &cfg).unwrap();

        let data = b"test".to_vec();

        match db.insert_batch(Some(DataCategory::State), vec![data], vec![]) {
            Err(DatabaseError::InvalidData) => (), // pass
            _ => panic!("should return error DatabaseError::InvalidData"),
        }

        db.clean_cf();
        db.clean_db();
    }

    #[test]
    fn test_insert_batch_error() {
        let db = RocksDB::open_default("rocksdb_test/insert_batch_error").unwrap();

        let data = b"test".to_vec();

        match db.insert_batch(None, vec![data], vec![]) {
            Err(DatabaseError::InvalidData) => (), // pass
            _ => panic!("should return error DatabaseError::InvalidData"),
        }
        db.clean_db();
    }

    #[test]
    fn test_iterator_with_category() {
        let cfg = Config::with_category_num(Some(1));
        let db = RocksDB::open("rocksdb_test/iterator_with_category", &cfg).unwrap();

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

        assert_eq!(contents.len(), 2);
        assert_eq!(&*contents[0].0, &*data1);
        assert_eq!(&*contents[0].1, &*data1);
        assert_eq!(&*contents[1].0, &*data2);
        assert_eq!(&*contents[1].1, &*data2);

        db.clean_cf();
        db.clean_db();
    }

    #[test]
    fn test_iterator() {
        let db = RocksDB::open_default("rocksdb_test/iterator").unwrap();

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
        db.clean_db();
    }

    #[test]
    fn test_close_with_category() {
        let cfg = Config::with_category_num(Some(1));
        let mut db = RocksDB::open("rocksdb_test/close_with_category", &cfg).unwrap();
        let data = b"test".to_vec();
        db.insert(Some(DataCategory::State), data.clone(), data.clone())
            .unwrap();
        assert_eq!(db.contains(Some(DataCategory::State), &data), Ok(true));
        // Can not open it again
        match RocksDB::open_default("rocksdb_test/close_with_category") {
            // "IO error: lock : rocksdb/test_close/LOCK: No locks available"
            Err(DatabaseError::Internal(_)) => (), // pass
            _ => panic!("should return error DatabaseError::Intrnal"),
        }
        db.close();
        // Can not query
        assert_eq!(db.contains(Some(DataCategory::State), &data), Ok(false));

        // Can open it again and query
        let cfg = Config::with_category_num(Some(1));
        let db = RocksDB::open("rocksdb_test/close_with_category", &cfg).unwrap();
        assert_eq!(db.contains(Some(DataCategory::State), &data), Ok(true));
        db.clean_db();
    }

    #[test]
    fn test_close() {
        let mut db = RocksDB::open_default("rocksdb_test/close").unwrap();
        let data = b"test".to_vec();
        db.insert(None, data.clone(), data.clone()).unwrap();
        assert_eq!(db.contains(None, &data), Ok(true));
        // Can not open it again
        match RocksDB::open_default("rocksdb_test/close") {
            // "IO error: lock : rocksdb/test_close/LOCK: No locks available"
            Err(DatabaseError::Internal(_)) => (), // pass
            _ => panic!("should return error DatabaseError::Intrnal"),
        }
        db.close();
        // Can not query
        assert_eq!(db.contains(None, &data), Ok(false));

        // Can open it again and query
        let db = RocksDB::open_default("rocksdb_test/close").unwrap();
        assert_eq!(db.contains(None, &data), Ok(true));
        db.clean_db();
    }

    #[test]
    fn test_restore() {
        // No backup
        if path_exists(BACKUP_PATH) {
            remove_dir_all(BACKUP_PATH).unwrap();
        }
        let mut db = RocksDB::open_default("rocksdb_test/restore_backup").unwrap();
        let new_path_with_backup = "rocksdb_test/restore_new_db_with_backup";
        let new_db = RocksDB::open_default(new_path_with_backup).unwrap();
        let data = b"test_no_backup".to_vec();
        new_db.insert(None, data.clone(), data.clone()).unwrap();
        assert_eq!(db.contains(None, &data), Ok(false));
        assert_eq!(db.restore(new_path_with_backup), Ok(()));
        assert_eq!(db.contains(None, &data), Ok(true));
        // Clean the data
        db.clean_db();
        new_db.clean_db();

        // Backup
        let new_path_no_backup = "rocksdb_test/restore_new_db_no_backup";
        let mut db = RocksDB::open_default("rocksdb_test/restore_no_backup").unwrap();
        if !path_exists(BACKUP_PATH) {
            create_dir(BACKUP_PATH).unwrap();
        }

        match db.restore(new_path_no_backup) {
            //`Err(Internal("Directory not empty (os error 39)"))`
            Err(DatabaseError::Internal(_)) => (), // pass
            _ => panic!("should return error DatabaseError::Intrnal"),
        }

        // Clean the data
        remove_dir_all(BACKUP_PATH).unwrap();
        db.clean_db();
    }
}
