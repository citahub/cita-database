use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::database::{DataCategory, Database, Result};
use crate::error::DatabaseError;
use rocksdb::DBIterator;

// For tests
pub struct MemoryDB {
    storage: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl MemoryDB {
    pub fn open() -> Self {
        MemoryDB {
            storage: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for MemoryDB {
    fn default() -> Self {
        MemoryDB {
            storage: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Database for MemoryDB {
    fn get(&self, category: Option<DataCategory>, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let storage = Arc::clone(&self.storage);
        let key = gen_key(&category, key.to_vec());

        let storage = storage.read().map_err(|_| map_rwlock_err())?;
        let v = storage.get(&key).map(|v| v.to_vec());
        Ok(v)
    }

    fn get_batch(
        &self,
        category: Option<DataCategory>,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>> {
        let storage = Arc::clone(&self.storage);
        let keys = gen_keys(&category, keys.to_vec());

        let storage = storage.read().map_err(|_| map_rwlock_err())?;
        let values = keys
            .into_iter()
            .map(|key| storage.get(&key.to_vec()).map(|v| v.to_vec()))
            .collect();

        Ok(values)
    }

    fn insert(&self, category: Option<DataCategory>, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let storage = Arc::clone(&self.storage);
        let key = gen_key(&category, key);
        let value = value.to_vec();

        let mut storage = storage.write().map_err(|_| map_rwlock_err())?;
        storage.insert(key, value);
        Ok(())
    }

    fn insert_batch(
        &self,
        category: Option<DataCategory>,
        keys: Vec<Vec<u8>>,
        values: Vec<Vec<u8>>,
    ) -> Result<()> {
        let storage = Arc::clone(&self.storage);
        let keys = gen_keys(&category, keys);
        let values = values.to_vec();

        if keys.len() != values.len() {
            return Err(DatabaseError::InvalidData);
        }

        let mut storage = storage.write().map_err(|_| map_rwlock_err())?;
        for i in 0..keys.len() {
            let key = keys[i].to_vec();
            let value = values[i].to_vec();

            storage.insert(key, value);
        }

        Ok(())
    }

    fn contains(&self, category: Option<DataCategory>, key: &[u8]) -> Result<bool> {
        let storage = Arc::clone(&self.storage);
        let key = gen_key(&category, key.to_vec());

        let storage = storage.read().map_err(|_| map_rwlock_err())?;
        Ok(storage.contains_key(&key))
    }

    fn remove(&self, category: Option<DataCategory>, key: &[u8]) -> Result<()> {
        let storage = Arc::clone(&self.storage);
        let key = gen_key(&category, key.to_vec());

        let mut storage = storage.write().map_err(|_| map_rwlock_err())?;
        storage.remove(&key);
        Ok(())
    }

    fn remove_batch(&self, category: Option<DataCategory>, keys: &[Vec<u8>]) -> Result<()> {
        let storage = Arc::clone(&self.storage);
        let keys = gen_keys(&category, keys.to_vec());

        let mut storage = storage.write().map_err(|_| map_rwlock_err())?;
        for key in keys {
            storage.remove(&key);
        }
        Ok(())
    }

    fn restore(&mut self, _new_db: &str) -> Result<()> {
        unimplemented!()
    }

    fn iterator(&self, _category: Option<DataCategory>) -> Option<DBIterator> {
        unimplemented!()
    }

    fn close(&mut self) {
        unimplemented!();
    }
}

fn gen_key(category: &Option<DataCategory>, key: Vec<u8>) -> Vec<u8> {
    match category {
        Some(category) => match category {
            DataCategory::State => [b"state-".to_vec(), key].concat(),
            DataCategory::Headers => [b"headers-".to_vec(), key].concat(),
            DataCategory::Bodies => [b"bodies-".to_vec(), key].concat(),
            DataCategory::Extra => [b"extra-".to_vec(), key].concat(),
            DataCategory::Trace => [b"trace-".to_vec(), key].concat(),
            DataCategory::AccountBloom => [b"account-bloom-".to_vec(), key].concat(),
            DataCategory::Other => [b"other-".to_vec(), key].concat(),
        },
        None => key,
    }
}

fn gen_keys(category: &Option<DataCategory>, keys: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    keys.into_iter().map(|key| gen_key(category, key)).collect()
}

fn map_rwlock_err() -> DatabaseError {
    DatabaseError::Internal("rwlock error".to_string())
}

#[cfg(test)]
mod tests {
    use super::MemoryDB;
    use crate::database::{DataCategory, Database};
    use crate::error::DatabaseError;
    use crate::test::{batch_op, insert_get_contains_remove};

    #[test]
    fn test_insert_get_contains_remove_with_category() {
        let db = MemoryDB::open();

        insert_get_contains_remove(&db, Some(DataCategory::State));
    }

    #[test]
    fn test_insert_get_contains_remove() {
        let db = MemoryDB::open();

        insert_get_contains_remove(&db, None);
    }

    #[test]
    fn test_batch_op_with_category() {
        let db = MemoryDB::open();

        batch_op(&db, Some(DataCategory::State));
    }

    #[test]
    fn test_batch_op() {
        let db = MemoryDB::open();

        batch_op(&db, None);
        batch_op(&db, Some(DataCategory::State));
    }

    #[test]
    fn test_insert_batch_error() {
        let db = MemoryDB::open();

        let data = b"test".to_vec();

        match db.insert_batch(None, vec![data], vec![]) {
            Err(DatabaseError::InvalidData) => (), // pass
            _ => panic!("should return error DatabaseError::InvalidData"),
        }
    }
}
