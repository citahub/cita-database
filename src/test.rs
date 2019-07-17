use crate::database::{DataCategory, Database, DatabaseError};

#[cfg(test)]
pub fn get_value<K: AsRef<[u8]>, D: Database>(
    db: &D,
    key: K,
) -> Result<Option<Vec<u8>>, DatabaseError> {
    let value = db.get(DataCategory::State, key.as_ref())?;

    Ok(value)
}
