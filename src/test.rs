use crate::database::{DataCategory, Database};
use crate::error::DatabaseError;

#[cfg(test)]
fn get_value<K: AsRef<[u8]>, D: Database>(
    db: &D,
    key: K,
    category: Option<DataCategory>,
) -> Result<Option<Vec<u8>>, DatabaseError> {
    let value = db.get(category, key.as_ref())?;

    Ok(value)
}

#[cfg(test)]
pub fn insert_get_contains_remove<D: Database>(db: &D, category: Option<DataCategory>) {
    let data = b"test".to_vec();
    let none_exist = b"none_exist".to_vec();

    // Get
    assert_eq!(get_value(db, "test", category.clone()), Ok(None));
    //Insert and get
    db.insert(category.clone(), data.clone(), data.clone())
        .unwrap();
    assert_eq!(
        get_value(db, "test", category.clone()),
        Ok(Some(data.clone()))
    );

    // Contains
    assert_eq!(db.contains(category.clone(), &data), Ok(true));
    assert_eq!(db.contains(category.clone(), &none_exist), Ok(false));

    // Remove
    db.remove(category.clone(), &data).unwrap();
    assert_eq!(get_value(db, data, category), Ok(None));
}

#[cfg(test)]
pub fn batch_op<D: Database>(db: &D, category: Option<DataCategory>) {
    let data1 = b"test1".to_vec();
    let data2 = b"test2".to_vec();
    db.insert_batch(
        category.clone(),
        vec![data1.clone(), data2.clone()],
        vec![data1.clone(), data2.clone()],
    )
    .unwrap();

    // Insert batch
    assert_eq!(
        get_value(db, data1.clone(), category.clone()),
        Ok(Some(data1.clone()))
    );
    assert_eq!(
        get_value(db, data2.clone(), category.clone()),
        Ok(Some(data2.clone()))
    );

    db.remove_batch(category.clone(), &[data1.clone(), data2.clone()])
        .unwrap();

    // Remove batch
    assert_eq!(get_value(db, data1, category.clone()), Ok(None));
    assert_eq!(get_value(db, data2, category.clone()), Ok(None));
}
