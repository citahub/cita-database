# 数据库接口

可分为通用的数据库接口、`RocksDB` 自身的接口及测试使用的接口。

## 通用的数据库接口

```rust
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
fn iterator(&self, category: Option<DataCategory>) -> Option<DBIterator>;
fn close(&mut self);
```

* get: 获取指定数据种类的指定 key 的值
* get_batch: 对 get 的批量操作，批量获取 key 的值列表
* insert: 插入指定数据种类的指定 key 的值
* insert_batch: 对 insert 的批量操作，批量插入 keys 的值
* contains: 验证指定数据种类的 key 是否存在
* remove: 移除指定数据种类的 key 的值
* remove_batch: 对 remove 的批量操作，批量移除 keys 的值
* restore: 恢复一个新的数据库，同时把已有老的数据库备份
* iterator: 对指定数据种类进行迭代
* close: 关闭数据库

## RocksDB 接口

```rust
pub fn open_default(path: &str) -> Result<Self>; 
pub fn open(path: &str, config: &Config) -> Result<Self>;
```

* open_default: 使用默认的配置打开数据库
* open: 使用指定配置打开数据库

## 测试使用的接口

```rust
fn clean_cf(&self);
fn clean_db(&self);
```

* clean_cf: 清除 RocksDB 的所有 columns
* clean_db: 清楚数据库文件
