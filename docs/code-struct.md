# 代码结构

代码实现在 `src` 目录下：

* `database.rs`: 数据库接口的定义及数据种类的定义，可根据不同种类分散存储
* `rocksdb.rs`: 使用 `RocksDB` 实现数据库接口
* `memorydb.rs`: 使用内存实现的数据库接口，为了测试使用
* `config.rs`: `RocksDB` 的配置信息
* `columns.rs`: `RocksDB` 对定义的数据种类的映射
* `error.rs`: 定义了数据库操作的一些错误信息
* `test.rs`: 对数据库接口的一些测试用例
