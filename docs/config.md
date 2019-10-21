# 配置

CITA 使用的一些关于 `RocksDB` 的配置，更多信息可查看 [RocksDB-Tuning-Guide]

## 关键数据结构

```rust
pub struct Config {
    /// WAL
    pub wal: bool,
    /// Number of categorys
    pub category_num: Option<u32>,
    /// Number of open files
    pub max_open_files: i32,
    /// About compaction
    pub compaction: Compaction,
    /// Good value for total_threads is the number of cores.
    pub increase_parallelism: Option<i32>,
}
```

* `WAL`: [write ahead log], 是否开启 WAL
* `category_num`: 数据种类的个数
* `max_open_files`: RocksDB 在缓存表中保存的最大数量的文件描述符
* `compaction`: [level-style-compaction]，压缩相关的配置，具体可见同文件里的 `Compaction` 结构
* `increase_parallelism`: [parallelism-options] 预留，未用

[RocksDB-Tuning-Guide]: https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
[level-style-compaction]: https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide#level-style-compaction
[parallelism-options]: https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide#parallelism-options
[write ahead log]: https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log
