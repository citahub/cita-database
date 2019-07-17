use crate::database::DataCategory;

// RocksDB columns
// TODO Use `Option<u32>`
/// For State
const COL_STATE: &str = "col0";
/// For Block headers
const COL_HEADERS: &str = "col1";
/// For Block bodies
const COL_BODIES: &str = "col2";
/// For Extras
const COL_EXTRA: &str = "col3";
/// For Traces
const COL_TRACE: &str = "col4";
/// TBD. For the empty accounts bloom filter.
const COL_ACCOUNT_BLOOM: &str = "col5";
const COL_OTHER: &str = "col6";

pub fn map_columns(category: DataCategory) -> &'static str {
    match category {
        DataCategory::State => COL_STATE,
        DataCategory::Headers => COL_HEADERS,
        DataCategory::Bodies => COL_BODIES,
        DataCategory::Extra => COL_EXTRA,
        DataCategory::Trace => COL_TRACE,
        DataCategory::AccountBloom => COL_ACCOUNT_BLOOM,
        DataCategory::Other => COL_OTHER,
    }
}
