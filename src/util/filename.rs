use crate::util::types::{Level, Run};

const FILE_SEPARATOR: char = '/';
pub const SST_FILE_EXTENSION: &str = "sst";
pub const BTREE_FILE_EXTENSION: &str = "btree";
pub const BLOOM_FILTER_FILE_EXTENSION: &str = "bloom";

//Responsible for all filename conversions
pub fn config(db_name: &str) -> String {
    String::from(db_name) + "/config.bin"
}
pub fn metadata(db_name: &str) -> String {
    String::from(db_name) + "/meta.bin"
}
pub fn sst(run: Run) -> String {
    run.to_string() + "." + SST_FILE_EXTENSION
}
pub fn sst_btree(run: Run) -> String {
    run.to_string() + "." + BTREE_FILE_EXTENSION
}
pub fn bloom_filter(run: Run) -> String {
    run.to_string() + "." + BLOOM_FILTER_FILE_EXTENSION
}
pub fn sst_compaction() -> String {
    "compaction.bin".to_string()
}

pub fn lsm_level_directory(db_name: &str, level: Level) -> String {
    format!("{db_name}{0}{level}{0}", FILE_SEPARATOR)
}
pub fn sst_path(db_name: &str, level: Level, run: Run) -> String {
    format!("{db_name}{0}{level}{0}{1}", FILE_SEPARATOR, sst(run))
}
pub fn sst_btree_path(db_name: &str, level: Level, run: Run) -> String {
    format!("{db_name}{0}{level}{0}{1}", FILE_SEPARATOR, sst_btree(run))
}
pub fn bloom_filter_path(db_name: &str, level: Level, run: Run) -> String {
    format!(
        "{db_name}{0}{level}{0}{1}",
        FILE_SEPARATOR,
        bloom_filter(run)
    )
}
pub fn sst_compaction_path(db_name: &str, level: Level) -> String {
    format!(
        "{db_name}{0}{level}{0}{1}",
        FILE_SEPARATOR,
        sst_compaction()
    )
}
