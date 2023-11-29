use crate::util::types::{Level, Run};

const FILE_SEPARATOR: char = '/';

//Responsible for all filename conversions
pub fn config(db_name: &str) -> String {
    String::from(db_name) + "/config.bin"
}
pub fn metadata(db_name: &str) -> String {
    String::from(db_name) + "/meta.bin"
}
pub fn sst(run: Run) -> String {
    run.to_string() + ".sst"
}

pub fn sst_btree(run: Run) -> String {
    run.to_string() + ".btree"
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
