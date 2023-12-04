use crate::db::Database;

use super::{
    filename,
    types::{CompactionPolicy, LevelAddress, SstImplementation, SstSearchAlgorithm},
};

#[allow(dead_code)]
pub fn setup_and_test_and_cleaup(level_address: &LevelAddress, test: &mut dyn FnMut()) {
    let dir = &filename::lsm_level_directory(level_address);
    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap(); //remove previous directory if panicked during tests and didn't clean up
    }
    std::fs::create_dir_all(dir).unwrap();

    test();

    std::fs::remove_dir_all(level_address.0).unwrap();
}

#[allow(dead_code)]
pub fn part1_db_alterations(db: Database) -> Database {
    db.set_compaction_policy(CompactionPolicy::None)
        .set_sst_size_ratio(2)
        .set_sst_implementation(SstImplementation::Array)
        .set_sst_search_algorithm(SstSearchAlgorithm::Default)
        .set_enable_buffer_pool(false)
        .set_buffer_pool_capacity(1)
        .set_buffer_pool_initial_size(1)
        .set_enable_bloom_filter(false)
        .set_bloom_filter_bits_per_entry(1)
}

#[allow(dead_code)]
pub fn part2_db_alterations(db: Database) -> Database {
    db.set_compaction_policy(CompactionPolicy::None)
        .set_sst_size_ratio(2)
        .set_sst_implementation(SstImplementation::Btree)
        .set_sst_search_algorithm(SstSearchAlgorithm::Default)
        .set_enable_buffer_pool(true)
        .set_buffer_pool_capacity(10)
        .set_buffer_pool_initial_size(4)
        .set_enable_bloom_filter(false)
        .set_bloom_filter_bits_per_entry(1)
}

#[allow(dead_code)]
pub fn part3_db_alterations(db: Database) -> Database {
    db.set_compaction_policy(CompactionPolicy::Dovstoevsky)
        .set_sst_size_ratio(2)
        .set_sst_implementation(SstImplementation::Btree)
        .set_sst_search_algorithm(SstSearchAlgorithm::Default)
        .set_enable_buffer_pool(true)
        .set_buffer_pool_capacity(10)
        .set_buffer_pool_initial_size(4)
        .set_enable_bloom_filter(true)
        .set_bloom_filter_bits_per_entry(5)
}
