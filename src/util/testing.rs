use super::{filename, types::Level};

#[allow(dead_code)]
pub fn setup_and_test_and_cleaup(db_name: &str, level: Level, test: &mut dyn FnMut()) {
    let dir = &filename::lsm_level_directory(db_name, level);
    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap(); //remove previous directory if panicked during tests and didn't clean up
    }
    std::fs::create_dir_all(dir).unwrap();

    test();

    std::fs::remove_dir_all(db_name).unwrap();
}
