use csc443_project::{db::Database, util::types::ENTRY_SIZE};
mod benchmarker;
mod experiment;

fn main() {
    const MEMTABLE_MB_SIZE: usize = 10;
    println!("Part 1: Experiment");
    println!("Memtable Size: {} MB\n", MEMTABLE_MB_SIZE);

    let database_alterations =
        |db: Database| -> Database { db.resize_memtable(MEMTABLE_MB_SIZE * 2_usize.pow(20) / ENTRY_SIZE) };

    experiment::run_and_save(Box::new(database_alterations), "part1_experiments");
}
