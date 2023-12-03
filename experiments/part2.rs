use csc443_project::db::Database;
mod benchmarker;
mod experiment;

fn main() {
    const MEMTABLE_MB_SIZE: usize = 1;
    println!("Part 2: Experiment");
    println!("Memtable Size: {} MB\n", MEMTABLE_MB_SIZE);

    let database_alterations =
        |db: Database| -> Database { db.set_memtable_capacity_mb(MEMTABLE_MB_SIZE) };

    //TODO: 2 experiments, one with binary search and one using B-tree search
    experiment::run_and_save(Box::new(database_alterations), "part2_experiments");
}