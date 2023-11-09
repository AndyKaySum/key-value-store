use csc443_project::db::Database;
mod benchmarker;
mod experiment;

fn main() {
    println!("Part 1: Experiment");
    println!("Memtable Size: 10 MB\n");

    let database_alterations =
        |db: Database| -> Database { db.resize_memtable(10 * 2_usize.pow(20)) };

    experiment::run(Box::new(database_alterations));
}
