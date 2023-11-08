use crate::memtable::Memtable;
use crate::sst::SSTable;
use crate::database::Database;

mod avl;
mod memtable;
mod sst;
mod database;
pub mod merge_k_lists;
fn main() {
    println!("Hello, world!");

}
