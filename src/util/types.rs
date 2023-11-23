use serde::{Deserialize, Serialize};

pub type Key = i64;
pub type Value = i64;
pub type Size = usize; //for lengths and capacities
pub type Level = usize; //LSM level number
pub type Run = usize; //Run number within LSM level
pub type Page = usize; //Page index (assumes consistent page sizes)

pub const ENTRY_SIZE: Size = std::mem::size_of::<Key>() + std::mem::size_of::<Value>(); //might not be the best place to put this, might change later

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum CompactionPolicy {
    None,
    Basic,
    Leveled,
    Tiered,
    Dovstoevsky,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum SstImplementation {
    Array,
    Btree,
}
