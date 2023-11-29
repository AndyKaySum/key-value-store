use serde::{Deserialize, Serialize};

pub type Key = i64;
pub type Value = i64;
pub type Size = usize; //for lengths and capacities

pub type Level = usize; //LSM level number
pub type Run = usize; //Run number within LSM level
pub type Page = usize; //Page index (assumes consistent page sizes)

pub type Depth = usize; //Depth in a B-tree
pub type Node = usize; //Node index in a B-tree

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
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
