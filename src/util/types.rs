use serde::{Deserialize, Serialize};

pub type Key = i64;
pub type Value = i64;
pub type Entry = (Key, Value);
pub type Size = usize; //for lengths and capacities

///LSM level number
pub type Level = usize;
///Run number within LSM level
pub type Run = usize;
///Name of database
pub type DatabaseName = str;
///Used to identify an LSM level within a specific database
pub type LevelAddress<'a> = (&'a DatabaseName, Level);
///Used to identify an SST run within a specific database
pub type RunAddress<'a> = (&'a DatabaseName, Level, Run);
///Page index (assumes consistent page sizes)
pub type Page = usize;

///Depth in a B-tree
pub type Depth = usize;
///Node index in a B-tree
pub type Node = usize;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum CompactionPolicy {
    None,
    Leveled,
    Tiered,
    Dovstoevsky,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum SstImplementation {
    Array,
    Btree,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum SstSearchAlgorithm {
    Default,
    BinarySearch,
}
