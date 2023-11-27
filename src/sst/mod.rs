pub mod array_sst;
pub mod btree_sst;
mod btree_util;
mod sst_util;

use std::io;

use crate::{
    buffer_pool::BufferPool,
    util::types::{Key, Level, Run, Size, Value},
};

///Common traits needed for for any sst implementation.
/// NOTE: this trait is only responsible for a single file, not the entire level or database's files
pub trait SortedStringTable {
    ///Write entire SST
    fn write(
        &self,
        db_name: &str,
        level: Level,
        run: Run,
        entries: &[(Key, Value)],
    ) -> io::Result<()>;

    ///Deserializes entire SST
    fn read(&self, db_name: &str, level: Level, run: Run) -> io::Result<Vec<(Key, Value)>>;

    ///Binary search for specific key
    fn get(
        &self,
        db_name: &str,
        level: Level,
        run: Run,
        key: Key,
        num_entries: Size,
        buffer_pool: Option<&mut BufferPool>,
    ) -> io::Result<Option<Value>>;

    ///Range scan operation, goes page by page using direct I/O
    fn scan(
        &self,
        db_name: &str,
        level: Level,
        run: Run,
        key_range: (Key, Key),
        num_entries: Size,
        buffer_pool: Option<&mut BufferPool>,
    ) -> io::Result<Vec<(Key, Value)>>;

    //Number of entries in SST
    fn len(&self, db_name: &str, level: Level, run: Run) -> io::Result<Size>;

    ///Compact all SSTs in a level into a single SST
    fn compact(
        &self,
        db_name: &str,
        level: Level,
        entry_counts: &[Size],
        discard_tombstones: bool,
    ) -> io::Result<Size>;
}
