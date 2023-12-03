pub mod array_sst;
pub mod btree_sst;
mod btree_util;
pub mod sst_util;

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

    ///Search for specific key
    fn get(
        &self,
        db_name: &str,
        level: Level,
        run: Run,
        key: Key,
        num_entries: Size,
        buffer_pool: Option<&mut BufferPool>,
    ) -> io::Result<Option<Value>>;

    //Search for a specific key using binary search explicitly
    fn binary_search_get(
        &self,
        db_name: &str,
        level: Level,
        run: Run,
        key: Key,
        num_entries: Size,
        buffer_pool: Option<&mut BufferPool>,
    ) -> io::Result<Option<Value>>;

    ///Range scan operation. NOTE: key range is inclusive
    fn scan(
        &self,
        db_name: &str,
        level: Level,
        run: Run,
        key_range: (Key, Key),
        num_entries: Size,
        buffer_pool: Option<&mut BufferPool>,
    ) -> io::Result<Vec<(Key, Value)>>;

    ///Range scan operation using binary search explicitly. NOTE: key range is inclusive
    fn binary_search_scan(
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

    ///Compact all SST runs in a level into a single SST run and update entry_counts to reflect that
    fn compact(
        &self,
        db_name: &str,
        level: Level,
        entry_counts: &mut Vec<Size>,
        discard_tombstones: bool,
        buffer_pool: Option<&mut BufferPool>,
    ) -> io::Result<()>;
}
