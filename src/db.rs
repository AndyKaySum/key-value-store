use serde::{Deserialize, Serialize};
use std::{
    collections::{BinaryHeap, HashSet},
    fs,
};

use crate::{
    buffer_pool::BufferPool,
    memtable::Memtable,
    sst::{array_sst, btree_sst, SortedStringTable},
    util::filename,
    util::{
        system_info::{self, ENTRY_SIZE},
        types::{CompactionPolicy, Key, Level, Run, Size, SstImplementation, Value},
    },
};

#[derive(Serialize, Deserialize, Debug)]
struct Config {
    memtable_capacity: Size, //in terms of number of entries
    sst_size_ratio: Size,    //size ratio between sst levels
    sst_implementation: SstImplementation,
    enable_buffer_pool: bool,
    buffer_pool_capacity: Size,
    buffer_pool_initial_size: Size,
    compaction_policy: CompactionPolicy,
}

impl Config {
    fn new() -> Self {
        Self {
            memtable_capacity: system_info::num_entries_per_page(),
            sst_size_ratio: Database::DEFAULT_SST_SIZE_RATIO,
            sst_implementation: SstImplementation::Array,
            enable_buffer_pool: false,   //TODO: change in step 2
            buffer_pool_capacity: 0,     //TODO: change in step 2
            buffer_pool_initial_size: 0, //TODO change in step 2
            compaction_policy: CompactionPolicy::None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Metadata {
    entry_counts: Vec<Vec<Size>>, //number of entries in each SST (outer index: Level, inner Index: run)
}

impl Metadata {
    fn new() -> Self {
        Self {
            entry_counts: vec![vec![]],
        }
    }
}

#[derive(Debug)]
pub struct Database {
    name: String, //name of db (directory that holds SSTs)
    config: Config,
    metadata: Metadata,
    memtable: Memtable,
    buffer_pool: BufferPool,
}

#[allow(dead_code)]
impl Database {
    const DEFAULT_SST_SIZE_RATIO: Size = 2;
    const LEVEL_ZERO: Level = 0; //Constant for step 1 and 2, needs to be removed in step 3

    //RESERVED VALUES BELOW (not allowed for normal input)
    ///Reserved tombstone value
    const TOMBSTONE_VALUE: Value = Value::MIN;
    ///Reserved this value so we can use negative keys without errors in our min heap in scan (try negative 32::MIN and see what happens)
    const INVALID_KEY: Key = Key::MIN;
    ///Reserved DB name for when no DB is open (like a null value)
    const NO_OPEN_DB_NAME: &str = "";

    ///INTERNAL ONLY (do not make public), externally should use open()
    fn new(name: &str, config: Config, metadata: Metadata) -> Database {
        if name == Self::NO_OPEN_DB_NAME {
            panic!("\"{name}\" is an invalid Database name");
        }
        if name.contains(char::is_whitespace) {
            panic!("\"{name}\" is an invalid Database name (cannot contain whitespaces)")
        }
        let Config {
            buffer_pool_initial_size,
            buffer_pool_capacity,
            ..
        } = config;
        Database {
            name: String::from(name),
            config,
            metadata,
            memtable: Memtable::new(),
            buffer_pool: BufferPool::new(buffer_pool_initial_size, buffer_pool_capacity),
        }
    }
    //GETTERS AND SETTERS (start)
    pub fn name(&self) -> String {
        self.name.clone()
    }
    pub fn memtable_capacity(&self) -> Size {
        self.config.memtable_capacity
    }
    pub fn set_memtable_capacity(mut self, memtable_capacity: Size) -> Self {
        if memtable_capacity < 1 {
            panic!("{memtable_capacity} is an invalid memtable capacity");
        }
        self.config.memtable_capacity = memtable_capacity;
        self
    }
    pub fn set_memtable_capacity_mb(self, memtable_capacity_mb: Size) -> Self {
        self.set_memtable_capacity(memtable_capacity_mb * 2_usize.pow(20) / ENTRY_SIZE)
    }
    pub fn sst_size_ratio(&self) -> Size {
        self.config.sst_size_ratio
    }
    pub fn set_sst_size_ratio(mut self, sst_size_ratio: Size) -> Self {
        self.config.sst_size_ratio = sst_size_ratio;
        self
    }
    pub fn sst_implementation(&self) -> SstImplementation {
        self.config.sst_implementation
    }
    pub fn set_sst_implementation(mut self, sst_implementation: SstImplementation) -> Self {
        self.config.sst_implementation = sst_implementation;
        self
    }
    pub fn enable_buffer_pool(&self) -> bool {
        self.config.enable_buffer_pool
    }
    pub fn set_enable_buffer_pool(mut self, enable_buffer_pool: bool) -> Self {
        self.config.enable_buffer_pool = enable_buffer_pool;
        self
    }
    pub fn buffer_pool_capacity(&self) -> Size {
        self.config.buffer_pool_capacity
    }
    pub fn set_buffer_pool_capacity(mut self, buffer_pool_capacity: Size) -> Self {
        self.config.buffer_pool_capacity = buffer_pool_capacity;
        self
    }
    pub fn buffer_pool_initial_size(&self) -> Size {
        self.config.buffer_pool_initial_size
    }
    pub fn set_buffer_pool_initial_size(mut self, buffer_pool_initial_size: Size) -> Self {
        self.config.buffer_pool_initial_size = buffer_pool_initial_size;
        self
    }
    pub fn compaction_policy(&self) -> CompactionPolicy {
        self.config.compaction_policy
    }
    pub fn set_compaction_policy(mut self, compaction_policy: CompactionPolicy) -> Self {
        self.config.compaction_policy = compaction_policy;
        self
    }
    fn is_closed(&self) -> bool {
        self.name == Self::NO_OPEN_DB_NAME
    }
    ///Gets number of SSTs in level, NOTE: a value of 0 can mean that there is no level
    fn sst_count(&self, level: Level) -> Size {
        match self.metadata.entry_counts.get(level) {
            None => 0,
            Some(level_entry_counts) => level_entry_counts.len(),
        }
    }
    //GETTERS AND SETTERS (end)

    fn write_config_file(&self) {
        let mut file = match fs::File::create(filename::config(&self.name)) {
            Ok(f) => f,
            Err(why) => {
                panic!(
                    "Unable to write to config file for {}, reason {}",
                    self.name(),
                    why
                );
            }
        };
        bincode::serialize_into(&mut file, &self.config).expect("Unable to serialize config file");
    }
    fn write_metadata_file(&self) {
        let mut file = match fs::File::create(filename::metadata(&self.name)) {
            Ok(f) => f,
            Err(why) => {
                panic!(
                    "Unable to write to metadata file for {}, reason {}",
                    self.name(),
                    why
                );
            }
        };
        bincode::serialize_into(&mut file, &self.metadata)
            .expect("Unable to serialize metadata file");
    }
    ///Writes config and metadata files
    fn write_db_state(&self) {
        if self.is_closed() {
            //NOTE: this should not happen unless we write after closing
            panic!("Attempted to write database state with no database opened")
        }
        self.write_config_file();
        self.write_metadata_file();
    }
    pub fn open(name: &str) -> Database {
        if name == Self::NO_OPEN_DB_NAME {
            panic!("Cannot open a database with the empty string as its name!")
        }
        match fs::read_dir(name) {
            Ok(_) => {
                //directory exists, assume that this is a valid db
                //read config and metadata files
                let config_file = match fs::File::open(filename::config(name)) {
                    Ok(f) => f,
                    Err(why) => {
                        panic!("Unable to read config file for {}, reason: {}", name, why);
                    }
                };
                let metadata_file = match fs::File::open(filename::metadata(name)) {
                    Ok(f) => f,
                    Err(why) => {
                        panic!("Unable to read metadata file for {}, reason: {}", name, why);
                    }
                };

                let config: Config = bincode::deserialize_from(config_file)
                    .expect("Failed to deserialize database config");
                let metadata: Metadata = bincode::deserialize_from(metadata_file)
                    .expect("Failed to deserialize database config");

                Database::new(name, config, metadata)
            }
            Err(_) => {
                //directory doesn't exist
                fs::create_dir(name)
                    .unwrap_or_else(|_| panic!("Unable to create directory for {}", name));

                //Step 1: make db
                let db = Database::new(name, Config::new(), Metadata::new());

                //Step 2: Create config file with default settings
                db.write_db_state();
                db
            }
        }
    }
    pub fn clear(&mut self) {
        self.name = String::from(Self::NO_OPEN_DB_NAME);
        self.config = Config::new();
        self.memtable.clear();
    }
    ///compacts depending on number of ssts at level and compaction policy
    fn handle_compaction(&mut self, level: Level) {
        let _num_runs = self.sst_count(level);
        let _max_runs = self.config.sst_size_ratio.pow(level as u32);
        let has_flushed_level = false;
        let num_entries_after_compaction = 0; //Number of entries after compaction step
        match self.config.compaction_policy {
            CompactionPolicy::None => {
                return;
            }
            CompactionPolicy::Basic => {
                return;
            }
            _ => {} //TODO: handle compaction for policies we plan to implement
        }
        //if we flushed, we need to potentially handle compaction for the next level
        if has_flushed_level && num_entries_after_compaction > 0 {
            if self.metadata.entry_counts.get(level + 1).is_none() {
                //new level created, with a single sst
                self.metadata.entry_counts[level].push(num_entries_after_compaction);
            }
            self.handle_compaction(level + 1)
        }
    }
    fn sst_interface(&self) -> Box<dyn SortedStringTable> {
        match self.config.sst_implementation {
            SstImplementation::Array => Box::new(array_sst::Sst {}),
            SstImplementation::Btree => Box::new(btree_sst::Sst {}),
        }
    }
    ///Writes memtable contents to disk, clears memtable, and handles compaction if needed
    fn flush_memtable(&mut self) {
        if self.memtable.len() < 1 {
            //shouldn't flush if there's nothing to flush
            return;
        }

        let level = Self::LEVEL_ZERO;
        let next_run_num = self.sst_count(level);

        //Write memtable to memory
        let sst = self.sst_interface();
        let entries = self.memtable.as_vec();
        if let Err(why) = sst.write(&self.name, level, next_run_num, &entries) {
            panic!("Failed to flush memtable to SST, reason: {why}");
        };
        self.metadata.entry_counts[level].push(entries.len());

        self.handle_compaction(level);

        self.memtable.clear();
    }
    pub fn close(&mut self) {
        if self.is_closed() {
            return;
        }
        self.flush_memtable();
        self.write_db_state();
        self.clear();
    }
    fn put_unchecked(&mut self, key: Key, value: Value) {
        if self.memtable.len() < self.memtable_capacity() {
            self.memtable.put(key, value);
            return;
        }

        self.flush_memtable();

        if self.memtable.len() >= self.memtable_capacity() {
            //This should only happen if capacity is zero, which should never happen
            panic!("Memtable at (or over) capacity after flush");
        }

        self.memtable.put(key, value);
    }
    pub fn put(&mut self, key: Key, value: Value) {
        if value == Database::TOMBSTONE_VALUE {
            panic!("Attempted to insert tombstone value");
        }
        if value == Self::INVALID_KEY {
            panic!("Attempted to insert invalid key");
        }
        self.put_unchecked(key, value);
    }
    pub fn delete(&mut self, key: Key) {
        self.put_unchecked(key, Self::TOMBSTONE_VALUE);
    }
    ///For each sst, from youngest to oldest, run a callback function (the callback returns true if we want to return early)
    fn for_each_sst(entry_counts: &[Vec<Size>], callback: &mut dyn FnMut(Level, Run) -> bool) {
        for (level, level_entry_counts) in entry_counts.iter().enumerate() {
            //lower level is younger
            let runs_in_level = level_entry_counts.len();
            for run in (0..runs_in_level).rev() {
                //higher number sst is younger
                if callback(level, run) {
                    return;
                }
            }
        }
    }
    pub fn get(&mut self, key: Key) -> Option<Value> {
        //check memtable first
        if let Some(value) = self.memtable.get(key) {
            if value == Self::TOMBSTONE_VALUE {
                return None;
            }
            return Some(value);
        }
        let sst = self.sst_interface();
        //search ssts within levels from youngest to oldest, return youngest value found
        let mut sst_search_result: Option<Value> = None;
        let mut buffer_pool = if self.config.enable_buffer_pool {
            Some(&mut self.buffer_pool)
        } else {
            None
        };
        let entry_counts = &self.metadata.entry_counts;
        let mut callback = |level, run| {
            match sst.get(&self.name, level, run, key, entry_counts[level][run], buffer_pool.take()) {
                Err(why) => panic!("Something went wrong trying to get key {key} at level {level}, sst {run}, reason: {why}"),
                Ok(get_attempt_result) => {
                    if get_attempt_result.is_none() {
                        return false;
                    }
                    sst_search_result = get_attempt_result; //found value
                    true//exit from "for each" loop
                }
            }
        };
        Self::for_each_sst(&self.metadata.entry_counts, &mut callback);
        if sst_search_result.is_some_and(|value| value == Self::TOMBSTONE_VALUE) {
            return None;
        }
        sst_search_result
    }
    pub fn scan(&mut self, key1: Key, key2: Key) -> Vec<(Key, Value)> {
        //NOTE: might be able to improve this by doing a "for each in range" on each SST instead, might not be worth it though
        let sst = self.sst_interface();
        let results = self.memtable.scan(key1, key2);
        let mut unique_key_set: HashSet<Key> =
            results.iter().map(|(key, _)| key.to_owned()).collect();
        //NOTE: the reason we can use negative keys in our max_heap is because negative Key::MIN is not allowed to be inserted, otherwise that would cause an overflow
        let mut max_heap: BinaryHeap<(Key, Value)> = results
            .iter()
            .map(|(key, value)| (-key, value.to_owned()))
            .collect();

        //for every sst (youngest to oldest)
        //scan and add values that have not been seen to the hashmap
        let mut buffer_pool = if self.config.enable_buffer_pool {
            Some(&mut self.buffer_pool)
        } else {
            None
        };
        let entry_counts = &self.metadata.entry_counts;
        let mut callback = |level, run| {
            match sst.scan(&self.name, level, run, (key1, key2), entry_counts[level][run], buffer_pool.take()) {
                Err(why) => panic!("Something went wrong trying to scan range ({key1} to {key2}) at level {level}, sst {run}, reason: {why}"),
                Ok(scan_result) => {
                    for (key, value) in scan_result {
                        //NOTE: because we only allow unique keys to be pushed to the min_heap, it will only ever compare the first item in the tuple (the key) when ordering
                        if !unique_key_set.contains(&key) {
                            unique_key_set.insert(key);
                            max_heap.push((-key, value));
                        }
                    }
                }
            }
            false
        };
        Self::for_each_sst(&self.metadata.entry_counts, &mut callback);
        let mut sorted_values = Vec::with_capacity(max_heap.len());
        while let Some((negative_key, value)) = max_heap.pop() {
            if value != Self::TOMBSTONE_VALUE {
                sorted_values.push((-negative_key, value))
            }
        }

        sorted_values
    }
}

///Runs on destruction, closes DB automatically
impl Drop for Database {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use crate::util::btree_info::fanout;

    use super::*;

    fn setup_and_test_and_cleaup(
        test_name: &str,
        database_alterations: &mut dyn FnMut(Database) -> Database,
        test: &mut dyn FnMut(Database) -> Database,
    ) {
        let test_dir = test_name;
        let db_name = format!("{test_dir}/test");
        if std::path::Path::new(test_dir).exists() {
            std::fs::remove_dir_all(test_dir).unwrap(); //remove previous directory if panicked during tests and didn't clean up
        }
        std::fs::create_dir_all(test_dir).unwrap();

        let mut db = test(database_alterations(Database::open(&db_name)));

        db.close();

        std::fs::remove_dir_all(test_dir).unwrap();
    }

    fn small_db_test(test_name: &str, database_alterations: &mut dyn FnMut(Database) -> Database) {
        let mut alterations = |db: Database| {
            database_alterations(db).set_memtable_capacity(2) //memtable capacity needed for tests below
        };
        let mut test = |mut db: Database| {
            //Test puts, and check if younger values are used instead of older values in gets
            db.put(0, 1); //This value should be immediately replaced
            assert_eq!(db.get(0), Some(1));
            db.put(0, 2);
            assert_eq!(db.get(0), Some(2)); //This value should be replaced after flushing
            db.put(10, 100);
            db.put(20, 200);
            db.put(0, 3); //original value of 0, should have been flushed
            assert_eq!(db.get(0), Some(3)); //case: memtable "blocking" sst search
            db.put(30, 300);
            db.put(40, 400);
            assert_eq!(db.get(0), Some(3)); //case: sst blocking older sst search
            db.put(0, 10);

            assert_eq!(db.sst_count(0), 3); // Should have flushed 3 times (since capacity is 2), NOTE: this will be wrong if the previous call to this function panicked

            //Test gets
            assert_eq!(db.get(0), Some(10));
            assert_eq!(db.get(20), Some(200));
            assert_eq!(db.get(10), Some(100));
            assert_eq!(db.get(40), Some(400));
            assert_eq!(db.get(30), Some(300));
            assert_eq!(db.get(-1), None);
            assert_eq!(db.get(1), None);
            assert_eq!(db.get(300), None);
            assert_eq!(db.get(400), None);

            //test scans
            assert_eq!(db.scan(1, 19), vec![(10, 100)]);
            assert_eq!(db.scan(0, 19), vec![(0, 10), (10, 100)]);
            assert_eq!(db.scan(1, 20), vec![(10, 100), (20, 200)]);
            assert_eq!(db.scan(0, 20), vec![(0, 10), (10, 100), (20, 200)]);
            assert_eq!(
                db.scan(-1, 9999),
                vec![(0, 10), (10, 100), (20, 200), (30, 300), (40, 400)]
            );

            //Test deletes
            db.delete(30);
            db.delete(20);
            assert_eq!(db.get(30), None);
            assert_eq!(db.get(20), None);
            assert_eq!(db.get(0), Some(10));
            assert_eq!(db.get(10), Some(100));
            assert_eq!(db.get(40), Some(400));

            //Test how deletes affect scan
            assert_eq!(db.scan(1, 19), vec![(10, 100)]);
            assert_eq!(db.scan(0, 19), vec![(0, 10), (10, 100)]);
            assert_eq!(db.scan(1, 20), vec![(10, 100)]);
            assert_eq!(db.scan(0, 20), vec![(0, 10), (10, 100)]);
            assert_eq!(db.scan(-1, 9999), vec![(0, 10), (10, 100), (40, 400)]);

            db
        };
        setup_and_test_and_cleaup(test_name, &mut alterations, &mut test)
    }

    fn large_db_test(test_name: &str, database_alterations: &mut dyn FnMut(Database) -> Database) {
        let memtable_cap = 896; //3.5 pages (4096 bytes * 3.5/16)

        let mut alterations = |db: Database| {
            database_alterations(db).set_memtable_capacity(memtable_cap) //memtable capacity needed for tests below
        };
        let mut test = |mut db: Database| {
            let (min, max) = (-1000, 1000);
            let range = min..max + 1; //3.5 pages of
            let mut entries = Vec::<(Key, Value)>::new();

            //Test puts
            for i in range.into_iter() {
                let entry = (i, i * 10);
                db.put(entry.0, entry.1);
                entries.push(entry);
            }

            assert_eq!(db.sst_count(0), entries.len() / memtable_cap); //NOTE: this will be wrong if the previous call to this function panicked

            //Test gets
            for (key, value) in entries.iter() {
                assert_eq!(db.get(*key), Some(*value), "key: {key}");
            }

            //Test getting values that are not in the db
            assert_eq!(db.get(min - 1000), None);
            assert_eq!(db.get(max + 1000), None);

            //Test scans
            //scan from [..i+1]
            for i in 0..entries.len() {
                let slice = entries[..i + 1].to_vec();
                let scan = db.scan(entries.first().unwrap().0, entries[i].0);
                assert_eq!(
                    scan.len(),
                    slice.len(),
                    "scan range ({}, {}), scan results {:?}",
                    entries.first().unwrap().0,
                    entries[i].0,
                    scan
                );
                assert_eq!(scan, slice);
            }

            //scan from [i..]
            for i in 0..entries.len() {
                let slice = entries[i..].to_vec();
                let scan = db.scan(entries[i].0, entries.last().unwrap().0);
                assert_eq!(scan.len(), slice.len());
                assert_eq!(scan, slice);
            }

            //scan from [i..j+1]
            for i in (0..entries.len()).step_by(57) {
                for j in (i..entries.len() - 1).step_by(37) {
                    let slice = entries[i..j + 1].to_vec();
                    let scan = db.scan(entries[i].0, entries[j].0);
                    assert_eq!(scan.len(), slice.len());
                    assert_eq!(scan, slice);
                }
            }

            //Test scans that include keys outside of min and max of db
            assert_eq!(db.scan(min - 1000, max + 1000), entries);

            //Test overwriting existing keys
            let step = 3;
            for (key, value) in entries.iter().step_by(step) {
                db.put(*key, value * 2);
            }
            for (i, (key, value)) in entries.iter().enumerate() {
                if i % step == 0 {
                    assert_eq!(db.get(*key), Some(value * 2));
                } else {
                    assert_eq!(db.get(*key), Some(*value));
                }
            }

            //Test deletes
            let delete_step = 3; //NOTE: if this value is different from <step>, you'll need a new vec to hold onto the new db values that the prev test changed
            for (key, _) in entries.iter().step_by(delete_step) {
                db.delete(*key);
                assert_eq!(db.get(*key), None);
            }

            //Test how deletes affect scan
            let filtered: Vec<(Key, Value)> = entries
                .iter()
                .enumerate()
                .filter(|&(i, _)| i % delete_step != 0)
                .map(|(_, v)| v.to_owned())
                .collect(); //entries without every <delete_step>th element
            let slice = &filtered;
            let scan = db.scan(entries.first().unwrap().0, entries.last().unwrap().0);
            assert_eq!(scan.len(), slice.len());
            assert_eq!(scan, slice.to_vec());

            db
        };

        setup_and_test_and_cleaup(test_name, &mut alterations, &mut test)
    }

    fn multi_page_btree_single_sst_db_test(
        test_name: &str,
        database_alterations: &mut dyn FnMut(Database) -> Database,
    ) {
        let memtable_cap = fanout().pow(2) + 1; //Btrees will have 3 inner nodes and root

        let mut alterations = |db: Database| {
            database_alterations(db).set_memtable_capacity(memtable_cap) //memtable capacity needed for tests below
        };
        let mut test = |mut db: Database| {
            let (min, max) = (0, memtable_cap as i64);
            let range = min..max + 1; //1 extra to force a flush to sst
            let mut entries = Vec::<(Key, Value)>::new();

            //Test puts
            for i in range.into_iter() {
                let entry = (i, i * 10);
                db.put(entry.0, entry.1);
                entries.push(entry);
            }

            assert_eq!(db.sst_count(0), 1); //confirm that we have a single sst

            //Test getting values that are not in the db
            assert_eq!(db.get(min - 1000), None);
            assert_eq!(db.get(max + 1000), None);

            //Test gets
            for (key, value) in entries.iter() {
                assert_eq!(db.get(*key), Some(*value), "key: {key}");
            }

            //check that scan of db returns all entries
            let scan_range = (entries.first().unwrap().0, entries.last().unwrap().0);
            let scan = db.scan(scan_range.0, scan_range.1);
            assert_eq!(
                scan.len(),
                entries.len(),
                "scan range ({}, {})",
                scan_range.0,
                scan_range.1
            );
            assert_eq!(scan, entries);

            //Test scans that include keys outside of min and max of db
            assert_eq!(db.scan(min - 1000, max + 1000), entries);

            //Test overwriting existing keys
            let step = 3;
            for (key, value) in entries.iter().step_by(step) {
                db.put(*key, value * 2);
            }
            for (i, (key, value)) in entries.iter().enumerate() {
                if i % step == 0 {
                    assert_eq!(db.get(*key), Some(value * 2));
                } else {
                    assert_eq!(db.get(*key), Some(*value));
                }
            }

            //Test deletes
            let delete_step = step; //NOTE: if this value is different from <step>, you'll need a new vec to hold onto the new db values that the prev test changed
            for (key, _) in entries.iter().step_by(delete_step) {
                db.delete(*key);
                assert_eq!(db.get(*key), None);
            }

            //Test how deletes affect scan
            let filtered: Vec<(Key, Value)> = entries
                .iter()
                .enumerate()
                .filter(|&(i, _)| i % delete_step != 0)
                .map(|(_, v)| v.to_owned())
                .collect(); //entries without every <delete_step>th element
            let slice = &filtered;
            let scan = db.scan(entries.first().unwrap().0, entries.last().unwrap().0);
            assert_eq!(scan.len(), slice.len());
            assert_eq!(scan, slice.to_vec());

            db
        };

        setup_and_test_and_cleaup(test_name, &mut alterations, &mut test)
    }

    fn part1_db_alterations(db: Database) -> Database {
        db.set_compaction_policy(CompactionPolicy::None)
            .set_enable_buffer_pool(false)
            .set_sst_size_ratio(2)
            .set_sst_implementation(SstImplementation::Array)
            .set_buffer_pool_capacity(0)
            .set_buffer_pool_initial_size(0)
    }

    fn part2_db_alterations(db: Database) -> Database {
        db.set_compaction_policy(CompactionPolicy::None)
            .set_enable_buffer_pool(false)
            .set_sst_size_ratio(2)
            .set_sst_implementation(SstImplementation::Btree)
            .set_buffer_pool_capacity(64)
            .set_buffer_pool_initial_size(16)
    }

    #[test]
    fn part_1_test_small() {
        small_db_test("part1_small_db_test", &mut part1_db_alterations);
    }

    #[test]
    fn part1_test_large() {
        large_db_test("part1_large_db_test", &mut part1_db_alterations);
    }

    #[test]
    #[ignore = "Test case redundant with large test"]
    fn part1_test_multi_page_btree_single_sst() {
        multi_page_btree_single_sst_db_test(
            "part1_multi_page_btree_single_sst_db_test",
            &mut part1_db_alterations,
        );
    }

    #[test]
    fn part_2_test_small() {
        small_db_test("part2_small_db_test", &mut part2_db_alterations);
    }

    #[test]
    fn part2_test_large() {
        large_db_test("part2_large_db_test", &mut part2_db_alterations);
    }

    #[test]
    fn part2_test_multi_page_btree_single_sst() {
        multi_page_btree_single_sst_db_test(
            "part2_multi_page_btree_single_sst_db_test",
            &mut part2_db_alterations,
        );
    }
}
