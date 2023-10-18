use crate::avl::{AvlTree, AvlNode};
use crate::memtable::{Memtable};

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use crate::sst::SSTable;
use std::str::FromStr;
#[derive(Debug)]
pub struct Database {
    name: String,
    memtable: Memtable,
    records: usize,
    num_sst: usize, 
    mem_size: usize, 
    records_in_memory: usize, 
    ssts: Vec<SSTable>,  
}

impl Database  {
    pub fn new(name: String, num_sst: usize, mem_size: usize) -> Database {
        Database {
            memtable: Memtable::new(mem_size, 0),
            num_sst,
            records: 0,
            mem_size,
            name: name.clone(),
            ssts: Vec::new(),
            records_in_memory: 0
        }
    }

        pub fn open(){

        }
        pub fn put(&mut self, key:i64, value: i64){
            self.memtable.put(key, value);
        }
       
        pub fn get(&mut self, key:i64) -> Option<i64> {
            self.memtable.get(key)
        }

        pub fn scan(&mut self, key1:i64, key2: i64)-> Vec<(i64, i64)>{
            self.memtable.scan(key1, key2)
        }
        pub fn close(){

        }
    }

    #[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_empty() {
        let mut db: Database = Database::new("test_db".to_string(), 0, 10);
        let result = db.scan(1, 3);
        assert_eq!(result, vec![]);
    }

    #[test]
    fn test_scan_single() {
        let mut db: Database = Database::new("test_db".to_string(), 0, 10);
        db.put(1, 1);
        let result = db.scan(1,2);
        assert_eq!(result, vec![(1, 1)]);
    }

    #[test]
    fn test_scan_multiple() {
        let mut db: Database = Database::new("test_db".to_string(), 0, 10);
        db.put(1, 11);
        db.put(3, 33);
        let result = db.scan(1, 3);
        assert_eq!(result, vec![(1, 11), (3, 33)]);
    }

}