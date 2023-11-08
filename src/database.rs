use crate::avl::{AvlTree, AvlNode};
use crate::memtable::{Memtable};

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use crate::sst::SSTable;
use std::str::FromStr;
#[derive(Debug)]
pub struct Database<K, V> {
    name: String,
    memtable: Memtable<K,V>,
    records: usize,
    num_sst: usize, 
    mem_size: usize, 
    records_in_memory: usize, 
    ssts: Vec<SSTable>,  
}

impl<
        K: Clone + std::cmp::PartialOrd + std::fmt::Display + std::default::Default + std::str::FromStr,
        V: Clone + std::default::Default + std::fmt::Display + std::str::FromStr,
    > Database<K, V>
    {
        pub fn new(name: String, num_sst: usize, mem_size:usize) -> Database<K, V> {
            Database {
                memtable: Memtable::new(mem_size, 0),
                num_sst,
                records: 0, 
                mem_size, 
                name: name.clone(), 
                ssts: Vec::new(), 
                records_in_memory: 01

            }
        }
        pub fn open(){

        }
        pub fn put(&mut self, key:K, value: V){
            self.memtable.put(key, value);
        }
       
        pub fn get(&mut self, key:K) -> Option<V> {
            self.memtable.get(key)
        }

        pub fn scan(&mut self, key1:K, key2: K)-> Vec<(K, V)>{
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
        let mut db: Database<String, String> = Database::new("test_db".to_string(), 0, 10);
        let result = db.scan("a".to_string(), "c".to_string());
        assert_eq!(result, vec![]);
    }

    #[test]
    fn test_scan_single() {
        let mut db: Database<String, u64> = Database::new("test_db".to_string(), 0, 10);
        db.put("a".to_string(), 1);
        let result = db.scan("a".to_string(), "a".to_string());
        assert_eq!(result, vec![("a".to_string(), 1)]);
    }

    #[test]
    fn test_scan_multiple() {
        let mut db: Database<i64, i64> = Database::new("test_db".to_string(), 0, 10);
        db.put(1, 11);
        db.put(3, 33);
        let result = db.scan(1, 3);
        assert_eq!(result, vec![(1, 11), (3, 33)]);
    }

}