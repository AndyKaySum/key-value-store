
use crate::avl::{AvlTree, AvlNode};
use crate::memtable::{Memtable};
// mod merge_k_lists; // Import the module
use crate::merge_k_lists::merge_k_lists::merge_k_sorted_lists;
// use merge_k_lists::merge_k_sorted_lists; // Import the function
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
            self.records += 1; 
            println!("records: {}", self.records);
            if self.records == self.mem_size{
               self.records = 0; 
               self.num_sst += 1;  
               let new_sstable_path = format!("memtable_{}.sst", self.num_sst);
               let new_sstable = SSTable::new(&new_sstable_path);
               let pairs = self.memtable.flush();
               new_sstable.fill(pairs, self.num_sst.try_into().unwrap());
               self.ssts.push(new_sstable);
               let new_memtable = Memtable::new(self.mem_size, self.num_sst);
               //    std::mem::replace(&mut self.memtable, new_memtable); //follow up on performance implication of making this a mutuable reference
               self.memtable = new_memtable;
            

            }
        }
       
        pub fn get(&mut self, key:i64) -> Option<i64> {
            self.memtable.get(key)
        }

        pub fn scan(&mut self, key1:i64, key2: i64)-> Vec<(i64, i64)>{ 
            let mut my_vec: Vec<Vec<(i64, i64)>> = Vec::new();
            my_vec.push(self.memtable.scan(key1, key2)); 
            println!("sst count: {}", self.num_sst);
            for i in 0..self.num_sst{
                println!("iteration: {}", i);
                my_vec.push(self.ssts[i].binary_search_range_scan(key1, key2).expect("error!"))
            }
            merge_k_sorted_lists(my_vec)
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
    #[test]
    fn test_scan_sst() { //TODO: Issue with this test!
        let mut db: Database = Database::new("test_db".to_string(), 0, 10);
        db.put(1, 11);
        db.put(3, 33);
        db.put(4, 11);
        db.put(5, 33);
        db.put(6, 11);
        db.put(7, 33);
        let result = db.scan(1,10);
        assert_eq!(result, vec![(1, 11), (3, 33), (4, 11),(5, 33),(6, 11),(7, 33)]);
    }

}