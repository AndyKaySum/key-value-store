use crate::avl::{AvlTree, AvlNode};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use crate::sst::SSTable;
use std::str::FromStr;
#[derive(Debug)]
pub struct Memtable<K, V> {
    tree: AvlTree<K, V>,
    capacity: usize,
    num_sst: usize,
}

impl<
        K: Clone + std::cmp::PartialOrd + std::fmt::Display + std::default::Default + std::str::FromStr,
        V: Clone + std::default::Default + std::fmt::Display + std::str::FromStr,
    > Memtable<K, V>
{
    ///Initializes an empty Memtable with a given capacity
    pub fn new(capacity: usize, num_sst: usize) -> Memtable<K, V> {
        Memtable {
            tree: AvlTree::new(),
            capacity,
            num_sst,
        }
    }
    pub fn parse_value(&self, input: &str) -> Result<V, <V as FromStr>::Err> {
        input.parse::<V>()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
    pub fn len(&self) -> usize {
        self.tree.len()
    }
    pub fn num_sst(&self) -> usize {
        self.num_sst
    }
    pub fn put(&mut self, key: K, value: V) {
        self.tree.insert(key, value);

        // Check if the tree is full
        if self.is_full() {
            // Get a list of all the key-value pairs in the tree
            let pairs = self.scan(self.tree.min_key().unwrap(), self.tree.max_key().unwrap());

            // Write the key-value pairs to a file in SSTable format
            let mut path = PathBuf::new();
            path.push(format!("memtable_{}.sst", self.num_sst));
            let file = File::create(&path).unwrap();
            let mut writer = BufWriter::new(file);
            for (key, value) in pairs {
                writeln!(writer, "{}\t{}", key.to_string(), value.to_string()).unwrap();
            }
            writer.flush().unwrap();

            // Assign a new memtable with the same capacity
            let new_memtable = Memtable::new(self.capacity, self.num_sst + 1);
            std::mem::replace(self, new_memtable);
        }
    }
    pub fn get(&self, key: K) -> Option<V> {
        let key_clone = key.clone();
        match self.tree.search(key_clone) {
            Some(value) => {
                // Do something if you have a value
                return Some(value)
            },
            None => {
                // Do something if you have no value
                for i in 0..self.num_sst + 1 {
                    let sstable1_path = format!("memtable_{}.sst", i);
                    let sstable1 = SSTable::new(&sstable1_path);
                    match sstable1.get(&key.to_string()) {
                        Some(sst_value) => {
                            match self.parse_value(&sst_value) {
                                Ok(parsed_value) => return Some(parsed_value),
                                Err(_) => {
                                    return None;
                                }
                            }
                        },
                        None => {}
                    }
                }
                None // Return None if the loop completes without finding a value
                
            },
        }
    }
    
    pub fn pop(&mut self, key: K) -> Option<V> {
        self.tree.delete(key)
    }
    pub fn is_full(&self) -> bool {
        self.tree.len() >= self.capacity()
    }
    // Performs inorder traversal of the tree and returns a vector of all the key-value pairs 
    // with key between key1 and key2 
    pub fn scan(&self, key1: K, key2: K) -> Vec<(K, V)> {
        let mut result = Vec::new();
        self.inorder_traversal(self.tree.root().as_ref(), &mut result, &key1, &key2);
        result
    }
    
    fn inorder_traversal(&self, node: Option<&Box<AvlNode<K, V>>>, result: &mut Vec<(K, V)>, key1: &K, key2: &K) {
        if let Some(node) = node {
            self.inorder_traversal(node.left().as_ref(), result, key1, key2);
            if node.key() >= *key1 && node.key() <= *key2 {
                result.push((node.key().clone(), node.value()));
            }
            self.inorder_traversal(node.right().as_ref(), result, key1, key2);
        }
    }
}

// Sanity tests, need to check for edge cases
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_empty() {
        let memtable: Memtable<String, String> = Memtable::new(10, 0);
        let result = memtable.scan("a".to_string(), "c".to_string());
        assert_eq!(result, vec![]);
    }

    #[test]
    fn test_scan_single() {
        let mut memtable: Memtable<String, u64> = Memtable::new(10, 0);
        memtable.put("a".to_string(), 1);
        let result = memtable.scan("a".to_string(), "a".to_string());
        assert_eq!(result, vec![("a".to_string(), 1)]);
    }

    #[test]
    fn test_scan_multiple() {
        let mut memtable: Memtable<u64, u64> = Memtable::new(10, 0);
        memtable.put(1, 11);
        memtable.put(3, 33);
        let result = memtable.scan(1, 3);
        assert_eq!(result, vec![(1, 11), (3, 33)]);
    }

    #[test]
    fn test_scan_order() {
        let mut memtable: Memtable<String, u64> = Memtable::new(10, 0);
        memtable.put("a".to_string(), 1);
        memtable.put("b".to_string(), 3);
        memtable.put("c".to_string(), 5);
        let result = memtable.scan("a".to_string(), "c".to_string());
        assert_eq!(result, vec![("a".to_string(), 1), ("b".to_string(), 3), ("c".to_string(), 5)]);
    }

    #[test]
    fn test_scan_invalid_range() {
        let mut memtable: Memtable<String, u64> = Memtable::new(10, 0);
        memtable.put("a".to_string(), 1);
        memtable.put("b".to_string(), 3);
        memtable.put("c".to_string(), 5);
        let result = memtable.scan("d".to_string(), "k".to_string());
        assert_eq!(result, vec![]);
    }

    #[test]
    fn test_memtable_put_over_capacity() {
        // Create a new memtable with a capacity of 2
        let mut memtable: Memtable<String, u64> = Memtable::new(2, 0);

        // Insert three key-value pairs
        memtable.put("a".to_string(), 1);
        memtable.put("b".to_string(), 3);
        memtable.put("c".to_string(), 5);

        // Check that the first two key-value pairs were flushed to disk
        assert_eq!(memtable.get("a".to_string()), Some(1));
        assert_eq!(memtable.get("b".to_string()), Some(3));
        assert_eq!(memtable.get("c".to_string()), Some(5));
    }

    #[test]
    fn test_sst_read() {
        // Create a new memtable with capacity 2
        let mut memtable = Memtable::new(3, 0);

        // Insert three key-value pairs
        memtable.put(1, 11);
        memtable.put(2, 22);
        memtable.put(3,33);
        memtable.put(4,44);
        memtable.put(5,55);
        memtable.put(6, 66);
        memtable.put(7, 77);
        memtable.put(8,88);
        memtable.put(9,99);


        // Check that the memtable num-sst counter in increased
        assert_eq!(memtable.num_sst, 3);

        // Check that the first SSTable contains the first two key-value pairs
      
        assert_eq!(memtable.get(1), Some(11));
        assert_eq!(memtable.get(9), Some(99));
    }
}
