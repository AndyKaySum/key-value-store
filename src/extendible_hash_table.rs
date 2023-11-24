#![allow(dead_code)]
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::fmt::Debug;

struct Bucket<K:Debug , V: Debug> {
    local_depth: usize,
    elements: Vec<(K, V)>,
    capacity: usize, 
    size: usize
}

struct ExtendibleHashTable<K: Debug, V: Debug, H = DefaultHasher> {
    directory: Vec<Box<Bucket<K, V>>>,
    global_depth: usize,
    max_size: usize,
    current_size: usize,
    hasher: H,
}


impl<K: Hash + Eq + Debug, V: Debug> Bucket<K, V> {    
    fn new(capacity: usize, local_depth: usize) -> Self {
        Bucket{
            local_depth, 
            capacity,
            size: 0, 
            elements: Vec::with_capacity(capacity)
        }
    }
    fn add_element(&mut self, element: (K,V)) -> bool{
        //if element already exists, update it
        let elements = self.get_elements();
        // println!("Elements: {:?}", elements); 
        for (index, existing_element) in elements.iter().enumerate() {
            // println!("Existing element: {:?} Element: {:?}", existing_element, element);
            if existing_element.0 == element.0 {
                self.elements[index] = element;
                return true
            }
        }
        //if not add it if there is space
        if !self.is_full() {
            self.elements.push(element); 
            self.size += 1;
            true 
        }
        else{
            false
        }
        
    }
    fn remove_element(&mut self, index: usize) -> Option<(K, V)>{
        if index < self.elements.len() {
            self.size -= 1;
            Some(self.elements.swap_remove(index))
        } else {
            None
        }
    }
    fn get_local_depth(&self) -> usize {
        self.local_depth 
    }
    fn get_elements(&self) -> &Vec<(K, V)> {
        &self.elements
    }
    fn is_full(&self) -> bool {
        self.get_size() == self.capacity
    }
    fn get_size(&self) -> usize {
        self.size
    }
}



impl<K: Hash + Eq + Debug, V: Debug, H: Hasher + Default + Debug> ExtendibleHashTable<K,V, H> {

    fn new(global_depth: usize, max_size: usize) -> Self {
        let mut directory = Vec::with_capacity(2usize.pow(global_depth as u32));
        for _ in 0..2usize.pow(global_depth as u32) {
            directory.push(Box::new(Bucket::new(global_depth, 4)));
        }
        ExtendibleHashTable {
            directory, 
            global_depth,
            max_size,
            current_size: 0,
            hasher: H::default()
        }
    }
    fn get(&self, key: K) -> Option<&V> {
        let bucket = self.get_bucket(self.hash_key(&key) as usize).unwrap();
        let elements = bucket.get_elements();
        for element in elements {
            if element.0 == key {
                return Some(&element.1)
            }
        }
        None
    }
    fn put(&mut self, key: K, value: V) -> bool{
        self.get_bucket_mut(self.hash_key(&key) as usize).unwrap().add_element((key, value))

    }
    fn delete(&mut self, key: K) -> Option<(K, V)> {
        let bucket = self.get_bucket_mut(self.hash_key(&key) as usize).unwrap();
        let elements = bucket.get_elements();
        for (index, element) in elements.iter().enumerate() {
            if element.0 == key {
                return bucket.remove_element(index)
            }
        }
        None
    }
    fn hash_key(&self, key: &K) -> u64 {
        let mut hasher: H = H::default();
        key.hash(&mut hasher);
        let hash_value = hasher.finish(); 
        hash_value % self.global_depth as u64
    }
    fn get_global_depth(&self) -> usize {
        self.global_depth
        
    }
    fn get_max_size(&self) -> usize {
        self.max_size
    }
    fn get_current_size(&self) -> usize {
        self.current_size
    }
    fn get_directory(&self) -> &Vec<Box<Bucket<K, V>>> {
        &self.directory
    }
    fn get_directory_mut(&mut self) -> &mut Vec<Box<Bucket<K, V>>>{
        &mut self.directory
    }
    fn get_bucket(&self, index: usize) -> Option<&Box<Bucket<K, V>>> {
        self.directory.get(index)    
    }
    fn get_bucket_mut(&mut self, index: usize) -> Option<&mut Box<Bucket<K, V>>> {
        self.directory.get_mut(index)    
    }

    
}

#[cfg(test)]
mod extendible_hash_table_tests {

    use super::*;
    #[test]
    fn test_test() {
        let mut hash_table = ExtendibleHashTable::<i32, i32, DefaultHasher>::new(3, 100);
        let bucket = hash_table.get_bucket_mut(0).unwrap();
        bucket.add_element((99, 10));
        bucket.add_element((199, 11));
        bucket.add_element((299, 12));
        bucket.remove_element(0);
        bucket.add_element((299, 13));

        assert_eq!(hash_table.get_global_depth(), 3);
        assert_eq!(hash_table.get_max_size(), 100); 
        println!("Hash vaue: {}", hash_table.hash_key(&10));
        hash_table.put(123, 23);
        hash_table.put(124, 24);
        hash_table.put(125, 25);
        hash_table.put(126, 26);
        hash_table.put(127, 27);
        hash_table.put(128, 28);
        hash_table.put(129, 29);
        hash_table.put(130, 30);
        hash_table.put(131, 31);
        hash_table.put(132, 32);

        for bucket in hash_table.get_directory() {
            println!("Bucket: {:?}",bucket.get_elements());
        }
        hash_table.put(131, 69);
        hash_table.delete(124);
        println!("----------------------");
        println!("get: {}", hash_table.get(128).unwrap());
 
        for bucket in hash_table.get_directory() {
            println!("Bucket: {:?}",bucket.get_elements());
        }
        // println!("Bucket: {:?}",bucket.get_elements());
        // let elements = bucket.get_elements();
        // for element in elements {
        //     println!("Element: {:?} Hash: {}",element, hash_table.hash_key(&element.1));
        // }

    }

}