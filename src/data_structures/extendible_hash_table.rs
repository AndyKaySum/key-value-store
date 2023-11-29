#![allow(dead_code)]
use rand::Rng;
use std::arch::x86_64::_MM_FROUND_NO_EXC;
// rand crate is required
use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::f32::consts::E;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

use rand::seq::index;

struct Bucket<K: Debug, V: Debug> {
    bucket_id: usize,
    local_depth: usize,
    elements: Vec<(K, V)>,
    capacity: usize,
    size: usize,
}

struct ExtendibleHashTable<K: Debug, V: Debug, H = DefaultHasher> {
    directory: Vec<Rc<RefCell<Bucket<K, V>>>>,
    buckets: Vec<Rc<RefCell<Bucket<K, V>>>>,
    global_depth: usize,
    max_size: usize,
    current_size: usize,
    hasher: H,
    num_buckets: usize,
    overflow: Vec<(K, V)>,
}

impl<K: Hash + Eq + Debug + Clone, V: Debug + Clone> Bucket<K, V> {
    fn new(capacity: usize, local_depth: usize, bucket_id: usize) -> Self {
        Bucket {
            local_depth,
            capacity,
            size: 0,
            bucket_id,
            elements: Vec::with_capacity(capacity),
        }
    }
    fn add_element(&mut self, element: (K, V)) -> bool {
        let elements = self.get_elements();
        for (index, existing_element) in elements.iter().enumerate() {
            if existing_element.0 == element.0 {
                self.elements[index] = (element.0, element.1);
                return true;
            }
        }
        if !self.is_full() {
            self.elements.push((element.0, element.1));
            self.size += 1;
            true
        } else {
            println!(
                "Failed to add element: {:?} bucket {} curr bucket {:?}",
                element, self.bucket_id, self.elements
            );
            false
        }
    }
    fn remove_element(&mut self, index: usize) -> Option<(K, V)> {
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
    fn get_bucket_id(&self) -> usize {
        self.bucket_id
    }
    fn clear(&mut self) {
        self.size = 0;
        // self.elements.clear()
    }
    fn get_high_bit(&self) -> u64 {
        1 << self.local_depth
    }
}
impl<K: Debug, V: Debug> Default for Bucket<K, V> {
    fn default() -> Self {
        // Provide the logic to create a default instance of Bucket<K, V>
        // This could be an empty bucket or a bucket with some default state
        Bucket {
            bucket_id: 0,
            local_depth: 0,
            elements: Vec::new(),
            capacity: 0,
            size: 0,
        }
    }
}

impl<K: Hash + Eq + Debug + Clone, V: Debug + Clone, H: Hasher + Default + Debug>
    ExtendibleHashTable<K, V, H>
{
    fn new(max_size: usize, bucket_size: usize) -> Self {
        // The number of unique buckets is half the size of the directory
        // Will always start with two buckets and directory size of 4
        let num_buckets = 2;
        let mut buckets = Vec::with_capacity(num_buckets);

        // Create the required number of unique buckets
        for i in 0..num_buckets {
            buckets.push(Rc::new(RefCell::new(Bucket::new(bucket_size, 1, i + 1))));
        }

        // Create the directory and assign buckets to each index
        let mut directory = Vec::with_capacity(4);
        for i in 0..4 {
            if i % 2 == 0 {
                directory.push(Rc::clone(&buckets[0]));
            } else {
                directory.push(Rc::clone(&buckets[1]));
            }
        }

        ExtendibleHashTable {
            directory,
            global_depth: 2,
            max_size,
            current_size: 0,
            hasher: H::default(),
            num_buckets: 2,
            buckets,
            overflow: Vec::new(),
        }
    }
    fn get(&self, key: K) -> Option<V> {
        let bucket = self.get_bucket(self.hash_key(&key) as usize).unwrap();
        let bucket = bucket.borrow_mut();
        let elements = bucket.get_elements();
        for element in elements {
            if element.0 == key {
                return Some(element.1.clone());
            }
        }
        None
    }
    fn get_next(&self, key: K) -> Option<V> {
        let mut index = self.hash_key(&key) as usize;
        let bucket = self.get_bucket(index).unwrap();
        let bucket = bucket.borrow_mut();
        let elements = bucket.get_elements();
        for (i, element) in elements.iter().enumerate() {
            if element.0 == key {
                if i == elements.len() - 1 {
                    // if we are at the end of the bucket

                    if (index + 1) >= self.directory.len() - 1 {
                        // if we are at the end of the directory
                        return None;
                    }
                    // Get the next non-empty bucket

                    let mut next_bucket = &self.directory[index + 1];
                    let mut next_elements_len = next_bucket.borrow().get_elements().len();
                    while next_elements_len == 0 {
                        next_bucket = &self.directory[index + 1];
                        next_elements_len = next_bucket.borrow_mut().get_elements().len();
                        index += 1;
                    }
                    return Some(next_bucket.borrow_mut().get_elements()[0].1.clone());
                }
                return Some(elements[i + 1].1.clone());
            }
        }
        None
    }
    fn add_to_directory(&mut self, bucket: Rc<RefCell<Bucket<K, V>>>, index: usize) {
        //add bucket to directory
        self.directory[index] = bucket;
    }
    fn rehash_bucket(&mut self, bucket: Rc<RefCell<Bucket<K, V>>>) {
        // Clone the elements to avoid borrowing issues
        let elements = bucket.borrow().get_elements().clone();
        for element in elements {
            self.put(element.0, element.1);
        }
    }
    fn put(&mut self, key: K, value: V) -> bool {
        let mut added = false; // flag to indicate if the element was added
        let index = self.hash_key(&key) as usize;
        let (local_depth, mut is_full) = {
            let bucket = self.get_bucket(index).unwrap().borrow();
            (bucket.get_local_depth(), bucket.is_full())
        };
        if !is_full {
            // if the bucket is not full, add the element.
            self.get_bucket_mut(index)
                .unwrap()
                .borrow_mut()
                .add_element((key.clone(), value.clone()));
            added = true;
        }
        is_full = self.get_bucket(index).unwrap().borrow().is_full(); // check if the bucket is full again
        if is_full {
            let global_depth = self.get_global_depth();
            if local_depth == global_depth {
                // need to double since can't split this bucket further
                self.global_depth += 1;
                let new_directory_size = 2usize.pow(self.global_depth as u32);
                let mut new_directory: Vec<Rc<RefCell<Bucket<K, V>>>> =
                    Vec::with_capacity(new_directory_size);
                for index in 0..new_directory_size {
                    new_directory.push(
                        self.directory
                            [truncate_binary(index as u64, self.global_depth - 1) as usize]
                            .clone(),
                    );
                }
                self.directory = new_directory;
            }
            let bucket1 = Rc::new(RefCell::new(Bucket::new(
                self.get_bucket(index).unwrap().borrow().capacity,
                local_depth + 1,
                self.num_buckets + 1,
            )));
            let bucket2 = Rc::new(RefCell::new(Bucket::new(
                self.get_bucket(index).unwrap().borrow().capacity,
                local_depth + 1,
                self.num_buckets + 2,
            )));
            let high_bit = self.get_bucket(index).unwrap().borrow().get_high_bit();
            let elements = std::mem::take(
                &mut self
                    .get_bucket_mut(index as usize)
                    .unwrap()
                    .borrow_mut()
                    .elements,
            );

            for element in elements {
                let index = self.hash_key(&element.0);

                if index & high_bit == 0 {
                    bucket1.borrow_mut().add_element(element.clone());
                } else {
                    bucket2.borrow_mut().add_element(element.clone());
                }
            }

            for i in ((index as u64 & (high_bit - 1))..self.directory.len() as u64)
                .step_by(high_bit as usize)
            {
                if i & high_bit == 0 {
                    //this clone only increases the ref count, doesn't clone the bucket
                    self.add_to_directory(bucket1.clone(), i as usize);
                } else {
                    self.add_to_directory(bucket2.clone(), i as usize);
                }
            }
        }
        return added;
    }
    fn delete(&mut self, key: K) -> Option<(K, V)> {
        let mut bucket = self
            .get_bucket_mut(self.hash_key(&key) as usize)
            .unwrap()
            .borrow_mut();
        let elements = bucket.get_elements();
        for (index, element) in elements.iter().enumerate() {
            if element.0 == key {
                return bucket.remove_element(index);
            }
        }
        None
    }
    fn hash_key(&self, key: &K) -> u64 {
        let mut hasher: H = H::default();
        key.hash(&mut hasher);
        let hash_value = hasher.finish() as u64;
        hash_value & ((1 << self.global_depth) - 1)
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
    fn get_directory(&self) -> &Vec<Rc<RefCell<Bucket<K, V>>>> {
        &self.directory
    }
    fn get_directory_mut(&mut self) -> &mut Vec<Rc<RefCell<Bucket<K, V>>>> {
        &mut self.directory
    }
    fn get_bucket(&self, index: usize) -> Option<&Rc<RefCell<Bucket<K, V>>>> {
        self.directory.get(index)
    }
    fn get_bucket_mut(&mut self, index: usize) -> Option<&mut Rc<RefCell<Bucket<K, V>>>> {
        self.directory.get_mut(index)
    }
}

fn truncate_binary(num: u64, length: usize) -> u64 {
    if length >= 64 {
        return num;
    }
    if num == 0 {
        return 0;
    }
    // Create a bitmask with the last `length` bits set to 1
    let bitmask: u64 = (1 << length) - 1;
    // Apply the bitmask to `num` to get the last `length` bits
    num & bitmask
}

#[cfg(test)]
mod extendible_hash_table_tests {

    use super::*;
    #[test]
    fn main_test_small_bucket() {
        let bucket_size = 2;
        let mut hash_table = ExtendibleHashTable::<i32, i32, DefaultHasher>::new(100, bucket_size);

        assert_eq!(hash_table.get_global_depth(), 2);
        assert_eq!(hash_table.get_max_size(), 100);
        let iters = 1000;
        for i in 0..iters {
            let mut result = hash_table.put(i, i);
            while !result {
                println!("Failed to add element: {} Trying agin", i);
                result = hash_table.put(i, i);
            }
        }
        println!("get 700 {}", hash_table.get(700).unwrap());
        for i in 0..iters {
            println!(
                "Getting element: {} from bucket {}",
                i,
                hash_table.hash_key(&i)
            );
            println!("Element: {:?}", hash_table.get(i).unwrap());
        }
        println!("Global depth: {}", hash_table.get_global_depth());
    }
    fn test_put_and_get() {
        let bucket_size = 10;
        let mut hash_table = ExtendibleHashTable::<i32, i32, DefaultHasher>::new(100, bucket_size);

        // Insert key-value pairs into the hash table
        hash_table.put(1, 10);
        hash_table.put(2, 20);
        hash_table.put(3, 30);

        // Retrieve values from the hash table using keys
        assert_eq!(hash_table.get(1), Some(10));

        assert_eq!(hash_table.get(2), Some(20));
        assert_eq!(hash_table.get(3), Some(30));
        assert_eq!(hash_table.get(4), None); // Non-existent key

        // Insert more key-value pairs
        hash_table.put(4, 40);
        hash_table.put(5, 50);

        // Retrieve values again
        assert_eq!(hash_table.get(4), Some(40));
        assert_eq!(hash_table.get(5), Some(50));
    }

    #[test]
    fn test_delete() {
        let bucket_size = 10;
        let mut hash_table = ExtendibleHashTable::<i32, i32, DefaultHasher>::new(100, bucket_size);

        // Insert key-value pairs into the hash table
        hash_table.put(1, 10);
        hash_table.put(2, 20);
        hash_table.put(3, 30);

        // Delete key-value pairs
        assert_eq!(hash_table.delete(2), Some((2, 20)));
        assert_eq!(hash_table.delete(4), None); // Non-existent key

        // Check if the deleted key-value pair is removed
        assert_eq!(hash_table.get(2), None);
    }

    // Add more tests for other methods and edge cases

    #[test]
    fn test_truncate_binary() {
        assert_eq!(truncate_binary(0b101010, 4), 0b1010);
        assert_eq!(truncate_binary(0b111111, 8), 0b111111);
        assert_eq!(truncate_binary(0b11001100, 6), 0b001100);
    }
    #[test]
    fn test_bucket_overflow_and_resizing() {
        let mut hash_table = ExtendibleHashTable::<i32, i32, DefaultHasher>::new(5, 10); // small initial size to trigger resizing
        for i in 0..10 {
            hash_table.put(i, i * 100);
        }

        for i in 0..10 {
            assert_eq!(hash_table.get(i), Some((i * 100)));
        }
        assert!(hash_table.get_global_depth() > 1); // ensure that global depth increased due to resizing
    }
    #[test]

    fn test_edge_cases() {
        let mut hash_table = ExtendibleHashTable::<i32, i32, DefaultHasher>::new(100, 10);
        // Inserting a large number of elements
        for i in 0..1000 {
            hash_table.put(i, i);
        }
        // Checking a high-index element
        assert_eq!(hash_table.get(999), Some(999));

        // Checking after deleting a high-index element
        hash_table.delete(999);
        assert_eq!(hash_table.get(999), None);
    }
    #[derive(Hash, Eq, PartialEq, Debug, Clone)]
    struct CustomKey {
        id: i32,
        name: String,
    }

    #[test]
    fn test_with_custom_structs() {
        let mut hash_table = ExtendibleHashTable::<CustomKey, i32, DefaultHasher>::new(100, 10);
        let key1 = CustomKey {
            id: 1,
            name: "Key1".to_string(),
        };
        let key2 = CustomKey {
            id: 2,
            name: "Key2".to_string(),
        };

        hash_table.put(key1, 100);
        hash_table.put(key2, 200);

        assert_eq!(
            hash_table.get(CustomKey {
                id: 1,
                name: "Key1".to_string()
            }),
            Some(100)
        );
    }
}
