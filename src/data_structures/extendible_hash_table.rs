#![allow(dead_code)]
use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

#[derive(Debug)]
struct Bucket<K: Debug, V: Debug> {
    bucket_id: usize,
    local_depth: usize,
    elements: VecDeque<(K, V)>,
    capacity: usize,
    access_bit: bool,
}

#[derive(Debug)]
pub struct ExtendibleHashTable<K: Debug, V: Debug, H = DefaultHasher> {
    directory: Vec<Rc<RefCell<Bucket<K, V>>>>,
    buckets: Vec<Rc<RefCell<Bucket<K, V>>>>,
    global_depth: usize,
    current_size: usize,
    hasher: H,
    num_buckets: usize,
}

impl<K: Hash + Eq + Debug, V: Debug> Bucket<K, V> {
    fn new(capacity: usize, local_depth: usize, bucket_id: usize) -> Self {
        Bucket {
            local_depth,
            capacity,
            bucket_id,
            elements: VecDeque::with_capacity(capacity),
            access_bit: true,
        }
    }
    fn add_element(&mut self, element: (K, V)) -> bool {
        self.set_accessed(true);
        let mut found = false;
        let mut found_index = 0;

        for (index, existing_element) in self.elements.iter().enumerate() {
            if existing_element.0 == element.0 {
                found = true;
                found_index = index;
                break;
            }
        }

        if found {
            // Remove the element at the found index and push it to the back
            if self.elements.remove(found_index).is_some() {
                self.elements.push_back((element.0, element.1));
            }
            return false;
        }

        if !self.is_full() {
            self.elements.push_back((element.0, element.1));
            true
        } else {
            false
        }
    }
    fn add_element_ignore(&mut self, element: (K, V)) -> bool {
        self.set_accessed(true);
        let mut found = false;
        let mut found_index = 0;

        for (index, existing_element) in self.elements.iter().enumerate() {
            if existing_element.0 == element.0 {
                found = true;
                found_index = index;
                break;
            }
        }

        if found {
            // Remove the element at the found index and push it to the back
            if self.elements.remove(found_index).is_some() {
                self.elements.push_back((element.0, element.1));
            }
            return false;
        }
        self.elements.push_back((element.0, element.1));
        true
    }
    fn remove_element(&mut self, index: usize) -> Option<(K, V)> {
        if index < self.elements.len() {
            self.elements.remove(index)
        } else {
            None
        }
    }
    fn get_item(&mut self, key: K) -> Option<&(K, V)> {
        self.set_accessed(true);
        let elements = self.get_elements();
        for (index, element) in elements.iter().enumerate() {
            if element.0 == key && index < self.elements.len() {
                // let output = self.elements[index];
                let removed = match self.remove_element(index) {
                    Some(e) => e,
                    None => return None,
                };

                self.add_element_ignore(removed);
                let element = self.elements.get(self.get_size() - 1);
                match element {
                    Some(element) => return Some(element),
                    None => return None,
                }
            }
        }

        None
    }
    fn get_local_depth(&self) -> usize {
        self.local_depth
    }
    fn get_elements(&self) -> &VecDeque<(K, V)> {
        &self.elements
    }
    fn is_full(&self) -> bool {
        self.get_size() >= self.capacity
    }
    fn get_size(&self) -> usize {
        self.elements.len()
    }
    fn get_bucket_id(&self) -> usize {
        self.bucket_id
    }
    fn get_high_bit(&self) -> u64 {
        1 << self.local_depth
    }
    fn pop_front(&mut self) -> Option<(K, V)> {
        self.elements.pop_front()
    }
    fn set_accessed(&mut self, accessed: bool) {
        self.access_bit = accessed;
    }
    fn get_accessed(&self) -> bool {
        self.access_bit
    }
}

impl<K: Debug, V: Debug> Default for Bucket<K, V> {
    fn default() -> Self {
        // Provide the logic to create a default instance of Bucket<K, V>
        // This could be an empty bucket or a bucket with some default state
        Bucket {
            bucket_id: usize::MAX,
            local_depth: 0,
            elements: VecDeque::new(),
            capacity: 0,
            access_bit: true,
        }
    }
}

impl<K: Hash + Eq + Debug + Clone, V: Debug + Clone, H: Hasher + Default + Debug>
    ExtendibleHashTable<K, V, H>
{
    pub fn with_capacity_buckets(
        bucket_capacity: usize,
        num_buckets: usize,
        dir_size: usize,
    ) -> Self {
        let mut buckets = Vec::with_capacity(num_buckets);
        for i in 0..num_buckets {
            buckets.push(Rc::new(RefCell::new(Bucket::new(
                bucket_capacity,
                1,
                i + 1,
            ))));
        }
        let directory: Vec<Rc<RefCell<Bucket<K, V>>>> = (0..dir_size)
            .map(|i| Rc::clone(&buckets[i % num_buckets]))
            .collect();

        ExtendibleHashTable {
            directory,
            global_depth: (dir_size).ilog2() as usize,
            current_size: 0,
            hasher: H::default(),
            num_buckets,
            buckets,
        }
    }
    fn new(bucket_size: usize) -> Self {
        // The number of unique buckets is half the size of the directory
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
            current_size: 0,
            hasher: H::default(),
            num_buckets: 2,
            buckets,
        }
    }

    pub fn len(&self) -> usize {
        self.current_size
    }
    pub fn num_buckets(&self) -> usize {
        self.buckets.len()
    }
    pub fn get(&self, key: &K) -> Option<V> {
        let bucket_index = self.hash_key(key) as usize;
        let bucket = match self.get_bucket(bucket_index) {
            Some(bucket) => bucket,
            None => {
                return None;
            }
        };
        let bucket_ref = bucket.borrow();

        let elements = bucket_ref.get_elements();

        let mut index_to_remove = None;
        for (index, element) in elements.iter().enumerate() {
            if element.0 == *key {
                index_to_remove = Some(index);
                break;
            }
        }

        // Drop the immutable borrow here
        drop(bucket_ref);

        if let Some(index) = index_to_remove {
            let mut bucket = bucket.borrow_mut();
            let element = match bucket.remove_element(index) {
                Some(e) => e,
                None => return None,
            };
            bucket.add_element_ignore(element);
            let elements = bucket.get_elements();
            let last_element = match elements.iter().last() {
                Some(e) => e.1.clone(),
                None => return None,
            };
            return Some(last_element);
        }
        None
    }
    fn add_to_directory(&mut self, bucket: Rc<RefCell<Bucket<K, V>>>, index: usize) {
        //add bucket to directory
        self.directory[index] = bucket;
    }

    pub fn put(&mut self, key: K, value: V) -> bool {
        let index = self.hash_key(&key) as usize;
        #[allow(unused_assignments)]
        let mut added = true; //NOTE: vscode thinks this is unused, but that's false

        {
            // Limited scope for mutable borrow
            let mut bucket = match self.get_bucket_mut(index) {
                Some(bucket) => bucket.borrow_mut(),
                None => {
                    return false;
                }
            };
            added = bucket.add_element_ignore((key, value));
        } // Mutable borrow ends here

        if added {
            self.current_size += 1;
        }

        let (local_depth, is_full) = {
            let bucket = match self.get_bucket(index) {
                Some(bucket) => bucket.borrow(),
                None => {
                    return false;
                }
            };
            (bucket.get_local_depth(), bucket.is_full())
        };

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

            let bucket = match self.get_bucket(index) {
                Some(bucket) => bucket.borrow(),
                None => {
                    return false;
                }
            };
            let bucket1 = Rc::new(RefCell::new(Bucket::new(
                bucket.capacity,
                local_depth + 1,
                self.num_buckets + 1,
            )));
            let bucket2 = Rc::new(RefCell::new(Bucket::new(
                bucket.capacity,
                local_depth + 1,
                self.num_buckets + 2,
            )));
            drop(bucket);
            self.buckets.push(Rc::clone(&bucket1));
            self.buckets.push(Rc::clone(&bucket2));
            let mut bucket = match self.get_bucket_mut(index) {
                Some(bucket) => bucket.borrow_mut(),
                None => {
                    return false;
                }
            };

            let high_bit = bucket.get_high_bit();
            let elements = std::mem::take(&mut bucket.elements);
            drop(bucket);

            for element in elements {
                let index = self.hash_key(&element.0);
                if index & high_bit == 0 {
                    bucket1.borrow_mut().add_element_ignore(element);
                } else {
                    bucket2.borrow_mut().add_element_ignore(element);
                }
            }

            for i in ((index as u64 & (high_bit - 1))..self.directory.len() as u64)
                .step_by(high_bit as usize)
            {
                if i & high_bit == 0 {
                    self.add_to_directory(bucket1.clone(), i as usize);
                } else {
                    self.add_to_directory(bucket2.clone(), i as usize);
                }
            }
        }
        added
    }

    pub fn remove(&mut self, key: &K) -> Option<(K, V)> {
        let mut index = None;
        {
            let bucket = match self.get_bucket(self.hash_key(key) as usize) {
                Some(bucket) => bucket.borrow(),
                None => {
                    return None;
                }
            };
            let elements = bucket.get_elements();
            for (i, element) in elements.iter().enumerate() {
                if element.0 == *key {
                    index = Some(i);
                    break;
                }
            }
        }
        if let Some(index) = index {
            self.current_size -= 1;
            let mut bucket = match self.get_bucket_mut(self.hash_key(key) as usize) {
                Some(bucket) => bucket.borrow_mut(),
                None => {
                    return None;
                }
            };
            return bucket.remove_element(index);
        }
        None
    }
    fn bucket_pop_front(&mut self, bucket_index: usize) -> Option<(K, V)> {
        let mut bucket = self.buckets[bucket_index].borrow_mut();
        let popped = bucket.pop_front();
        match popped {
            Some(element) => {
                self.current_size -= 1;
                Some(element)
            }
            None => None,
        }
    }
    ///Removes the least recently used element in the the bucket at index <bucket_index>.
    /// Returns key if successfully removed an element
    pub fn bucket_remove_lru(&mut self, bucket_index: usize) -> Option<K> {
        self.bucket_pop_front(bucket_index).map(|(key, ..)| key) //NOTE: elements are moved to the back on access, so front is least recently accessed
    }
    fn hash_key(&self, key: &K) -> u64 {
        let mut hasher: H = H::default();
        key.hash(&mut hasher);
        let hash_value = hasher.finish();
        hash_value & ((1 << self.global_depth) - 1)
    }
    fn get_global_depth(&self) -> usize {
        self.global_depth
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
    pub fn accessed(&self, index: usize) -> bool {
        let bucket = self.buckets[index].borrow();
        bucket.get_accessed()
    }
    pub fn set_accessed(&mut self, index: usize, accessed: bool) {
        let mut bucket = self.buckets[index].borrow_mut();
        bucket.set_accessed(accessed)
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
    fn test_test() {
        let mut hash_table = ExtendibleHashTable::<i32, i32, DefaultHasher>::new(10);

        assert_eq!(hash_table.get_global_depth(), 2);
        let iters = 1000;
        for i in 0..iters {
            hash_table.put(i, i);
        }

        println!("Global depth: {}", hash_table.get_global_depth());
        for i in 0..iters {
            println!(
                "Getting element: {} from bucket {}",
                i,
                hash_table.hash_key(&i)
            );
            println!("Element: {:?}", hash_table.get(&i).unwrap());
        }
        for bucket in hash_table.get_directory() {
            let borrowed_bucket = bucket.borrow_mut();
            println!(
                "Bucket: {:?} ({}) is full? {} size {} capacity {}",
                borrowed_bucket.get_elements(),
                borrowed_bucket.get_bucket_id(),
                borrowed_bucket.is_full(),
                borrowed_bucket.get_size(),
                borrowed_bucket.capacity
            );
        }
        println!("Global depth: {}", hash_table.get_global_depth());
    }
    fn test_put_and_get() {
        let mut hash_table = ExtendibleHashTable::<i32, i32, DefaultHasher>::new(10);

        // Insert key-value pairs into the hash table
        hash_table.put(1, 10);
        hash_table.put(2, 20);
        hash_table.put(3, 30);

        // Retrieve values from the hash table using keys
        assert_eq!(hash_table.get(&1), Some(10));

        assert_eq!(hash_table.get(&2), Some(20));
        assert_eq!(hash_table.get(&3), Some(30));
        assert_eq!(hash_table.get(&4), None); // Non-existent key

        // Insert more key-value pairs
        hash_table.put(4, 40);
        hash_table.put(5, 50);

        // Retrieve values again
        assert_eq!(hash_table.get(&4), Some(40));
        assert_eq!(hash_table.get(&5), Some(50));
    }

    #[test]
    fn test_delete() {
        let mut hash_table = ExtendibleHashTable::<i32, i32, DefaultHasher>::new(10);

        // Insert key-value pairs into the hash table
        hash_table.put(1, 10);
        hash_table.put(2, 20);
        hash_table.put(3, 30);

        // Delete key-value pairs
        assert_eq!(hash_table.remove(&2), Some((2, 20)));
        assert_eq!(hash_table.remove(&4), None); // Non-existent key

        // Check if the deleted key-value pair is removed
        assert_eq!(hash_table.get(&2), None);
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
        let mut hash_table = ExtendibleHashTable::<i32, i32, DefaultHasher>::new(10); // small initial size to trigger resizing
        for i in 0..10 {
            hash_table.put(i, i * 100);
        }

        for i in 0..10 {
            assert_eq!(hash_table.get(&i), Some(i * 100));
        }
        assert!(hash_table.get_global_depth() > 1); // ensure that global depth increased due to resizing
    }
    #[test]

    fn test_edge_cases() {
        let mut hash_table = ExtendibleHashTable::<i32, i32, DefaultHasher>::new(10);
        // Inserting a large number of elements
        for i in 0..1000 {
            hash_table.put(i, i);
        }
        // Checking a high-index element
        assert_eq!(hash_table.get(&999), Some(999));

        // Checking after deleting a high-index element
        hash_table.remove(&999);
        assert_eq!(hash_table.get(&999), None);
    }
    #[derive(Hash, Eq, PartialEq, Debug, Clone)]
    struct CustomKey {
        id: i32,
        name: String,
    }

    #[test]
    fn test_with_custom_structs() {
        let mut hash_table = ExtendibleHashTable::<CustomKey, i32, DefaultHasher>::new(10);
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
            hash_table.get(&CustomKey {
                id: 1,
                name: "Key1".to_string()
            }),
            Some(100)
        );
    }

    #[test]

    fn test_capacity_with_bucket() {
        let dir_size = 32;
        let num_buckets = 7;
        let bucket_size = 10;
        let mut hash_table = ExtendibleHashTable::<i32, i32, DefaultHasher>::with_capacity_buckets(
            bucket_size,
            num_buckets,
            dir_size,
        );
        assert_eq!(hash_table.get_global_depth(), 5);
        for i in 0..10 {
            hash_table.put(i, i * 100);
        }
        let mut max_index = 0;
        for bucket in hash_table.get_directory() {
            let borrowed_bucket = bucket.borrow_mut();
            let bucket_index = borrowed_bucket.get_bucket_id();
            println!(
                "Bucket: {:?} ({}) is full? {} size {} capacity {}",
                borrowed_bucket.get_elements(),
                bucket_index,
                borrowed_bucket.is_full(),
                borrowed_bucket.get_size(),
                borrowed_bucket.capacity
            );
            if bucket_index > max_index {
                max_index = bucket_index;
            }
        }
        for i in 0..10 {
            assert_eq!(hash_table.get(&i), Some(i * 100));
        }
        assert_eq!(max_index, num_buckets);
        assert_eq!(hash_table.get_directory().len(), dir_size);
    }
    #[test]
    fn test_add_reorders() {
        let mut hash_table = ExtendibleHashTable::<i32, i32, DefaultHasher>::new(10);
        for i in 0..10 {
            hash_table.put(i, i * 100);
        }
        for bucket in hash_table.get_directory() {
            let borrowed_bucket = bucket.borrow_mut();
            println!(
                "Bucket: {:?} ({}) is full? {} size {} capacity {}",
                borrowed_bucket.get_elements(),
                borrowed_bucket.get_bucket_id(),
                borrowed_bucket.is_full(),
                borrowed_bucket.get_size(),
                borrowed_bucket.capacity
            );
        }
        hash_table.get(&2);
        println!("--------");
        for bucket in hash_table.get_directory() {
            let borrowed_bucket = bucket.borrow_mut();
            println!(
                "Bucket: {:?} ({}) is full? {} size {} capacity {}",
                borrowed_bucket.get_elements(),
                borrowed_bucket.get_bucket_id(),
                borrowed_bucket.is_full(),
                borrowed_bucket.get_size(),
                borrowed_bucket.capacity
            );
        }
    }
    #[test]
    fn test_update_move_to_end() {
        let mut hash_table = ExtendibleHashTable::<i32, i32, DefaultHasher>::new(10);
        for i in 0..10 {
            hash_table.put(i, i * 100);
        }
        for bucket in hash_table.get_directory() {
            let borrowed_bucket = bucket.borrow_mut();
            println!(
                "Bucket: {:?} ({}) is full? {} size {} capacity {}",
                borrowed_bucket.get_elements(),
                borrowed_bucket.get_bucket_id(),
                borrowed_bucket.is_full(),
                borrowed_bucket.get_size(),
                borrowed_bucket.capacity
            );
        }
        hash_table.put(2, 2);
        hash_table.put(11, 11);
        hash_table.put(9, 9);
        println!("--------");
        for bucket in hash_table.get_directory() {
            let borrowed_bucket = bucket.borrow_mut();
            println!(
                "Bucket: {:?} ({}) is full? {} size {} capacity {}",
                borrowed_bucket.get_elements(),
                borrowed_bucket.get_bucket_id(),
                borrowed_bucket.is_full(),
                borrowed_bucket.get_size(),
                borrowed_bucket.capacity
            );
        }
    }
    #[test]
    fn test_pop_bucket() {
        let mut hash_table = ExtendibleHashTable::<i32, i32, DefaultHasher>::new(10);
        for i in 0..10 {
            hash_table.put(i, i * 100);
        }
        for bucket in hash_table.get_directory() {
            let borrowed_bucket = bucket.borrow_mut();
            println!(
                "Bucket: {:?} ({}) is full? {} size {} capacity {}",
                borrowed_bucket.get_elements(),
                borrowed_bucket.get_bucket_id(),
                borrowed_bucket.is_full(),
                borrowed_bucket.get_size(),
                borrowed_bucket.capacity
            );
        }
        hash_table.get(&1);
        let popped = hash_table.bucket_pop_front(0);
        assert_eq!(popped.unwrap(), (0, 0));
        let popped = hash_table.bucket_pop_front(1);
        assert_eq!(popped.unwrap(), (2, 200));
        println!("--------");
        for bucket in hash_table.get_directory() {
            let borrowed_bucket = bucket.borrow_mut();
            println!(
                "Bucket: {:?} ({}) is full? {} size {} capacity {}",
                borrowed_bucket.get_elements(),
                borrowed_bucket.get_bucket_id(),
                borrowed_bucket.is_full(),
                borrowed_bucket.get_size(),
                borrowed_bucket.capacity
            );
        }
    }

    #[test]
    fn test_table_size() {
        let mut hash_table = ExtendibleHashTable::<i32, i32, DefaultHasher>::new(10);
        for i in 0..10 {
            hash_table.put(i, i * 100);
        }
        assert_eq!(hash_table.get_current_size(), 10);
        hash_table.remove(&1);
        assert_eq!(hash_table.get_current_size(), 9);
        hash_table.put(1, 1);
        assert_eq!(hash_table.get_current_size(), 10);
        hash_table.put(2, 5);
        assert_eq!(hash_table.get_current_size(), 10);
    }

    #[test]
    fn test_access_bit() {
        let mut hash_table = ExtendibleHashTable::<i32, i32, DefaultHasher>::new(10);
        for i in 0..10 {
            hash_table.put(i, i * 100);
        }
        hash_table.get(&1);
        assert!(hash_table.accessed(1));
        hash_table.set_accessed(1, false);
        assert!(!hash_table.accessed(1));
    }

    #[test]
    fn test_small_bucket_size() {
        let mut hash_table = ExtendibleHashTable::<i32, i32, DefaultHasher>::new(2);
        let iters = 10000;
        for i in 0..iters {
            hash_table.put(i, i);
        }

        for i in 0..iters {
            assert_eq!(hash_table.get(&i), Some(i));
        }
    }
    #[test]
    fn test_insert_large_values() {
        let mut hash_table = ExtendibleHashTable::<i32, i32, DefaultHasher>::new(10);
        let iters = 10000;

        for i in 0..iters {
            hash_table.put(i, 10000000);
        }

        for i in 0..iters {
            assert_eq!(hash_table.get(&i).unwrap(), 10000000);
        }
    }
}
