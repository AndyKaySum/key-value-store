use crate::{
    data_structures::avl::AvlTree,
    util::types::{Key, Size, Value},
};

#[derive(Debug)]
pub struct Memtable {
    tree: AvlTree<Key, Value>,
}

impl Memtable {
    ///Initializes an empty Memtable with a given capacity
    pub fn new() -> Self {
        Memtable {
            tree: AvlTree::new(),
        }
    }
    pub fn len(&self) -> Size {
        self.tree.len()
    }
    ///Insert value into memtable, returns None if fails to insert (when it's full)
    pub fn put(&mut self, key: Key, value: Value) {
        self.tree.insert(key, value);
    }
    pub fn get(&self, key: Key) -> Option<Value> {
        self.tree.search(key)
    }
    pub fn clear(&mut self) {
        self.tree = AvlTree::new()
    }
    // Performs inorder traversal of the tree and returns a vector of all the key-value pairs
    // with key between key1 and key2
    pub fn scan(&self, key1: Key, key2: Key) -> Vec<(Key, Value)> {
        let mut result = Vec::new();
        self.tree
            .for_each_in_range(&key1, &key2, &mut |key, value| result.push((*key, *value)));
        result
    }
    pub fn as_vec(&self) -> Vec<(Key, Value)> {
        let mut result = Vec::new();
        self.tree
            .for_each(&mut |key, value| result.push((*key, *value)));
        result
    }
}

// Sanity tests, need to check for edge cases
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_empty() {
        let memtable: Memtable = Memtable::new();
        let result = memtable.scan(1, 3);
        assert_eq!(result, vec![]);
    }

    #[test]
    fn test_scan_single() {
        let mut memtable: Memtable = Memtable::new();
        memtable.put(1, 1);
        let result = memtable.scan(1, 1);
        assert_eq!(result, vec![(1, 1)]);
    }

    #[test]
    fn test_scan_multiple() {
        let mut memtable: Memtable = Memtable::new();
        memtable.put(1, 11);
        memtable.put(3, 33);
        let result = memtable.scan(1, 3);
        assert_eq!(result, vec![(1, 11), (3, 33)]);
    }

    #[test]
    fn test_scan_order() {
        let mut memtable: Memtable = Memtable::new();
        memtable.put(1, 1);
        memtable.put(2, 3);
        memtable.put(3, 5);
        let result = memtable.scan(1, 3);
        assert_eq!(result, vec![(1, 1), (2, 3), (3, 5)]);
    }

    #[test]
    fn test_scan_invalid_range() {
        let mut memtable: Memtable = Memtable::new();
        memtable.put(1, 1);
        memtable.put(2, 3);
        memtable.put(3, 5);
        assert_eq!(memtable.get(1), Some(1));

        let result = memtable.scan(4, 11);
        assert_eq!(result, vec![]);
    }

    #[test]
    fn test_sst_read() {
        // Create a new memtable with capacity 2
        let mut memtable = Memtable::new();

        // Insert three key-value pairs
        memtable.put(1, 11);
        memtable.put(2, 22);
        memtable.put(3, 33);
        memtable.put(4, 44);
        memtable.put(5, 55);
        memtable.put(6, 66);
        memtable.put(7, 77);
        memtable.put(8, 88);
        memtable.put(9, 99);

        assert_eq!(memtable.get(1), Some(11));
        assert_eq!(memtable.get(9), Some(99));
    }
}
