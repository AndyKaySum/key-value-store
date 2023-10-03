use crate::avl_tree::Tree;

#[derive(Debug)]
pub struct Memtable<K, V> {
    tree: Tree<K, V>,
    capacity: usize,
}

impl<
        K: Clone + std::cmp::PartialOrd + std::fmt::Display + std::default::Default,
        V: Clone + std::default::Default,
    > Memtable<K, V>
{
    ///Initializes an empty Memtable with a given capacity
    pub fn new(capacity: usize) -> Memtable<K, V> {
        Memtable {
            tree: Tree::new(),
            capacity,
        }
    }
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    pub fn len(&self) -> usize {
        self.tree.len()
    }
    pub fn put(&mut self, key: K, value: V) {
        self.tree.insert(key, value);
    }
    pub fn get(&self, key: K) -> Option<V> {
        self.tree.search(key)
    }
    pub fn pop(&mut self, key: K) -> Option<V> {
        self.tree.delete(key)
    }
    pub fn scan(&self, key1: K, key2: K) -> Vec<(K, V)> {
        unimplemented!() //TODO: part 1.2
    }
}
