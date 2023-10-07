use crate::avl::AvlTree;

#[derive(Debug)]
pub struct Memtable<K, V> {
    tree: AvlTree<K, V>,
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
            tree: AvlTree::new(),
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
    pub fn is_full(&self) -> bool {
        self.tree.len() >= self.capacity
    }
    // Performs inorder traversal of the tree and returns a vector of all the key-value pairs 
    // with key between key1 and key2 
    pub fn scan(&self, key1: K, key2: K) -> Vec<(K, V)> {
        let mut result = Vec::new();
        let mut stack = Vec::new();
        let mut current = self.tree.root().as_ref().map(|node| &**node);

        while let Some(node) = current {
            if node.key() >= key1 && node.key() <= key2 {
                result.push((node.key().clone(), node.value()));
            }

            if node.left().is_some() && node.key() >= key1 {
                stack.push(node.left().as_ref().unwrap());
            }

            if node.right().is_some() && node.key() <= key2 {
                current = node.right().as_ref().map(|node| &**node);
            } else {
                current = stack.pop().map(|node| &**node);
            }
        }
        result
    }
}
