type NodePtr<K, V> = Option<Box<AvlNode<K, V>>>;

#[derive(Debug, Default, PartialEq)]
pub struct AvlNode<K, V> {
    key: K,
    value: V,
    height: usize,
    left: NodePtr<K, V>,
    right: NodePtr<K, V>,
}

impl<K: Clone + std::cmp::PartialOrd + std::fmt::Display, V: Clone> AvlNode<K, V> {
    const DEFAULT_HEIGHT: usize = 0;
    const NONE_HEIGHT: i32 = -1; //Height of children that are None
    ///Creates a new instance of Node
    pub fn new(key: K, value: V) -> AvlNode<K, V> {
        AvlNode {
            key,
            value,
            height: 0,
            right: None,
            left: None,
        }
    }
    pub fn key(&self) -> K {
        self.key.clone()
    }
    pub fn value(&self) -> V {
        self.value.clone()
    }
    pub fn height(&self) -> usize {
        self.height
    }
    fn balance(&self) -> i32 {
        let [mut l_height, mut r_height] = [Self::NONE_HEIGHT; 2];
        if let Some(left) = &self.left {
            l_height = left.height() as i32;
        }
        if let Some(right) = &self.right {
            r_height = right.height() as i32;
        }

        r_height - l_height
    }
    ///readjusts height values of node based on heights of its children
    fn recalc_height(&mut self) {
        let [mut l_height, mut r_height] = [Self::DEFAULT_HEIGHT; 2];
        if let Some(left) = &self.left {
            l_height = left.height();
        }
        if let Some(right) = &self.right {
            r_height = right.height();
        }

        if self.left.is_none() && self.right.is_none() {
            self.height = Self::DEFAULT_HEIGHT;
        } else {
            self.height = 1 + std::cmp::max(l_height, r_height);
        }
    }

    fn rotate_left(&mut self) {
        let mut subtree = self
            .right
            .take()
            .unwrap_or_else(|| panic!("Nothing to rotate left on"));
        self.right = subtree.left.take();
        self.recalc_height();

        std::mem::swap(self, subtree.as_mut());

        self.left = Some(subtree);
        self.recalc_height();
    }
    fn rotate_right(&mut self) {
        let mut subtree = self
            .left
            .take()
            .unwrap_or_else(|| panic!("Nothing to rotate right on"));
        self.left = subtree.right.take();
        self.recalc_height();

        std::mem::swap(self, subtree.as_mut());

        self.right = Some(subtree);
        self.recalc_height();
    }
    fn rotate_left_right(&mut self) {
        self.left
            .as_mut()
            .unwrap_or_else(|| panic!("Nothing to rotate left-right on"))
            .rotate_left();
        self.rotate_right();
    }
    fn rotate_right_left(&mut self) {
        self.right
            .as_mut()
            .unwrap_or_else(|| panic!("Nothing to rotate right-left on"))
            .rotate_right();
        self.rotate_left();
    }

    fn rebalance(&mut self) {
        //considers all cases and rebalances subtree accordingly
        self.recalc_height();
        let balance = self.balance();

        if balance < -1 {
            //left heavy
            if self.left.as_ref().is_some_and(|left| left.balance() <= 0) {
                self.rotate_right();
            } else {
                self.rotate_left_right();
            }
        } else if balance > 1 {
            //right heavy
            if self
                .right
                .as_ref()
                .is_some_and(|right| right.balance() >= 0)
            {
                self.rotate_left();
            } else {
                self.rotate_right_left();
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct AvlTree<K, V> {
    root: NodePtr<K, V>,
    len: usize,
}

impl<K: Clone + std::cmp::PartialOrd + std::fmt::Display, V: Clone> AvlTree<K, V> {
    ///Creates a new AVL tree instance
    pub fn new() -> AvlTree<K, V> {
        AvlTree { root: None, len: 0 }
    }
    ///Returns number of elements in tree
    pub fn len(&self) -> usize {
        self.len
    }
    ///Insert key-value pair into Avl Subtree using recursion, returns true iff size size increases
    fn insert_recursive(subtree: &mut Box<AvlNode<K, V>>, key: K, value: V) -> bool {
        if subtree.key() == key {
            subtree.value = value;
            return false;
        }

        let (insertion_subtree, other_subtree) = if key < subtree.key() {
            (&mut subtree.left, &subtree.right)
        } else {
            (&mut subtree.right, &subtree.left)
        };

        match *insertion_subtree {
            None => {
                if other_subtree.is_none() {
                    //in any other case, height will not increase
                    subtree.height += 1;
                }
                *insertion_subtree = Some(Box::new(AvlNode::<K, V>::new(key, value)));
                true
            }
            Some(ref mut node) => {
                let has_added_node = Self::insert_recursive(node, key, value);
                subtree.rebalance();
                has_added_node
            }
        }
    }
    ///Inserts key-value pair into Avl Subtree
    pub fn insert(&mut self, key: K, value: V) {
        if let Some(ref mut root) = self.root {
            let increased_size = Self::insert_recursive(root, key, value);
            if increased_size {
                self.len += 1;
            }
        } else {
            self.root = Some(Box::new(AvlNode::new(key, value)));
            self.len += 1;
        }
    }
    ///Recursively searches through tree for node with key
    fn search_node_recursive(subtree: &AvlNode<K, V>, key: K) -> Option<&AvlNode<K, V>> {
        if subtree.key() == key {
            return Some(subtree);
        }

        let next_node = if key < subtree.key() {
            &subtree.left
        } else {
            &subtree.right
        };

        if let Some(node) = next_node {
            Self::search_node_recursive(node, key)
        } else {
            None
        }
    }
    ///Searches tree for key, returns value if exists
    pub fn search(&self, key: K) -> Option<V> {
        self.root.as_ref().and_then(|root| {
            Self::search_node_recursive(root, key).map(|result_node| result_node.value())
        })
    }
    ///Removes the node with smallest key in subtree, if exists
    fn take_min_node(subtree: &mut NodePtr<K, V>) -> NodePtr<K, V> {
        if let Some(mut node) = subtree.take() {
            //Recurse along the left side
            if let Some(smaller_subtree) = Self::take_min_node(&mut node.left) {
                //Took the smallest from below; update this node and put it back in the tree
                node.rebalance();
                *subtree = Some(node);
                Some(smaller_subtree)
            } else {
                //Take this node and replace it with its right child
                *subtree = node.right.take();
                Some(node)
            }
        } else {
            None
        }
    }
    ///Deletes node with matching key recursively, returns (replacement node, removed node).
    /// * Replacement node - new head of subtree
    fn delete_node_recursive(
        subtree: &mut NodePtr<K, V>,
        key: K,
    ) -> (NodePtr<K, V>, NodePtr<K, V>) {
        if let Some(mut root) = subtree.take() {
            if key < root.key() {
                let (replacement, removed) = Self::delete_node_recursive(&mut root.left, key);
                root.left = replacement;

                if removed.is_some() {
                    root.rebalance();
                }

                (Some(root), removed)
            } else if key > root.key() {
                let (replacement, removed) = Self::delete_node_recursive(&mut root.right, key);
                root.right = replacement;
                if removed.is_some() {
                    root.rebalance();
                }

                (Some(root), removed)
            } else {
                //need to remove root
                if root.left.is_none() && root.right.is_none() {
                    //case 1: no children
                    (None, Some(root))
                } else if root.left.is_some() && root.right.is_some() {
                    //case 2: 2 children
                    let mut replacement_node = Self::take_min_node(&mut root.right).unwrap(); //should be able to safely unwrap here
                                                                                              //replacement node guaranteed to not have left children, otherwise that child would be the replacement
                    replacement_node.left = root.left.take();
                    replacement_node.rebalance();
                    (Some(replacement_node), Some(root))
                } else {
                    //case 3: 1 child
                    let mut replacement_node = if root.left.is_some() {
                        root.left.take().unwrap()
                    } else {
                        root.right.take().unwrap()
                    };

                    replacement_node.rebalance();
                    (Some(replacement_node), Some(root))
                }
            }
        } else {
            (None, None)
        }
    }
    ///Deletes node from tree, returns deleted node
    fn delete_node(&mut self, key: K) -> NodePtr<K, V> {
        let (new_root, removed_node) = Self::delete_node_recursive(&mut self.root, key);
        if removed_node.is_some() {
            self.len -= 1;
        }
        self.root = new_root;
        removed_node
    }
    ///Deletes node from tree, returns deleted value
    pub fn delete(&mut self, key: K) -> Option<V> {
        self.delete_node(key).map(|node| node.value())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_balanced_tree() -> AvlTree<&'static str, u32> {
        AvlTree::<&str, u32> {
            len: 3,
            root: Some(Box::new(AvlNode::<&str, u32> {
                key: "b",
                value: 2,
                height: 1,
                left: Some(Box::new(AvlNode {
                    key: "a",
                    value: 1,
                    height: 0,
                    left: None,
                    right: None,
                })),
                right: Some(Box::new(AvlNode {
                    key: "c",
                    value: 3,
                    height: 0,
                    left: None,
                    right: None,
                })),
            })),
        }
    }
    fn get_big_balanced_tree() -> AvlTree<&'static str, u32> {
        AvlTree::<&str, u32> {
            len: 6,
            root: Some(Box::new(AvlNode::<&str, u32> {
                key: "d",
                value: 4,
                height: 2,
                left: Some(Box::new(AvlNode {
                    key: "b",
                    value: 2,
                    height: 1,
                    left: Some(Box::new(AvlNode {
                        key: "a",
                        value: 1,
                        height: 0,
                        left: None,
                        right: None,
                    })),
                    right: Some(Box::new(AvlNode {
                        key: "c",
                        value: 3,
                        height: 0,
                        left: None,
                        right: None,
                    })),
                })),
                right: Some(Box::new(AvlNode {
                    key: "e",
                    value: 5,
                    height: 1,
                    left: None,
                    right: Some(Box::new(AvlNode {
                        key: "f",
                        value: 6,
                        height: 0,
                        left: None,
                        right: None,
                    })),
                })),
            })),
        }
    }

    #[test]
    fn test_insert_size() {
        let mut tree = AvlTree::<&str, u32>::new();
        tree.insert("a", 1);
        tree.insert("c", 3);
        tree.insert("b", 2);

        assert_eq!(tree.len(), 3);
    }

    #[test]
    fn test_rotation_left() {
        let mut tree = AvlTree::<&str, u32>::new();
        //forced a left rotation with insertion order
        tree.insert("a", 1);
        tree.insert("b", 2);
        tree.insert("c", 3);

        assert_eq!(tree, get_balanced_tree());
    }

    #[test]
    fn test_rotation_right() {
        let mut tree = AvlTree::<&str, u32>::new();
        //forced a right rotation with insertion order
        tree.insert("c", 3);
        tree.insert("b", 2);
        tree.insert("a", 1);

        assert_eq!(tree, get_balanced_tree());
    }

    #[test]
    fn test_rotation_left_right() {
        let mut tree = AvlTree::<&str, u32>::new();
        //forced a left-right rotation with insertion order
        tree.insert("c", 3);
        tree.insert("a", 1);
        tree.insert("b", 2);

        assert_eq!(tree, get_balanced_tree());
    }

    #[test]
    fn test_rotation_right_left() {
        let mut tree = AvlTree::<&str, u32>::new();
        //forced a right-left rotation with insertion order
        tree.insert("c", 3);
        tree.insert("a", 1);
        tree.insert("b", 2);

        assert_eq!(tree, get_balanced_tree());
    }

    #[test]
    fn test_rotation_big_tree() {
        let mut tree = AvlTree::<&str, u32>::new();
        //forced a right-left rotation with insertion order
        tree.insert("c", 3);
        tree.insert("a", 1);
        tree.insert("b", 2);

        //tree should be balanced here
        tree.insert("d", 4);
        tree.insert("e", 5);
        tree.insert("f", 6);
        assert_eq!(tree, get_big_balanced_tree());
    }

    #[test]
    fn test_search_at_root() {
        assert_eq!(2, get_balanced_tree().search("b").unwrap());
    }

    #[test]
    fn test_search_deep() {
        assert_eq!(6, get_big_balanced_tree().search("f").unwrap());
    }

    #[test]
    fn test_take_min() {
        let mut tree = get_big_balanced_tree();
        let min = AvlTree::take_min_node(&mut tree.root).unwrap();
        assert_eq!(min.key, "a");
    }

    #[test]
    #[ignore = "not implemented yet"]
    fn test_take_min_rearrangement() {
        todo!(); //TODO: check if swap with min and right child work correctly
    }

    #[test]
    fn test_delete() {
        let removed_tree = AvlTree::<&str, u32> {
            len: 5,
            root: Some(Box::new(AvlNode::<&str, u32> {
                key: "d",
                value: 4,
                height: 2,
                left: Some(Box::new(AvlNode {
                    key: "b",
                    value: 2,
                    height: 1,
                    left: None,
                    right: Some(Box::new(AvlNode {
                        key: "c",
                        value: 3,
                        height: 0,
                        left: None,
                        right: None,
                    })),
                })),
                right: Some(Box::new(AvlNode {
                    key: "e",
                    value: 5,
                    height: 1,
                    left: None,
                    right: Some(Box::new(AvlNode {
                        key: "f",
                        value: 6,
                        height: 0,
                        left: None,
                        right: None,
                    })),
                })),
            })),
        };

        let mut tree = get_big_balanced_tree();
        let removed = tree.delete("a");
        assert_eq!(tree, removed_tree);
        assert_eq!(removed, Some(1));
    }

    #[test]
    fn test_delete_root() {
        let removed_tree = AvlTree::<&str, u32> {
            len: 2,
            root: Some(Box::new(AvlNode::<&str, u32> {
                key: "c",
                value: 3,
                height: 1,
                left: Some(Box::new(AvlNode {
                    key: "a",
                    value: 1,
                    height: 0,
                    left: None,
                    right: None,
                })),
                right: None,
            })),
        };

        let mut tree = get_balanced_tree();
        let removed = tree.delete("b");
        assert_eq!(tree, removed_tree);
        assert_eq!(removed, Some(2))
    }

    #[test]
    fn test_delete_non_key() {
        let mut tree = get_big_balanced_tree();
        let removed = tree.delete("x");
        assert_eq!(tree, get_big_balanced_tree());
        assert_eq!(removed, None)
    }
}
