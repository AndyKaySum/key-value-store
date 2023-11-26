use std::io::Seek;

use crate::buffer_pool::BufferPool;
use crate::ceil_div;
use crate::file_io::serde_btree;
use crate::util::algorithm::binary_search_leftmost;
use crate::util::btree_info::{fanout, node_size, ROOT_PAGE_OFFSET};
use crate::util::system_info::num_entries_per_page;
use crate::util::types::{Depth, Level, Node, Run};
use crate::util::types::{Key, Page, Size};

use super::sst_util::get_btree_page;

pub fn has_inner_nodes(num_entries: Size) -> bool {
    num_entries <= num_entries_per_page()
}

pub fn num_leaves(num_entries: Size) -> Size {
    ceil_div!(num_entries, num_entries_per_page())
}

///Depth of B-tree, same as number of inner node levels
pub fn tree_depth(num_entries: Size) -> Size {
    // (num_entries as f64).log(fanout() as f64).ceil() as Size
    (num_leaves(num_entries) as f64).log(fanout() as f64).ceil() as Size
}

pub fn subtree_height(depth: Depth, num_entries: Size) -> Size {
    tree_depth(num_entries) - depth
}

///Number of nodes at a given depth
pub fn num_nodes(depth: Depth, num_entries: Size) -> Size {
    // let b = fanout();
    // let total_levels = num_levels(num_entries);
    // let denominator = b.pow((total_levels - depth) as u32);
    // // (num_entries + denominator - 1) / denominator //ceil division
    // ceil_div!(num_entries, denominator)
    ceil_div!(
        num_leaves(num_entries),
        fanout().pow(subtree_height(depth, num_entries) as u32)
    )
}

///Number of leaves that are skipped when moving a long each node at a given depth
pub fn leaves_in_subtree(depth: Depth, num_entries: Size) -> Size {
    fanout().pow(subtree_height(depth, num_entries) as u32)
}

///get page index of first node at a depth
pub fn depth_page_index(depth: Depth, num_entries: Size) -> Page {
    //NOTE: there is probably room for optimization here, might be able to change this into a closed form expression (instead of summation)
    (0..depth).fold(ROOT_PAGE_OFFSET, |acc, d| acc + num_nodes(d, num_entries))
}

///get page index of a node
pub fn node_page_index(depth: Depth, node: Node, num_entries: Size) -> Page {
    depth_page_index(depth, num_entries) + node
}

///get byte index of first byte in a node
pub fn node_byte_index(depth: Depth, node: Node, num_entries: Size) -> u64 {
    (node_page_index(depth, node, num_entries) * node_size()) as u64
}

pub fn seek_node(
    file: &mut std::fs::File,
    depth: Depth,
    node: Node,
    num_entries: Size,
) -> std::io::Result<u64> {
    let seek_offset = node_byte_index(depth, node, num_entries);
    file.seek(std::io::SeekFrom::Start(seek_offset as u64))?;
    Ok(seek_offset)
}

// pub fn total_nodes(num_entries: Size) -> Size {
//     depth_page_index(tree_depth(num_entries), num_entries) - ROOT_PAGE_OFFSET
// }

///Gets the largest values in each chunk of an array of keys. Useful for building inner nodes of B-tree
/// NOTE: assumes array is sorted
pub fn get_last_in_each_chunk(elements: &[Key], chunk_size: usize) -> Vec<Key> {
    elements
        .chunks(chunk_size)
        .map(|delimeter| {
            let key = delimeter
                .last()
                .expect("Failed to collect last element of each leaf node"); //unwrapping should be safe here, but I'll leave the expect anyway

            *key
        })
        .collect()
}

///Navigate inner nodes of B-tree starting from root, returns page index of where key may be
pub fn btree_navigate(
    db_name: &str,
    level: Level,
    run: Run,
    key: Key,
    num_entries: Size,
    mut buffer_pool: Option<&mut BufferPool>,
) -> std::io::Result<Page> {
    let num_inner_levels = tree_depth(num_entries);

    let mut curr_leaf_page_index: Page = 0;
    let mut next_node: Node = 0;
    for depth in 0..num_inner_levels {
        println!(
            "next_node: {}, num_nodes at depth {}: {}",
            next_node,
            depth,
            num_nodes(depth, num_entries)
        );
        let node_page_index = node_page_index(depth, next_node, num_entries);
        let node_page = get_btree_page(
            db_name,
            level,
            run,
            node_page_index,
            buffer_pool.as_deref_mut(),
        )?; //NOTE: watch out for the deref_mut, we don't want to accdientally copy the buffer pool, TODO: verify this doesn't break it

        let node_delimeters = serde_btree::deserialize(&node_page).unwrap_or_else(|_| panic!("Failed to deserialize B-tree node during B-tree navigation while searching for key: {key}, name: {db_name}, level: {level}, run: {run}, page_index: {node_page_index} num_entries: {num_entries}"));

        // println!("binary search rank: depth {depth}, next_node = {next_node}, num_delimeters {}, delimeters {:?}", root_delimeters.len(), root_delimeters); //TODO: remove
        // println!("next_node, {}", next_node);//TODO: remove
        next_node = binary_search_leftmost(&node_delimeters, key);
        curr_leaf_page_index += next_node * leaves_in_subtree(depth + 1, num_entries);
    }

    assert!(
        curr_leaf_page_index < num_leaves(num_entries),
        "Btree navigated to leaf page index that does not exist, page index: {}, num_leaves {}",
        curr_leaf_page_index,
        num_leaves(num_entries)
    );

    // Ok(next_node as Page)
    Ok(curr_leaf_page_index)
}

mod tests {
    use crate::util::types::Value; //NOTE: my vscode is marking this as unused, but that's not true

    use super::*;

    #[test]
    fn test_num_leaves() {
        let entries_per_page = num_entries_per_page();
        assert_eq!(num_leaves(0), 0);
        assert_eq!(num_leaves(1), 1);

        assert_eq!(num_leaves(entries_per_page), 1);
        assert_eq!(num_leaves(entries_per_page + 1), 2);
        assert_eq!(num_leaves(entries_per_page - 1), 1);

        assert_eq!(num_leaves(entries_per_page * 99), 99);
        assert_eq!(num_leaves(entries_per_page * 99 + 1), 100);
        assert_eq!(num_leaves(entries_per_page * 99 - 1), 99);
    }

    #[test]
    fn test_tree_depth() {
        let entries_per_page = num_entries_per_page();
        let fanout = fanout();

        assert_eq!(tree_depth(0), 0);
        assert_eq!(tree_depth(1), 0); //no internal nodes needed for 1 entry

        assert_eq!(tree_depth(entries_per_page), 0); //no internal nodes needed for 1 page worth of entries
        assert_eq!(tree_depth(entries_per_page + 1), 1); //2 pages worth of entries, need 1 node to manage it
        assert_eq!(tree_depth(entries_per_page - 1), 0); //no internal nodes needed for less than 1 page worth of entries

        //#fanout should be the max number of leaves a single node can handle
        assert_eq!(tree_depth(entries_per_page * fanout), 1);
        assert_eq!(tree_depth(entries_per_page * fanout + 1), 2); //now we need 2 nodes to handle the leaves, and a root to handle those 2 nodes
        assert_eq!(tree_depth(entries_per_page * fanout - 1), 1);

        //#fanout^2 leaves should be handled by #fanout nodes and a root to handle those nodes
        //this amount should be the limit, so anything more needs another level of nodes
        assert_eq!(tree_depth(entries_per_page * fanout.pow(2)), 2);
        assert_eq!(tree_depth(entries_per_page * fanout.pow(2) + 1), 3);
        assert_eq!(tree_depth(entries_per_page * fanout.pow(2) - 1), 2);
    }

    #[test]
    fn test_num_nodes() {
        let entries_per_page = num_entries_per_page();
        let fanout = fanout();

        assert_eq!(num_nodes(0, 0), 0); //no nodes needed for 1 entry
        assert_eq!(num_nodes(0, 1), 1); //1 leaf node needed for 1 entry

        //#fanout should be the max number of leaves a single node can handle
        let num_entries = entries_per_page * fanout;
        assert_eq!(num_nodes(1, num_entries), num_leaves(num_entries)); //confirm above comment
        assert_eq!(num_nodes(0, num_entries), 1); //confirm above comment

        //#fanout should be the max number of leaves a single node can handle
        let num_entries = entries_per_page * fanout + 1;
        assert_eq!(num_nodes(2, num_entries), num_leaves(num_entries)); //confirm above comment
        assert_eq!(num_nodes(1, num_entries), 2); //confirm above comment
        assert_eq!(num_nodes(0, num_entries), 1); //confirm above comment

        //#fanout^2 leaves should be handled by #fanout nodes and a root to handle those nodes
        let num_entries = entries_per_page * fanout.pow(2);
        assert_eq!(num_nodes(2, num_entries), num_leaves(num_entries)); //confirm above comment
        assert_eq!(num_nodes(1, num_entries), fanout); //confirm above comment
        assert_eq!(num_nodes(0, num_entries), 1); //confirm above comment

        //#fanout^2 + 1 leaves should be handled by #fanout + 1 nodes, 2 nodes above those, and a root to handle those nodes
        let num_entries = entries_per_page * fanout.pow(2) + 1;
        assert_eq!(num_nodes(3, num_entries), num_leaves(num_entries)); //confirm above comment
        assert_eq!(num_nodes(2, num_entries), fanout + 1); //confirm above comment
        assert_eq!(num_nodes(1, num_entries), 2); //confirm above comment
        assert_eq!(num_nodes(0, num_entries), 1); //confirm above comment
    }

    #[test]
    fn test_indexing() {
        let entries_per_page = num_entries_per_page();
        let fanout = fanout();

        //#fanout^2 leaves should be handled by #fanout nodes and a root to handle those nodes
        //this amount should be the limit, so anything more needs another level of nodes
        let num_entries = entries_per_page * fanout.pow(2);
        assert_eq!(node_page_index(0, 0, num_entries), 0);
        assert_eq!(node_page_index(1, 0, num_entries), 1);
        assert_eq!(node_page_index(2, 0, num_entries), 1 + fanout);
        assert_eq!(
            node_page_index(3, 0, num_entries),
            1 + fanout + fanout.pow(2)
        );

        //has no inner nodes, NOTE: should not be a real use case
        assert_eq!(node_page_index(0, 0, 0), 0);
        // assert_eq!(node_page_index(1, 2, 0), 0);
    }

    #[test]
    fn get_each_node_largest_entry_test() {
        let b = fanout();
        let entries: Vec<Key> = (0..b * 3 - 100).map(|value| value as Key).collect(); //3 nodes worth of entries
        let largest_in_each_node = get_last_in_each_chunk(&entries, b);

        let expected_largest: Vec<Key> = vec![b as i64 - 1, 2 * b as i64 - 1, 3 * b as i64 - 101];
        assert_eq!(largest_in_each_node, expected_largest);

        //recursively running this should give you the largest element in the subtree
        let largest_in_each_node = get_last_in_each_chunk(&largest_in_each_node, b);
        let expected_largest: Vec<Key> = vec![3 * b as i64 - 101];
        assert_eq!(largest_in_each_node, expected_largest);
    }
}
