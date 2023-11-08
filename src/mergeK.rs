use std::collections::BinaryHeap;
use std::cmp::Ordering;
use std::cmp::Reverse;

#[derive(Eq, PartialEq)]
struct Node {
    val: i64,
    list_idx: usize,
    elem_idx: usize,
}

impl Ord for Node {
    fn cmp(&self, other: &Self) -> Ordering {
        other.val.cmp(&self.val)
    }
}

impl PartialOrd for Node {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn merge_k_sorted_lists(lists: Vec<Vec<i64>>) -> Vec<i64> {
    let mut heap = BinaryHeap::new();
    let mut result = vec![];

    for (list_idx, list) in lists.iter().enumerate() {
        if !list.is_empty() {
            heap.push(Node { val: list[0], list_idx, elem_idx: 0 });
        }
    }

    while let Some(Node { val, list_idx, elem_idx }) = heap.pop() {
        result.push(val);

        if elem_idx + 1 < lists[list_idx].len() {
            heap.push(Node {
                val: lists[list_idx][elem_idx + 1],
                list_idx,
                elem_idx: elem_idx + 1,
            });
        }
    }

    result
}

