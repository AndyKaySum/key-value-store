pub mod merge_k_lists {
    use std::cmp::Ordering;
    use std::collections::BinaryHeap;

    #[derive(Eq, PartialEq)]
    pub struct Node {
        key: i64,
        val: i64,
        list_idx: usize,
        elem_idx: usize,
    }

    impl Ord for Node {
        fn cmp(&self, other: &Self) -> Ordering {
            other.val.cmp(&self.key)
        }
    }

    impl PartialOrd for Node {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    pub fn merge_k_sorted_lists(lists: Vec<Vec<(i64, i64)>>) -> Vec<(i64, i64)> {
        let mut heap = BinaryHeap::new();
        let mut result = vec![];

        for (list_idx, list) in lists.iter().enumerate() {
            if !list.is_empty() {
                heap.push(Node {
                    key: list[0].0,
                    val: list[0].1,
                    list_idx,
                    elem_idx: 0,
                });
            }
        }

        while let Some(Node {
            key,
            val,
            list_idx,
            elem_idx,
        }) = heap.pop()
        {
            result.push((key, val));

            if elem_idx + 1 < lists[list_idx].len() {
                heap.push(Node {
                    key: lists[list_idx][elem_idx + 1].0,
                    val: lists[list_idx][elem_idx + 1].1,
                    list_idx,
                    elem_idx: elem_idx + 1,
                });
            }
        }

        result
    }
}
