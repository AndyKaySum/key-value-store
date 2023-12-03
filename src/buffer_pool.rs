use std::collections::{HashMap, HashSet};

use crate::{
    data_structures::extendible_hash_table::ExtendibleHashTable,
    util::{
        hash::FastHasher,
        types::{Page, Size},
    },
};

type PathString = String;
type PageKey = (PathString, Page);

#[derive(Debug, Clone)]
struct Frame {
    //NOTE: all vectors should be at most system_info::page_size() number of bytes
    bytes: Vec<u8>,
}

impl Frame {
    fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }
}

#[derive(Debug)]
pub struct BufferPool {
    frames: ExtendibleHashTable<PageKey, Frame, FastHasher>,
    filename_pages: HashMap<PathString, HashSet<Page>>, //keeps track of the pages we have in the bufferpool for a given filename, NOTE: we need this for when files are deleted or replaced and the items in the buffer pool are no longer valid
    capacity: Size,
    clock_handle: usize, //index into buckets array in our extendible hashtable, used for clock+LRU hybrid
}

#[allow(dead_code)] //TODO: remove when ready
impl BufferPool {
    pub fn new(initial_size: Size, capacity: Size) -> Self {
        Self {
            frames: ExtendibleHashTable::with_capacity_buckets(10, initial_size, initial_size), //HashMap::with_capacity_and_hasher(capacity, hasher),
            filename_pages: HashMap::new(),
            capacity,
            clock_handle: 0,
        }
    }

    ///Number of elements in the buffer pool
    pub fn len(&self) -> Size {
        self.frames.len()
    }

    pub fn capacity(&self) -> Size {
        self.capacity
    }
    pub fn set_capacity(&mut self, capacity: Size) {
        self.capacity = capacity;
        if self.len() > capacity {
            self.evict(self.len() - capacity);
        }
    }

    pub fn get(&mut self, path: &str, page_index: Page) -> Option<Vec<u8>> {
        self.move_clock_handle();

        let get_result = self.frames.get(&(path.to_string(), page_index));
        if let Some(frame) = get_result {
            let data = frame.bytes;
            return Some(data);
        };
        None
    }

    fn move_clock_handle(&mut self) {
        let handle = &mut self.clock_handle;
        *handle += 1;
        *handle %= self.frames.num_buckets();
        self.frames.set_accessed(*handle, false);
    }

    fn evict(&mut self, num_to_evict: Size) {
        let mut num_evicted = 0;
        while num_evicted < num_to_evict {
            let handle = self.clock_handle;
            let frames = &mut self.frames;

            //if this bucket has not been accessed and we can remove its least recently used page, increment counter
            if !frames.accessed(handle) && frames.bucket_remove_lru(handle) {
                num_evicted += 1;
            }
            self.move_clock_handle();
        }
    }

    pub fn insert(&mut self, path: &str, page_index: Page, page_data: &[u8]) {
        if self.len() >= self.capacity {
            self.evict(self.len() - self.capacity + 1); //evict enough, so that we have space for 1 insertion
        }

        self.frames.put(
            (path.to_string(), page_index),
            Frame::new(page_data.to_vec()),
        );

        //Add page index to our metadata hashtable
        match self.filename_pages.get_mut(path) {
            Some(page_indexes) => {
                page_indexes.insert(page_index);
            }
            None => {
                let page_indexes = HashSet::from([page_index]);
                self.filename_pages.insert(path.to_string(), page_indexes);
            }
        };
    }

    pub fn remove(&mut self, path: &str) {
        if let Some(page_indexes) = self.filename_pages.get(path) {
            for page in page_indexes {
                self.frames.remove(&(path.to_string(), *page));
            }
        }
        self.filename_pages.remove(path);
    }
}

#[cfg(test)]
mod tests {
    use crate::util::system_info::page_size;

    use super::*;
    #[test]
    fn test_insert() {
        let mut b = BufferPool::new(1, 3);
        let page_data = vec![0, 0, 1, 0, 1];
        let path = "database/0/0.sst";
        let page_index = 0;
        b.insert(path, page_index, &page_data);

        let result = b.get(path, page_index);
        assert_eq!(result, Some(page_data));
    }

    #[test]
    fn test_insert_replacement() {
        let mut b = BufferPool::new(1, 3);
        let page_data = vec![0, 0, 1, 0, 1];
        let path = "database/0/0.sst";
        let page_index = 0;
        b.insert(path, page_index, &page_data);

        let result = b.get(path, page_index);
        assert_eq!(result, Some(page_data));

        let replacement_data = vec![1, 1, 1, 1, 1];
        b.insert(path, page_index, &replacement_data);

        assert_eq!(b.len(), 1); //replacement should not change length

        let result = b.get(path, page_index);
        assert_eq!(result, Some(replacement_data));
    }

    #[test]
    fn test_eviction() {
        let mut b = BufferPool::new(1, 3);
        let path = "database/0/0.sst";
        b.insert(path, 0, &[0, 0, 0, 0, 0]);
        b.insert(path, 1, &[0, 0, 0, 0, 1]);
        b.insert(path, 2, &[0, 0, 0, 1, 0]);
        b.insert(path, 3, &[0, 0, 0, 1, 1]);

        assert_eq!(b.len(), 3); //Make sure that we don't go over capacity

        //check if new page is added and oldest is evicted
        assert_eq!(b.get(path, 3), Some(vec![0, 0, 0, 1, 1]));
        assert_eq!(b.get(path, 0), None);

        b.insert(path, 4, &[0, 0, 1, 0, 0]);

        //check if new page is added and oldest is evicted
        assert_eq!(b.get(path, 4), Some(vec![0, 0, 1, 0, 0]));
        assert_eq!(b.get(path, 1), None);

        //Our oldest page should be 2 at this point, when we access it, 3 should be our oldest and get evicted on next insert
        b.get(path, 2);

        b.insert(path, 5, &[0, 0, 1, 0, 1]);

        //check if new page is added and oldest is evicted
        assert_eq!(b.get(path, 5), Some(vec![0, 0, 1, 0, 1]));
        assert_eq!(b.get(path, 3), None);
    }

    #[test]
    fn test_remove() {
        let mut b = BufferPool::new(1, 3);
        let path = "database/0/0.sst";
        let path2 = "database/0/1.sst";
        b.insert(path, 0, &[0, 0, 0, 0, 0]);
        b.insert(path, 1, &[0, 0, 0, 0, 1]);
        b.insert(path2, 0, &[0, 0, 0, 1, 0]);

        //Should remove all pages with path, but nothing else
        b.remove(path);
        assert_eq!(b.get(path, 0), None);
        assert_eq!(b.get(path, 1), None);
        assert_eq!(b.get(path2, 0), Some(vec![0, 0, 0, 1, 0]));
    }

    #[test]
    fn test_set_capacity() {
        let mut b = BufferPool::new(1, 3);
        let path = "database/0/0.sst";
        b.insert(path, 0, &[0, 0, 0, 0, 0]);
        b.insert(path, 1, &[0, 0, 0, 0, 1]);
        b.insert(path, 2, &[0, 0, 0, 1, 0]);
        b.insert(path, 3, &[0, 0, 0, 1, 1]);

        assert_eq!(b.len(), 3);
        assert!(b.len() <= b.capacity());

        b.set_capacity(1);
        assert_eq!(b.get(path, 0), None);
        assert_eq!(b.get(path, 1), None);
        assert_eq!(b.get(path, 2), None);
        assert_eq!(b.get(path, 3), Some(vec![0, 0, 0, 1, 1]));
        assert_eq!(b.len(), 1);

        assert!(b.len() <= b.capacity());
    }

    #[test]
    fn test_large() {
        let mut b = BufferPool::new(4, 10);
        let path = "database/0/0.sst";

        let page = |_index| {
            let page_data: Vec<u8> = vec![0; page_size()];
            page_data
        };

        for i in 0..10000 {
            b.insert(path, i, &page(i));
            assert!(
                b.len() <= b.capacity(),
                "Went over capacity at insertion {}",
                i
            );
        }

        assert!(b.len() <= b.capacity());
    }
}
