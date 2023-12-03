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

#[derive(Debug, Clone)] //TODO: remove Clone
struct Frame {
    //NOTE: all vectors should be at most system_info::page_size() number of bytes
    data: Vec<u8>,
}

#[allow(dead_code, unused)] //TODO: remove when ready
impl Frame {
    fn new(data: Vec<u8>) -> Self {
        Self { data }
    }
}

#[allow(dead_code, unused)] //TODO: remove when ready
#[derive(Debug)]
pub struct BufferPool {
    frames: ExtendibleHashTable<PageKey, Frame, FastHasher>,
    filename_pages: HashMap<PathString, HashSet<Page>>, //keeps track of the pages we have in the bufferpool for a given filename, NOTE: we need this for when files are deleted or replaced and the items in the buffer pool are no longer valid
    capacity: Size,
    clock_handle: usize, //index into buckets array in our extendible hashtable, used for clock+LRU hybrid
}

#[allow(dead_code, unused)] //TODO: remove when ready
impl BufferPool {
    pub fn new(initial_size: Size, capacity: Size) -> Self {
        Self {
            frames: ExtendibleHashTable::with_capacity_buckets(10, initial_size, initial_size), //HashMap::with_capacity_and_hasher(capacity, hasher),
            filename_pages: HashMap::new(),
            capacity,
            clock_handle: 0, //TODO: change
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
        let get_result = self.frames.get(&(path.to_string(), page_index));
        if let Some(frame) = get_result {
            let data = frame.data;
            return Some(data);
        };
        None
    }

    fn move_clock_handle(&mut self) {
        self.clock_handle += 1;
        self.clock_handle %= self.frames.num_buckets();
    }

    fn evict(&mut self, num_to_evict: Size) {
        //TODO: evict until we have space for one entry (one less than capacity)
        let mut num_evicted = 0;
        while num_evicted < num_to_evict {
            let handle = self.clock_handle;
            let frames = &mut self.frames;
            if frames.accessed(handle) {
                frames.bucket_remove_lru(handle);
                num_evicted += 1;
            } else {
                frames.set_accessed(handle, false);
            }
            self.move_clock_handle();
        }
    }

    pub fn insert(&mut self, path: &str, page_index: Page, page_data: &[u8]) {
        if self.len() >= self.capacity {
            self.evict(self.len() - self.capacity + 1); //evict enough, so that we have space for 1 insertion
        }

        //Add to actual buffer pool
        let mut count = 0;
        let failure_threshold = 10;
        //NOTE: if very unlucky extendible hashtable may fail to add values, happens when a bucket splits and all existing values are
        while !self.frames.try_insert(
            (path.to_string(), page_index),
            Frame::new(page_data.to_vec()),
        ) && count < failure_threshold
        {
            count += 0;
        }

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
