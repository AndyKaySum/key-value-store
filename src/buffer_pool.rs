use std::collections::{HashMap, LinkedList};

use crate::util::types::{Page, Size};

type PageKey = (String, Page);

#[derive(Debug)]
struct Frame {
    data: Vec<u8>,
    access: bool,
}

#[allow(dead_code, unused)] //TODO: remove when ready
impl Frame {
    fn new(data: Vec<u8>) -> Self {
        Self { data, access: true }
    }
}

#[allow(dead_code, unused)] //TODO: remove when ready
#[derive(Debug)]
pub struct BufferPool {
    //NOTE: all vectors should be at most system_info::page_size() number of bytes
    frames: HashMap<PageKey, Frame>, //TODO: step 2.1 replace with extendible hashing data structure
    filename_pages: HashMap<String, LinkedList<Page>>, //keeps track of the pages we have in the bufferpool for a given filename, NOTE: we need this for when files are deleted or replaced and the items in the buffer pool are no longer valid
    capacity: Size,
    clock_handle: PageKey, //Option<std::collections::hash_map::IterMut<'a, PageKey, Frame>>
}

#[allow(dead_code, unused)] //TODO: remove when ready
impl BufferPool {
    pub fn new(initial_size: Size, capacity: Size) -> Self {
        Self {
            frames: HashMap::with_capacity(initial_size), //HashMap::with_capacity_and_hasher(capacity, hasher),
            filename_pages: HashMap::new(),
            capacity,
            clock_handle: ("".to_string(), 0), //TODO: change
        }
    }

    pub fn get(&mut self, path: &str, page_index: Page) -> Option<&Vec<u8>> {
        let get_result = self.frames.get_mut(&(path.to_string(), page_index));
        if let Some(frame) = get_result {
            frame.access = true;
            let data = &frame.data;
            return Some(data);
        };
        None
    }

    fn evict(&mut self) {
        //TODO: evict until we have space for one entry (one less than capacity)
    }

    pub fn insert(&mut self, path: &str, page_index: Page, page_data: &[u8]) {
        if self.frames.len() >= self.capacity {
            self.evict();
        }

        let insert_result = self.frames.insert(
            (path.to_string(), page_index),
            Frame::new(page_data.to_vec()),
        );

        assert!(
            insert_result.is_none(),
            "inserted existing key (key should have been removed prior)"
        );

        match self.filename_pages.get_mut(path) {
            Some(page_indexes) => page_indexes.push_back(page_index),
            None => {
                let page_indexes = LinkedList::from([page_index]);
                self.filename_pages.insert(path.to_string(), page_indexes);
            }
        };
    }

    pub fn remove(&mut self, path: &str) {
        let page_indexes_option = self.filename_pages.get(path);
        if let Some(page_indexes) = page_indexes_option {
            for page in page_indexes {
                self.frames.remove(&(path.to_string(), *page));
            }
        }
        self.filename_pages.remove(path);
    }
}
