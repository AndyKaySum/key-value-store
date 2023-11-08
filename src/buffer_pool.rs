use std::collections::{HashMap, LinkedList};

use crate::util::{
    hash_key,
    types::{Level, Page, Run, Size},
};

//NOTE: feel free to change any of this, but try to support the functions get_sst_page and insert_sst page
//NOTE: note, we will need to handle the case when an sst is deleted (happens during compaction) and the data is invalid (we don't wawnt to accidentally read an invalid page)
//      that's what valid_pages was supposed to do (keeps track of which pages are in the buffer pool and on removal, deletes those). We want to handle the invalid data completely within this struct

#[allow(dead_code, unused)] //TODO: remove when ready
#[derive(Debug)]
struct Frame {
    flags: u8,
    data: Vec<u8>,
}

#[allow(dead_code, unused)] //TODO: remove when ready
impl Frame {
    const FLAG_CLOCK: u8 = 0b1;
    const FLAG_DIRTY: u8 = 0b10; //means frame is not consistent with value in storage

    fn new(data: Vec<u8>) -> Self {
        Self {
            flags: Self::FLAG_CLOCK,
            data,
        }
    }
}

#[derive(Debug)]
pub struct BufferPool {
    //NOTE: all vectors should be at most system_info::page_size() number of bytes
    frames: HashMap<String, Frame>, //TODO: step 2.1 replace with extendible hashing data structure
    valid_pages: HashMap<String, LinkedList<Page>>, //for keeping track of which pages need to be removed when a run is removed (happens during compaction)
}

#[allow(dead_code, unused)] //TODO: remove when ready
impl BufferPool {
    pub fn new(initial_size: Size, capacity: Size) -> Self {
        Self {
            frames: HashMap::new(),
            valid_pages: HashMap::new(),
        }
    }
    pub fn get_sst_page(&self, level: &Level, run: &Run, page_index: &Page) -> Option<&Vec<u8>> {
        self.frames
            .get(&hash_key::sst_page(level, run, page_index))
            .map(|frame| &frame.data)
        // None
    }
    pub fn insert_sst_page(
        &mut self,
        level: &Level,
        run: &Run,
        page_index: &Page,
        page_bytes: Vec<u8>,
    ) {
        self.frames.insert(
            hash_key::sst_page(level, run, page_index),
            Frame::new(page_bytes),
        );

        let sst_key = hash_key::sst(level, run);
        match self.valid_pages.get_mut(&sst_key) {
            Some(valid_sst_pages) => valid_sst_pages.push_back(page_index.to_owned()),
            None => {
                self.valid_pages
                    .insert(sst_key, LinkedList::from([page_index.to_owned()]));
            }
        }
    }

    ///Only use this internally
    fn remove_sst_frame(&mut self, level: &Level, run: &Run, page_index: &Page) {
        self.frames
            .remove(&hash_key::sst_page(level, run, page_index));
    }

    fn remove_sst(&mut self, level: &Level, run: &Run) {
        let sst_key = hash_key::sst(level, run);
        let pages_to_remove = self.valid_pages.get(&sst_key);
        if let Some(pages) = pages_to_remove {
            for page_index in pages {
                self.frames
                    .remove(&hash_key::sst_page(level, run, page_index));
            }
            self.valid_pages.remove(&sst_key);
        };
    }
}
