use std::io;

use crate::{buffer_pool::BufferPool, util::types::Page};

use super::direct_io;

//This file is responsible for getting pages through the buffer pool if enabled
// and interacting with files in a way that keeps the buffer pool in sync with
// the state of the file system

pub fn get_page(
    path: &str,
    page_index: Page,
    buffer_pool: Option<&mut BufferPool>,
) -> io::Result<Vec<u8>> {
    if let Some(pool) = buffer_pool {
        //NOTE: watch out for bugs from this, not super sure how things work out here
        if let Some(page) = pool.get(path, page_index) {
            Ok(page)
        } else {
            let mut file = direct_io::open_read(path)?;
            let page_bytes = direct_io::read_page(&mut file, page_index)?;
            pool.insert(path, page_index, &page_bytes);
            Ok(page_bytes)
        }
    } else {
        let mut file = direct_io::open_read(path)?;
        direct_io::read_page(&mut file, page_index)
    }
}

pub fn remove_file(path: &str, buffer_pool: Option<&mut BufferPool>) -> io::Result<()> {
    if let Some(pool) = buffer_pool {
        pool.remove(path)
    }
    std::fs::remove_file(path)
}

pub fn rename_file(
    old_path: &str,
    new_path: &str,
    buffer_pool: Option<&mut BufferPool>,
) -> io::Result<()> {
    if let Some(pool) = buffer_pool {
        pool.rename(old_path, new_path)
    }
    std::fs::rename(old_path, new_path)
}
