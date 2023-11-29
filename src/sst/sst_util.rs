use crate::{
    buffer_pool::BufferPool,
    file_io::{direct_io, serde_entry},
    util::{
        filename,
        types::{Key, Level, Page, Run, Value},
    },
};
use std::io;

pub fn get_page(
    path: &str,
    page_index: Page,
    buffer_pool: Option<&mut BufferPool>,
) -> io::Result<Vec<u8>> {
    if let Some(pool) = buffer_pool {
        //NOTE: watch out for bugs from this, not super sure how things work out here
        if let Some(page) = pool.get(path, page_index) {
            Ok(page.to_vec())
        } else {
            let mut file = direct_io::open_read(path)?;
            let page_bytes = direct_io::read_page(&mut file, page_index)?;
            pool.insert(path, page_index, page_bytes.as_slice());
            Ok(page_bytes)
        }
    } else {
        let mut file = direct_io::open_read(path)?;
        direct_io::read_page(&mut file, page_index)
    }
}

pub fn get_sst_page(
    db_name: &str,
    level: Level,
    run: Run,
    page_index: Page,
    buffer_pool: Option<&mut BufferPool>,
) -> io::Result<Vec<u8>> {
    let path = filename::sst_path(db_name, level, run);
    get_page(&path, page_index, buffer_pool)
}

///Get page from bufferpool or through I/O and return the entries in that page
pub fn get_entries_at_page(
    db_name: &str,
    level: Level,
    run: Run,
    page_index: Page,
    buffer_pool: Option<&mut BufferPool>,
) -> io::Result<Vec<(Key, Value)>> {
    let page = get_sst_page(db_name, level, run, page_index, buffer_pool)?;
    let entries = serde_entry::deserialize(&page).unwrap_or_else(|_| {
        panic!(
            "Failed to deserialize page {} from db {db_name} level {level} run {run}",
            page_index
        )
    });
    Ok(entries)
}

pub fn get_btree_page(
    db_name: &str,
    level: Level,
    run: Run,
    page_index: Page,
    buffer_pool: Option<&mut BufferPool>,
) -> io::Result<Vec<u8>> {
    let path = filename::sst_btree_path(db_name, level, run);
    get_page(&path, page_index, buffer_pool)
}
