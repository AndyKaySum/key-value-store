use crate::{
    buffer_pool::BufferPool,
    ceil_div,
    file_io::{file_interface, serde_entry},
    util::{
        filename,
        system_info::num_entries_per_page,
        types::{Entry, Level, Page, Run, Size},
    },
};
use std::io;

pub fn num_pages(num_entries: Size) -> Size {
    ceil_div!(num_entries, num_entries_per_page())
}

pub fn get_sst_page(
    db_name: &str,
    level: Level,
    run: Run,
    page_index: Page,
    buffer_pool: Option<&mut BufferPool>,
) -> io::Result<Vec<u8>> {
    let path = filename::sst_path(db_name, level, run);
    file_interface::get_page(&path, page_index, buffer_pool)
}

///Get page from bufferpool or through I/O and return the entries in that page
pub fn get_entries_at_page(
    db_name: &str,
    level: Level,
    run: Run,
    page_index: Page,
    buffer_pool: Option<&mut BufferPool>,
) -> io::Result<Vec<Entry>> {
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
    file_interface::get_page(&path, page_index, buffer_pool)
}
