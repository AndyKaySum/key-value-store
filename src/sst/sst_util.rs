use crate::{
    buffer_pool::BufferPool,
    file_io::direct_io,
    util::{
        filename,
        types::{Level, Page, Run},
    },
};
use std::io;

pub fn get_sst_page(
    db_name: &str,
    level: Level,
    run: Run,
    page_index: Page,
    buffer_pool: Option<&mut BufferPool>,
) -> io::Result<Vec<u8>> {
    if let Some(pool) = buffer_pool {
        //NOTE: watch out for bugs from this, not super sure how things work out here
        if let Some(page) = pool.get_sst_page(&level, &run, &page_index) {
            Ok(page.to_vec())
        } else {
            let mut file = direct_io::open_read(&filename::sst_path(db_name, level, run))?;
            let page_bytes = direct_io::read_page(&mut file, page_index)?;
            pool.insert_sst_page(&level, &run, &page_index, page_bytes.clone());
            Ok(page_bytes)
        }
    } else {
        let mut file = direct_io::open_read(&filename::sst_path(db_name, level, run))?;
        direct_io::read_page(&mut file, page_index)
    }
}

pub fn get_btree_page(
    db_name: &str,
    level: Level,
    run: Run,
    page_index: Page,
    buffer_pool: Option<&mut BufferPool>,
) -> io::Result<Vec<u8>> {
    if let Some(pool) = buffer_pool {
        //NOTE: watch out for bugs from this, not super sure how things work out here
        if let Some(page) = pool.get_sst_page(&level, &run, &page_index) {
            //TODO: change this to get btree page instead
            Ok(page.to_vec())
        } else {
            let mut file = direct_io::open_read(&filename::sst_btree_path(db_name, level, run))?;
            let page_bytes = direct_io::read_page(&mut file, page_index)?;
            pool.insert_sst_page(&level, &run, &page_index, page_bytes.clone());
            Ok(page_bytes)
        }
    } else {
        let mut file = direct_io::open_read(&filename::sst_btree_path(db_name, level, run))?;
        direct_io::read_page(&mut file, page_index)
    }
}
