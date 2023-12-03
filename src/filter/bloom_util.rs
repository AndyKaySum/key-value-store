use crate::{
    buffer_pool::BufferPool,
    ceil_div,
    file_io::file_interface,
    util::{
        filename,
        system_info::page_size,
        types::{Level, Page, Run, Size},
    },
};

pub const BYTE_SIZE: usize = 8;

pub fn bitmap_len(num_entries: Size, bits_per_entry: Size) -> Size {
    ceil_div!(num_entries * bits_per_entry, BYTE_SIZE)
}

pub fn bitmap_num_bits(num_entries: Size, bits_per_entry: Size) -> Size {
    ceil_div!(num_entries * bits_per_entry, BYTE_SIZE) * BYTE_SIZE
}

///Convert bitmap index to to (byte, bit) index
pub fn bit_index(bitmap_index: usize) -> (usize, usize) {
    let byte_index = bitmap_index / BYTE_SIZE;
    let bit_index = bitmap_index % BYTE_SIZE;
    (byte_index, bit_index)
}

///convert bitmap bit index to (page_index, byte_index, bit_index)
pub fn page_bit_index(bitmap_index: usize) -> (Page, usize, usize) {
    let (byte_index, bit_index) = bit_index(bitmap_index);

    let page_index = byte_index / page_size();
    let byte_within_page_index = byte_index % page_size();

    (page_index, byte_within_page_index, bit_index)
}

pub fn num_hash_functions(bits_per_entry: usize) -> usize {
    let opt_num = (bits_per_entry as f64 * std::f64::consts::LN_2).ceil() as usize;
    std::cmp::max(opt_num, 1) //always use at least 1 hash function
}

pub fn get_bloom_page(
    db_name: &str,
    level: Level,
    run: Run,
    page_index: Page,
    buffer_pool: Option<&mut BufferPool>,
) -> std::io::Result<Vec<u8>> {
    let path = filename::bloom_filter_path(db_name, level, run);
    file_interface::get_page(&path, page_index, buffer_pool)
}

#[test]
fn test_bitmap_indexing_conversion() {
    let bitmap_index = 21313;
    let (page_index, byte_index, bit_index) = page_bit_index(bitmap_index);
    assert_eq!(
        page_index * page_size() + byte_index * BYTE_SIZE + bit_index,
        bitmap_index
    )
}
