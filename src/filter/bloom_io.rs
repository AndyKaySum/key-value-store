use std::io::{self, Read, Write};

use crate::{
    buffer_pool::BufferPool,
    file_io::{direct_io, serde_entry, serde_util::nearest_min_write_size_multiple},
    sst::sst_util::{get_sst_page, num_pages},
    util::{
        filename,
        hash::FastHasher,
        types::{Key, Level, Page, Run, Size},
    },
};

use super::{
    bloom_filter::BloomFilter,
    bloom_util::{bitmap_len, bitmap_num_bits, get_bloom_page, num_hash_functions, page_bit_index},
};

///Responsible for writing bloom filter bitmaps to storage and querying bloomfilters in storage
pub struct BloomFilterIO {}

#[allow(dead_code)]
impl BloomFilterIO {
    ///Write bloom filter bitmap to storage
    pub fn write(db_name: &str, level: Level, run: Run, bitmap: &[u8]) -> io::Result<()> {
        let path = filename::bloom_filter_path(db_name, level, run);
        let mut file = direct_io::create(&path)?;

        let mut buffer = bitmap.to_vec();
        buffer.resize(nearest_min_write_size_multiple(buffer.len()), 0);
        file.write_all(&buffer)?;
        file.set_len(bitmap.len() as u64)?;
        Ok(())
    }
    ///Deserialize an entire bloom filter file to bloom filter struct, useful for testing
    pub fn read(
        db_name: &str,
        level: Level,
        run: Run,
        bits_per_entry: Size,
        num_entries: Size,
    ) -> io::Result<BloomFilter> {
        let path = filename::bloom_filter_path(db_name, level, run);
        let mut file = direct_io::open_read(&path)?;

        let bitmap_size = bitmap_len(num_entries, bits_per_entry);
        let mut buffer: Vec<u8> = vec![0; nearest_min_write_size_multiple(bitmap_size)];
        let bytes_read = file.read(&mut buffer)?;

        let mut filter = BloomFilter::new(num_entries, bits_per_entry);

        assert_eq!(bitmap_size, bytes_read, "Incorrect bitmap size");
        filter.bitmap = buffer[0..bitmap_size].to_vec();
        Ok(filter)
    }
    ///Write filter using entries in an SST, useful for compaction
    pub fn write_from_sst(
        db_name: &str,
        level: Level,
        run: Run,
        bits_per_entry: Size,
        num_entries: Size,
    ) -> io::Result<()> {
        let mut filter = BloomFilter::new(num_entries, bits_per_entry);
        for page_index in 0..num_pages(num_entries) {
            let page = get_sst_page(db_name, level, run, page_index, None)?;
            let entries = serde_entry::deserialize(&page).unwrap_or_else(|why| panic!("Failed to deserialize entries during bloom filter creation, db_name: {db_name}, level: {level} run: {run}, page_index: {page_index}, reason: {why}"));
            filter.insert_entries(&entries);
        }

        Self::write(db_name, level, run, &filter.bitmap)
    }
    ///Check if bloom filter file contains an element. Returns false on first 0 found, otherwise true.
    pub fn contains(
        db_name: &str,
        level: Level,
        run: Run,
        key: Key,
        bits_per_entry: Size,
        num_entries: Size,
        mut buffer_pool: Option<&mut BufferPool>,
    ) -> io::Result<bool> {
        let num_hash_functions = num_hash_functions(bits_per_entry);

        //for page caching, just in case bufferpool is disabled
        let mut curr_page_index = Page::MAX;
        let mut curr_page = Vec::<u8>::new();

        for seed in 0..num_hash_functions {
            let bitmap_index = FastHasher::hash_to_index(
                key.to_le_bytes(),
                seed as u64,
                bitmap_num_bits(num_entries, bits_per_entry),
            );
            let (page_index, byte_index, bit_index) = page_bit_index(bitmap_index as usize);

            if page_index != curr_page_index {
                curr_page =
                    get_bloom_page(db_name, level, run, page_index, buffer_pool.as_deref_mut())?;
                curr_page_index = page_index;
            }

            if (curr_page[byte_index] & (1 << bit_index)) == 0 {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        sst::{array_sst, SortedStringTable},
        util::testing::setup_and_test_and_cleaup,
    };

    use super::*;

    #[test]
    fn test() {
        let db_name = "test_bloom_io";
        const LEVEL: Level = 0;
        let mut test = || {
            let level = LEVEL;
            let run = 0;
            let bits_per_entry = 5;
            let entries = vec![(0, 0), (1001, 1001)];
            let num_entries = entries.len();

            array_sst::Sst.write(db_name, level, run, &entries).unwrap();
            BloomFilterIO::write_from_sst(db_name, level, run, bits_per_entry, num_entries)
                .unwrap();

            let read_filter =
                BloomFilterIO::read(db_name, level, run, bits_per_entry, num_entries).unwrap();

            assert!(read_filter.contains(0));
            assert!(read_filter.contains(1001));
            assert!(!read_filter.contains(1002));

            let contains = |key| {
                BloomFilterIO::contains(
                    db_name,
                    level,
                    run,
                    key,
                    bits_per_entry,
                    entries.len(),
                    None,
                )
                .unwrap()
            };

            assert!(contains(0));
            assert!(contains(1001));
            assert!(!contains(1002));
        };
        setup_and_test_and_cleaup(db_name, LEVEL, &mut test);
    }
}
