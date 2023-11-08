use serde_entry::{deserialize_from, serialize_into};
use std::{fs, io}; //can swap "serde_entry" with "bincode" to swap functions

use crate::file_io::{direct_io, serde_entry};
use crate::{
    buffer_pool::BufferPool,
    file_io::{
        direct_io::read_page,
        serde_entry::{deserialize, deserialize_entry_within_page},
    },
    util::{
        filename, system_info,
        types::{Key, Level, Page, Run, Size, Value, ENTRY_SIZE},
    },
};

use super::SortedStringTable;

fn index_to_2d_index(row_size: usize, index: usize) -> (usize, usize) {
    let row_index = index / row_size;
    let index_within_row = index % row_size;
    (row_index, index_within_row)
}

fn get_page(
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
            let page_bytes = read_page(&mut file, page_index)?;
            pool.insert_sst_page(&level, &run, &page_index, page_bytes.clone());
            Ok(page_bytes)
        }
    } else {
        let mut file = direct_io::open_read(&filename::sst_path(db_name, level, run))?;
        read_page(&mut file, page_index)
    }
}

pub struct Sst;

impl SortedStringTable for Sst {
    ///Writes key-value array (or vec) onto SST file in appropriate directory.
    ///NOTE: Avoid using arrays larger than the size of the buffer. We shouldn't need to handle very large writes
    /// since compaction will be implemented after we switch to a static btree implementation
    fn write(
        &self,
        db_name: &str,
        level: Level,
        run: Run,
        array: &[(Key, Value)],
    ) -> io::Result<()> {
        //create directory for the level if needed
        let directory = filename::lsm_level_directory(db_name, level);
        if !direct_io::path_exists(&directory) {
            fs::create_dir(&directory)?;
        }

        let path = filename::sst_path(db_name, level, run);
        let mut file = direct_io::create(&path)?;
        serialize_into(&mut file, array)?;
        Ok(())
    }

    ///Deserializes entire SST to entry vec
    fn read(&self, db_name: &str, level: Level, run: Run) -> io::Result<Vec<(Key, Value)>> {
        let mut file = direct_io::open_read(&filename::sst_path(db_name, level, run))?;
        deserialize_from(&mut file)
    }

    fn get(
        &self,
        db_name: &str,
        level: Level,
        run: Run,
        key: Key,
        mut buffer_pool: Option<&mut BufferPool>,
    ) -> io::Result<Option<Value>> {
        let num_entries = self.len(db_name, level, run)? as u64; //TODO: remove this and keep track of file sizes somewhere

        let mut curr_page_index = usize::MAX;
        let mut curr_page = Vec::<u8>::new();

        let mut get_middle = |left: i64, right: i64| -> io::Result<((Key, Value), i64)> {
            let middle_index = (left + right) / 2;
            let (middle_page_index, entry_index) =
                index_to_2d_index(system_info::num_entries_per_page(), middle_index as usize);
            //check if we need to read in a new page
            if middle_page_index != curr_page_index {
                curr_page_index = middle_page_index;
                let bp = buffer_pool.as_deref_mut(); //NOTE: watch out for this, not quite sure if it will cause bugs, shouldn't though
                curr_page = get_page(db_name, level, run, middle_page_index, bp)?;
            };
            // let middle_entry = curr_page_entries[entry_index];
            let middle_entry = deserialize_entry_within_page(&curr_page, entry_index)
                .expect("Invalid number of bytes in page");
            Ok((middle_entry, middle_index))
        };

        //https://en.wikipedia.org/wiki/Binary_search_algorithm#Procedure
        //implemented "non alternate" version to optimize for I/O operations
        let (mut left, mut right): (i64, i64) = (0, num_entries as i64 - 1);
        while left <= right {
            let ((middle_key, middle_value), middle_index) = get_middle(left, right)?;
            match middle_key.cmp(&key) {
                std::cmp::Ordering::Less => left = middle_index + 1,
                std::cmp::Ordering::Greater => right = middle_index - 1,
                std::cmp::Ordering::Equal => return Ok(Some(middle_value)),
            };
        }
        Ok(None)
    }
    ///Perform binary search to find the starting and end positions for our scan, then append all values within those bounds
    fn scan(
        &self,
        db_name: &str,
        level: Level,
        run: Run,
        key1: Key,
        key2: Key,
        mut buffer_pool: Option<&mut BufferPool>,
    ) -> io::Result<Vec<(Key, Value)>> {
        let num_entries = self.len(db_name, level, run)? as u64; //TODO: remove this and keep track of file sizes somewhere

        let mut results: Vec<(Key, Value)> = Vec::new();

        //hold onto the current page we're looking at to avoid some repeated deserialization
        let mut curr_page_index = usize::MAX;
        let mut curr_page = Vec::<u8>::new();

        let mut get_middle = |left: i64, right: i64| -> io::Result<(Key, i64)> {
            let middle_index = (left + right) / 2;
            let (middle_page_index, entry_index) =
                index_to_2d_index(system_info::num_entries_per_page(), middle_index as usize);
            //check if we need to read in a new page
            if middle_page_index != curr_page_index {
                curr_page_index = middle_page_index;
                let bp = buffer_pool.as_deref_mut(); //NOTE: watch out for this (.as_deref_mut), not quite sure if it will cause bugs, shouldn't though
                curr_page = get_page(db_name, level, run, middle_page_index, bp)?;
            };
            // let (middle_key, _) = curr_page_entries[entry_index];
            let (middle_key, _) = deserialize_entry_within_page(&curr_page, entry_index)
                .expect("Invalid number of bytes in page");
            Ok((middle_key, middle_index))
        };

        //step 1: find position of inclusive lowerbound
        //https://en.wikipedia.org/wiki/Binary_search_algorithm#Procedure_for_finding_the_leftmost_element
        let (mut left, mut right) = (0, num_entries as i64);
        while left < right {
            let (middle_key, middle_index) = get_middle(left, right)?;
            if middle_key < key1 {
                left = middle_index + 1;
            } else {
                right = middle_index;
            }
        }
        let lowerbound_index = left;
        if lowerbound_index >= num_entries as i64 {
            return Ok(vec![]);
        }
        let (lowerbound_page_index, lowerbound_within_page_index) = index_to_2d_index(
            system_info::num_entries_per_page(),
            lowerbound_index as usize,
        );

        //step 2: find position of inclusive upperbound
        //https://en.wikipedia.org/wiki/Binary_search_algorithm#Procedure_for_finding_the_rightmost_element
        let (mut left, mut right): (i64, i64) = (0, num_entries as i64);
        while left < right {
            let (middle_key, middle_index) = get_middle(left, right)?;
            if middle_key > key2 {
                right = middle_index;
            } else {
                left = middle_index + 1;
            }
        }
        let upperbound_index = right - 1;
        if upperbound_index < 0 {
            return Ok(vec![]);
        }
        let (upperbound_page_index, upperbound_within_page_index) = index_to_2d_index(
            system_info::num_entries_per_page(),
            upperbound_index as usize,
        );

        //EDGE CASE: lowerbound and upperbound are in the same page
        //NOTE: this case means the work we did to get the lowerbound_entries and upperbound_entries array slices is wasted, hopefully compiler optimization can handle that
        if lowerbound_page_index == upperbound_page_index {
            //NOTE: curr_page_entries should contain all the values within our bounds
            results = deserialize(&curr_page).unwrap_or_else(|_| panic!("Unable to deserialize lowerbound page during scan, level: {level}, run: {run} page_index: {lowerbound_page_index}"))[lowerbound_within_page_index..upperbound_within_page_index + 1].to_vec();
            return Ok(results);
        }

        //NOTE: we set upperbound page first because there's a higher change that the if condition is true and we don't need to go back for a page
        let upperbound_bound_page = if curr_page_index == lowerbound_page_index {
            curr_page.to_owned()
        } else {
            get_page(
                db_name,
                level,
                run,
                upperbound_page_index,
                buffer_pool.as_deref_mut(),
            )?
        };
        let upperbound_entries = &deserialize(&upperbound_bound_page).unwrap_or_else(|_| panic!("Unable to deserialize upperbound page during scan, level: {level}, run: {run} page_index: {upperbound_page_index}"))[..upperbound_within_page_index + 1]; //NOTE: curr_page_entries should be the same page that we found our upperbound in

        let lower_bound_page = if curr_page_index == lowerbound_page_index {
            curr_page
        } else {
            get_page(
                db_name,
                level,
                run,
                lowerbound_page_index,
                buffer_pool.as_deref_mut(),
            )?
        };
        let lowerbound_entries = &deserialize(&lower_bound_page).unwrap_or_else(|_| panic!("Unable to deserialize lowerbound page during scan, level: {level}, run: {run} page_index: {lowerbound_page_index}"))[lowerbound_within_page_index..]; //NOTE: curr_page_entries should be the same page that we found our lowerbound in

        //step 3: get all entries between the pages that contain our lowerbound and upperbound values
        //NOTE: by this point we have all the values in the pages that contain our bounds
        results.extend_from_slice(lowerbound_entries);

        for i in (lowerbound_page_index + 1)..upperbound_page_index {
            // let page_entries = deserialize_page(&mut file, i)?;
            let page = get_page(db_name, level, run, i, buffer_pool.as_deref_mut())?;
            let page_entries = &deserialize(&page).unwrap_or_else(|_| panic!("Unable to deserialize page during scan, level: {level}, run: {run} page_index: {i}"));

            results.extend(page_entries);
        }
        results.extend_from_slice(upperbound_entries);

        Ok(results)
    }
    ///Gets the number of entries in an sst
    fn len(&self, db_name: &str, level: Level, run: Run) -> io::Result<Size> {
        let byte_count = direct_io::open_read(&filename::sst_path(db_name, level, run))?
            .metadata()?
            .len();
        Ok(byte_count as Size / ENTRY_SIZE)
    }
}
