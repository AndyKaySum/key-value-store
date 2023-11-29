use crate::{
    buffer_pool::BufferPool,
    db::Database,
    file_io::{
        direct_io, file_interface,
        serde_entry::{
            self, deserialize, deserialize_entry_within_page, deserialize_from, serialize_into,
        },
    },
    sst::sst_util::{get_entries_at_page, num_pages},
    util::{
        filename,
        system_info::{self, num_entries_per_page, ENTRY_SIZE},
        types::{Key, Level, Page, Run, Size, Value},
    },
};
use std::{collections::BinaryHeap, fs, io};

use super::{sst_util::get_sst_page, SortedStringTable};

fn index_to_2d_index(row_size: usize, index: usize) -> (usize, usize) {
    let row_index = index / row_size;
    let index_within_row = index % row_size;
    (row_index, index_within_row)
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
        entries: &[(Key, Value)],
    ) -> io::Result<()> {
        //create directory for the level if needed
        let directory = filename::lsm_level_directory(db_name, level);
        if !direct_io::path_exists(&directory) {
            fs::create_dir(&directory)?;
        }

        let path = filename::sst_path(db_name, level, run);
        let mut file = direct_io::create(&path)?;
        serialize_into(&mut file, entries)?;
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
        num_entries: Size,
        mut buffer_pool: Option<&mut BufferPool>,
    ) -> io::Result<Option<Value>> {
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
                curr_page = get_sst_page(db_name, level, run, middle_page_index, bp)?;
            };
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
                std::cmp::Ordering::Equal => {
                    return Ok(Some(middle_value));
                }
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
        key_range: (Key, Key),
        num_entries: Size,
        mut buffer_pool: Option<&mut BufferPool>,
    ) -> io::Result<Vec<(Key, Value)>> {
        let (key1, key2) = key_range;
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
                curr_page = get_sst_page(db_name, level, run, middle_page_index, bp)?;
            };
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
            get_sst_page(
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
            get_sst_page(
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
            let page = get_sst_page(db_name, level, run, i, buffer_pool.as_deref_mut())?;
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

    ///Compact all SST runs in a level into a single SST run and update entry_counts to reflect that
    fn compact(
        &self,
        db_name: &str,
        level: Level,
        entry_counts: &mut Vec<Size>,
        discard_tombstones: bool,
        mut buffer_pool: Option<&mut BufferPool>,
    ) -> io::Result<()> {
        let num_runs = entry_counts.len(); //Number of SST runs

        if num_runs < 2 {
            return Ok(()); //nothing to compact
        }

        let page_counts: Vec<Size> = entry_counts
            .iter()
            .map(|num_entries| num_pages(*num_entries))
            .collect(); //Number of pages in each SST run

        let get_entries =
            |run, page_index| get_entries_at_page(db_name, level, run, page_index, None);

        //Input buffer along with metadata for each run, index within buffer, page_index of buffer entries, boolean to indicate whether or not any more values exist in SST
        type InputBufferData = (Vec<(Key, Value)>, usize, Page);
        let mut input_buffers: Vec<InputBufferData> = (0..num_runs)
            .map(|run| {
                let entries = match get_entries(run, 0) {
                    Err(_) => vec![],
                    Ok(entries) => entries,
                };
                (entries, 0, 0)
            })
            .collect();

        //Pull entry from buffer, if at end, then fill buffer with next page of entries
        //Return None if no more entires to pull in run's SST
        let pull_entry = |input_buffers: &mut Vec<InputBufferData>,
                          run: Run|
         -> io::Result<Option<(Key, Value)>> {
            let (entries, curr_index, curr_page) = &mut input_buffers[run];

            if *curr_index >= entries.len() {
                //check if we are at the end of this input buffer
                if *curr_page + 1 < page_counts[run] {
                    //if there is another page of entries to pull into our buffer, do it
                    *curr_page += 1;
                    *entries = get_entries(run, *curr_page)?;

                    *curr_index = 0;
                } else {
                    //no more entries in the SST to pull into buffer
                    return Ok(None);
                }
            }

            let entry = entries[*curr_index];
            *curr_index += 1;

            Ok(Some(entry))
        };

        type BufferHeap = BinaryHeap<(Key, Run, Value)>;
        let mut heap = BufferHeap::new(); //to ensure we write the smallest value in our buffers

        let mut output_buffer: Vec<(Key, Value)> = Vec::with_capacity(num_entries_per_page());
        let temp_file_name = filename::sst_compaction_path(db_name, level);
        let mut output = direct_io::create(&temp_file_name)?;
        let mut entries_written: Size = 0;

        let heap_insert = |heap: &mut BufferHeap, key: Key, value, run| {
            //NOTE: tuple elements are sorted lexicographically in the heap by default, this fact is very
            //      important for the implementation. In case of a tie, the youngest (highest number) SST
            //      run will have its value used
            heap.push((-key, run, value)); //Negative key to use as min_heap instead
        };
        let heap_extract = |heap: &mut BufferHeap| {
            if let Some((negative_key, run, value_option)) = heap.pop() {
                let key = -negative_key;
                return Some(((key, value_option), run));
            }
            None
        };

        //take item from heap and replace it with another element in its run (if there is any)
        let heap_swap_extract = |heap: &mut BufferHeap,
                                 input_buffers: &mut Vec<InputBufferData>|
         -> io::Result<Option<(Key, Value)>> {
            if let Some((entry, run)) = heap_extract(heap) {
                if let Some(replacement_entry) = pull_entry(input_buffers, run)? {
                    let (key, value) = replacement_entry;
                    heap_insert(heap, key, value, run);
                }
                return Ok(Some(entry));
            }
            Ok(None)
        };
        let mut flush_output_buffer = |output_buffer: &mut Vec<(Key, Value)>| -> io::Result<()> {
            if output_buffer.is_empty() {
                return Ok(());
            }
            serde_entry::serialize_into_no_resize(&mut output, output_buffer)?;
            entries_written += output_buffer.len();
            output_buffer.clear();
            Ok(())
        };
        let mut output_buffer_insert =
            |output_buffer: &mut Vec<(Key, Value)>, entry| -> io::Result<()> {
                output_buffer.push(entry);
                //if we filled up our buffer, flush buffer to compaction file
                if output_buffer.len() >= num_entries_per_page() {
                    flush_output_buffer(output_buffer)?;
                }
                Ok(())
            };

        //put one entry from each buffer, NOTE: higher run number is younger
        for run in (0..entry_counts.len()).rev() {
            let entry_option = pull_entry(&mut input_buffers, run)?;
            if entry_option.is_none() {
                continue;
            }
            let (key, value) = entry_option.unwrap(); //NOTE: can unwrap safely because of earlier check
            heap_insert(&mut heap, key, value, run);
        }

        let mut recent_key: Option<Key> = None;

        //put entries into output buffer until there are no more entries to pull from any buffer
        loop {
            let entry_option = heap_swap_extract(&mut heap, &mut input_buffers)?;

            if let Some((key, value)) = entry_option {
                if recent_key.is_some_and(|recent| recent == key) {
                    continue; //we already have inserted the value (or it we discarded its tombstone already)
                }
                if !discard_tombstones || value != Database::TOMBSTONE_VALUE {
                    output_buffer_insert(&mut output_buffer, (key, value))?;
                }
                recent_key = Some(key);
            } else {
                //nothing in heap, should be done
                break;
            }
        }
        //flush remaining elements
        flush_output_buffer(&mut output_buffer)?;
        output.set_len((entries_written * ENTRY_SIZE) as u64)?; //set correct file size

        //delete other runs
        for path_result in fs::read_dir(filename::lsm_level_directory(db_name, level))? {
            let path = path_result?.path();
            if let Some(file_extension) = path.extension() {
                if file_extension == filename::SST_FILE_EXTENSION {
                    // fs::remove_file(path)?;
                    file_interface::remove_file(
                        path.as_os_str().to_str().unwrap(),
                        buffer_pool.as_deref_mut(),
                    )? //TODO: change function to use path directly
                }
            }
        }

        //if we write no entries, then we should delete the compaction file instead and set entry counts to be empty (to represent the fact there are no more SST runs on this level)
        if entries_written == 0 {
            entry_counts.clear();
            return fs::remove_file(&temp_file_name);
        }

        //By this point we know our compaction file has entries, so we rename it to an actual SST file name

        fs::rename(&temp_file_name, filename::sst_path(db_name, level, 0))?;
        *entry_counts = vec![entries_written];

        Ok(())
    }
}

mod tests {
    use super::*;

    #[allow(dead_code)]
    fn setup_and_test_and_cleaup(db_name: &str, level: Level, mut test: Box<dyn FnMut()>) {
        let dir = &filename::lsm_level_directory(db_name, level);
        if std::path::Path::new(dir).exists() {
            std::fs::remove_dir_all(dir).unwrap(); //remove previous directory if panicked during tests and didn't clean up
        }
        std::fs::create_dir_all(dir).unwrap();

        test();

        std::fs::remove_dir_all(db_name).unwrap();
    }

    #[test]
    fn test_small_compaction() {
        let db_name = "array_sst_compaction_small";
        const LEVEL: Level = 0;
        let test = || {
            let sst = Sst {};
            // let iter = 0..num_entries_per_page() as Key;
            let entries0: Vec<(Key, Value)> = vec![(0, 0), (1, 0)];
            let entries1: Vec<(Key, Value)> = vec![(0, 1), (1, 1)];
            let expected_result: Vec<(Key, Value)> = entries1.clone();

            let mut entry_counts = vec![entries0.len(), entries1.len()];
            sst.write(db_name, LEVEL, 0, &entries0).unwrap();
            sst.write(db_name, LEVEL, 1, &entries1).unwrap();
            sst.compact(db_name, LEVEL, &mut entry_counts, false, None)
                .unwrap();

            assert_eq!(entry_counts, vec![expected_result.len()]);

            let compaction_entries = sst.read(db_name, LEVEL, 0).unwrap();
            assert_eq!(compaction_entries, expected_result);
        };
        setup_and_test_and_cleaup(db_name, LEVEL, Box::new(test));
    }

    #[test]
    fn test_small_compaction2() {
        let db_name = "array_sst_compaction_small2";
        const LEVEL: Level = 0;
        let test = || {
            let sst = Sst {};
            // let iter = 0..num_entries_per_page() as Key;
            let entries0: Vec<(Key, Value)> = vec![(0, 0), (2, 0)];
            let entries1: Vec<(Key, Value)> = vec![(1, 1), (2, 1)];
            let expected_result: Vec<(Key, Value)> = vec![(0, 0), (1, 1), (2, 1)];

            let mut entry_counts = vec![entries0.len(), entries1.len()];
            sst.write(db_name, LEVEL, 0, &entries0).unwrap();
            sst.write(db_name, LEVEL, 1, &entries1).unwrap();
            sst.compact(db_name, LEVEL, &mut entry_counts, false, None)
                .unwrap();

            assert_eq!(entry_counts, vec![expected_result.len()]);

            let compaction_entries = sst.read(db_name, LEVEL, 0).unwrap();
            assert_eq!(compaction_entries, expected_result);
        };
        setup_and_test_and_cleaup(db_name, LEVEL, Box::new(test));
    }

    #[test]
    fn test_interspersed_compaction() {
        let db_name = "array_sst_interspersed_compaction";
        const LEVEL: Level = 0;
        let test = || {
            let sst = Sst {};
            let iter = 0..num_entries_per_page() as Key;
            let mut entries0: Vec<(Key, Value)> = iter
                .to_owned()
                .skip(0)
                .step_by(2)
                .map(|key| (key, -key))
                .collect();
            let mut entries1: Vec<(Key, Value)> = iter
                .to_owned()
                .skip(1)
                .step_by(2)
                .map(|key| (key, key))
                .collect();
            let mut expected_result: Vec<(Key, Value)> = iter
                .to_owned()
                .map(|key| {
                    if key % 2 == 0 {
                        (key, -key)
                    } else {
                        (key, key)
                    }
                })
                .collect();

            //deleted value, but the newer SST has copy, so it should be in the result
            let key = 99999;
            entries0.push((key, Database::TOMBSTONE_VALUE));
            entries1.push((key, 0));
            expected_result.push((key, 0));

            //deleted value in newer SST, expected result should not have it if we want to discard tombstones
            //but if we don't discard tombstones it should in the compaction result
            let key = 199999;
            entries0.push((key, 100));
            entries1.push((key, Database::TOMBSTONE_VALUE));
            let no_tomstones_result = expected_result.clone();
            expected_result.push((key, Database::TOMBSTONE_VALUE));

            //TEST 1: test including tombstones
            let mut entry_counts = vec![entries0.len(), entries1.len()];
            sst.write(db_name, LEVEL, 0, &entries0).unwrap();
            sst.write(db_name, LEVEL, 1, &entries1).unwrap();
            sst.compact(db_name, LEVEL, &mut entry_counts, false, None)
                .unwrap();

            assert_eq!(entry_counts, vec![expected_result.len()]);

            let compaction_entries = sst.read(db_name, LEVEL, 0).unwrap();
            assert_eq!(compaction_entries, expected_result);

            //TEST 2: test discarding tombstones
            let mut entry_counts = vec![entries0.len(), entries1.len()];
            sst.write(db_name, LEVEL, 0, &entries0).unwrap();
            sst.write(db_name, LEVEL, 1, &entries1).unwrap();
            sst.compact(db_name, LEVEL, &mut entry_counts, true, None)
                .unwrap();

            assert_eq!(entry_counts, vec![no_tomstones_result.len()]);

            let compaction_entries = sst.read(db_name, LEVEL, 0).unwrap();
            assert_eq!(compaction_entries, no_tomstones_result);
            //make sure there are no tombstones
            assert!(!compaction_entries
                .iter()
                .any(|(_, value)| { *value == Database::TOMBSTONE_VALUE }));
        };
        setup_and_test_and_cleaup(db_name, LEVEL, Box::new(test));
    }

    #[test]
    fn test_compaction_edge_cases() {
        let db_name = "array_sst_compaction_edge_cases";
        const LEVEL: Level = 0;
        let test = || {
            let sst = Sst {};
            let iter = 0..num_entries_per_page() as Key;
            let entries0: Vec<(Key, Value)> = iter
                .to_owned()
                .skip(0)
                .step_by(2)
                .map(|key| (key, -key))
                .collect();

            //EDGE case tests

            //EDGE CASE TEST 1: compacting a single sst with itself
            let mut entry_counts = vec![entries0.len()];
            sst.write(db_name, LEVEL, 0, &entries0).unwrap();
            sst.compact(db_name, LEVEL, &mut entry_counts, false, None)
                .unwrap();

            let compaction_entries = sst.read(db_name, LEVEL, 0).unwrap();
            assert_eq!(entry_counts, vec![entries0.len()]);
            assert_eq!(compaction_entries, entries0);

            //EDGE CASE TEST 2: compacting empty SST with a non empty one
            let mut entry_counts = vec![entries0.len(), 0];
            sst.write(db_name, LEVEL, 0, &entries0).unwrap();
            sst.write(db_name, LEVEL, 1, &[]).unwrap();
            sst.compact(db_name, LEVEL, &mut entry_counts, false, None)
                .unwrap();

            let compaction_entries = sst.read(db_name, LEVEL, 0).unwrap();
            assert_eq!(entry_counts, vec![entries0.len()]);
            assert_eq!(compaction_entries, entries0);

            //EDGE CASE TEST 3: compacting 2 empty SSTs
            let mut entry_counts = vec![0, 0];
            sst.write(db_name, LEVEL, 0, &[]).unwrap();
            sst.write(db_name, LEVEL, 1, &[]).unwrap();
            sst.compact(db_name, LEVEL, &mut entry_counts, false, None)
                .unwrap();

            assert_eq!(entry_counts, vec![]);
            assert!(sst.read(db_name, LEVEL, 0).is_err());

            //EDGE CASE TEST 3: compacting 1 SST filled with tombstones
            let entries0: Vec<(Key, Value)> = iter
                .to_owned()
                .skip(0)
                .step_by(2)
                .map(|key| (key, Database::TOMBSTONE_VALUE))
                .collect();
            let entries1: Vec<(Key, Value)> = iter
                .to_owned()
                .skip(1)
                .step_by(2)
                .map(|key| (key, Database::TOMBSTONE_VALUE))
                .collect();
            let expected_result: Vec<(Key, Value)> =
                iter.map(|key| (key, Database::TOMBSTONE_VALUE)).collect();

            //3.1: test with discard_tombstones disabled
            let mut entry_counts = vec![entries0.len(), entries1.len()];
            sst.write(db_name, LEVEL, 0, &entries0).unwrap();
            sst.write(db_name, LEVEL, 1, &entries1).unwrap();
            sst.compact(db_name, LEVEL, &mut entry_counts, false, None)
                .unwrap();

            assert_eq!(entry_counts, vec![expected_result.len()]);
            let compaction_entries = sst.read(db_name, LEVEL, 0).unwrap();
            assert_eq!(compaction_entries, expected_result);

            //3.2 test with discard_tombstones disabled
            let mut entry_counts = vec![entries0.len(), entries1.len()];
            sst.write(db_name, LEVEL, 0, &entries0).unwrap();
            sst.write(db_name, LEVEL, 1, &entries1).unwrap();
            sst.compact(db_name, LEVEL, &mut entry_counts, true, None)
                .unwrap();

            assert_eq!(entry_counts, vec![]);
            assert!(sst.read(db_name, LEVEL, 0).is_err());
        };
        setup_and_test_and_cleaup(db_name, LEVEL, Box::new(test));
    }

    #[test]
    fn test_multi_compaction() {
        let db_name = "array_sst_compaction";
        const LEVEL: Level = 0;
        let test = || {
            let sst = Sst {};
            let entries0: Vec<(Key, Value)> = vec![(0, 0), (1, 0), (32, 0), (64, 0)];
            let entries1: Vec<(Key, Value)> = vec![(0, 1), (1, Database::TOMBSTONE_VALUE)];
            let entries2: Vec<(Key, Value)> =
                vec![(1, 2), (16, Database::TOMBSTONE_VALUE), (32, 2)];
            let expected_result: Vec<(Key, Value)> = vec![
                (0, 1),
                (1, 2),
                (16, Database::TOMBSTONE_VALUE),
                (32, 2),
                (64, 0),
            ];
            let mut entry_counts = vec![entries0.len(), entries1.len(), entries2.len()];

            //TEST 1: without discarding tombstones
            sst.write(db_name, LEVEL, 0, &entries0).unwrap();
            sst.write(db_name, LEVEL, 1, &entries1).unwrap();
            sst.write(db_name, LEVEL, 2, &entries2).unwrap();

            sst.compact(db_name, LEVEL, &mut entry_counts, false, None)
                .unwrap();

            assert_eq!(entry_counts, vec![expected_result.len()]);

            let compaction_entries = sst.read(db_name, LEVEL, 0).unwrap();
            assert_eq!(compaction_entries, expected_result);

            //TEST 2: with discarding tombstones
            let expected_result: Vec<(Key, Value)> = vec![(0, 1), (1, 2), (32, 2), (64, 0)];
            let mut entry_counts = vec![entries0.len(), entries1.len(), entries2.len()];

            sst.write(db_name, LEVEL, 0, &entries0).unwrap();
            sst.write(db_name, LEVEL, 1, &entries1).unwrap();
            sst.write(db_name, LEVEL, 2, &entries2).unwrap();

            sst.compact(db_name, LEVEL, &mut entry_counts, true, None)
                .unwrap();

            assert_eq!(entry_counts, vec![expected_result.len()]);

            let compaction_entries = sst.read(db_name, LEVEL, 0).unwrap();
            assert_eq!(compaction_entries, expected_result);
        };
        setup_and_test_and_cleaup(db_name, LEVEL, Box::new(test));
    }
}
