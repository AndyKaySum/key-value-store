use std::{fs, io};

use crate::file_io::{file_interface, serde_entry};
use crate::sst::btree_util::num_leaves;
use crate::util::algorithm::{
    binary_search_entries, binary_search_leftmost, binary_search_rightmost,
};
use crate::util::types::{Depth, Node};
use crate::{
    buffer_pool::BufferPool,
    file_io::{direct_io, serde_btree},
    sst::btree_util::num_nodes,
    util::{
        btree_info::fanout,
        filename,
        system_info::num_entries_per_page,
        types::{Entry, Key, Level, Run, Size, Value},
    },
};

use super::btree_util::{
    btree_navigate, get_last_in_each_chunk, has_inner_nodes, seek_node, tree_depth,
};
use super::sst_util::{get_entries_at_page, get_sst_page, num_pages};
use super::{array_sst, SortedStringTable};

type DelimeterBuffer = Vec<(Vec<Key>, Node)>; //Type alias for datastructure used to recursively build inner B-tree nodes from an SST

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
        entries: &[Entry], //assumes this is sorted properly
    ) -> io::Result<()> {
        //step 1: create directory for level if needed and write sorted entries into SST file
        array_sst::Sst.write(db_name, level, run, entries)?;

        let num_entries = entries.len();

        //step 2: write file for inner nodes, if needed
        if has_inner_nodes(num_entries) {
            return Ok(()); //we only have enough entries for 1 node, that means it is the "root"
        }

        let path = filename::sst_btree_path(db_name, level, run);
        let mut file = direct_io::create(&path)?; //NOTE: the directory should exist by this point, so no checks needed (created in array_sst::write)

        //get largest entry in each SST page (last value in each)
        let entry_keys: Vec<Key> = entries.iter().map(|(key, _)| *key).collect();
        let mut delimeters: Vec<Key> = get_last_in_each_chunk(&entry_keys, num_entries_per_page());
        assert_eq!(
            num_leaves(num_entries),
            delimeters.len(),
            "Miscalculated number of leaves"
        );
        let node_chunk_size = fanout();

        //build parent nodes all the way up to root
        for depth in (0..tree_depth(num_entries)).rev() {
            let num_nodes = num_nodes(depth, num_entries);
            let delimeters_per_node = delimeters.chunks(node_chunk_size); //each chunk corresponds to the values in each node on this level
            assert_eq!(delimeters_per_node.len(), num_nodes, "Calculated number of nodes on level {level} differs from number of delimeter chunks allocated to this level, chunk sizes: {:?}", delimeters_per_node.map(|delimeter_chunk| delimeter_chunk.len()).collect::<Vec<usize>>()); //if this breaks one of these is wrong

            for (node, node_elements) in delimeters_per_node.enumerate() {
                seek_node(&mut file, depth, node, num_entries)?;
                serde_btree::serialize_into(&mut file, node_elements)?;
            }

            //get largest delimeter in subtrees
            delimeters = get_last_in_each_chunk(&delimeters, fanout());
        }

        Ok(())
    }

    ///Deserializes entire SST to entry vec
    fn read(&self, db_name: &str, level: Level, run: Run) -> io::Result<Vec<Entry>> {
        array_sst::Sst.read(db_name, level, run)
    }

    fn binary_search_get(
        &self,
        db_name: &str,
        level: Level,
        run: Run,
        key: Key,
        num_entries: Size,
        buffer_pool: Option<&mut BufferPool>,
    ) -> io::Result<Option<Value>> {
        array_sst::Sst.binary_search_get(db_name, level, run, key, num_entries, buffer_pool)
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
        if has_inner_nodes(num_entries) {
            //there is no btree file, only entries
            return array_sst::Sst.get(db_name, level, run, key, num_entries, buffer_pool);
        }

        //get SST page that should contain the entry we want, using inner node navigation
        let page_index = btree_navigate(
            db_name,
            level,
            run,
            key,
            num_entries,
            buffer_pool.as_deref_mut(),
        )?; //next_node;

        let entries = get_entries_at_page(db_name, level, run, page_index, buffer_pool)?;

        Ok(binary_search_entries(&entries, key))
    }

    fn binary_search_scan(
        &self,
        db_name: &str,
        level: Level,
        run: Run,
        key_range: (Key, Key),
        num_entries: Size,
        buffer_pool: Option<&mut BufferPool>,
    ) -> io::Result<Vec<Entry>> {
        array_sst::Sst.binary_search_scan(db_name, level, run, key_range, num_entries, buffer_pool)
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
    ) -> io::Result<Vec<Entry>> {
        if num_entries <= fanout() {
            //there is no btree file, only entries
            return array_sst::Sst.scan(db_name, level, run, key_range, num_entries, buffer_pool);
        }

        let (key1, key2) = key_range;

        let lowerbound_page_index = btree_navigate(
            db_name,
            level,
            run,
            key1,
            num_entries,
            buffer_pool.as_deref_mut(),
        )?;

        let upperbound_page_index = btree_navigate(
            db_name,
            level,
            run,
            key2,
            num_entries,
            buffer_pool.as_deref_mut(),
        )?;

        let mut get_entries = |page_index| {
            get_entries_at_page(db_name, level, run, page_index, buffer_pool.as_deref_mut())
        }; //for readability: reduce duplicate args

        let lowerbound_page_entries = get_entries(lowerbound_page_index)?;
        let lowerbound_keys: Vec<Key> = lowerbound_page_entries
            .iter()
            .map(|(key, _)| *key)
            .collect(); //TODO: write a specific binary search for entries instead of creating mapped array
        let lowerbound_within_page_index = binary_search_leftmost(&lowerbound_keys, key1);

        let (upperbound_keys, upperbound_page_entries) =
            if upperbound_page_index == lowerbound_page_index {
                (lowerbound_keys, lowerbound_page_entries.to_owned()) //TODO: avoid copying this second item
            } else {
                let upperbound_page_entries = get_entries(upperbound_page_index)?;
                let upperbound_keys: Vec<Key> = upperbound_page_entries
                    .iter()
                    .map(|(key, _)| *key)
                    .collect(); //TODO: write a specific binary search for entries instead of creating mapped array
                (upperbound_keys, upperbound_page_entries)
            };
        let upperbound_within_page_index = binary_search_rightmost(&upperbound_keys, key2);

        //EDGE CASE: lowerbound and upperbound are in the same page
        //then, we should already have the entries on this page
        if lowerbound_page_index == upperbound_page_index {
            if lowerbound_within_page_index as i64 > upperbound_within_page_index {
                return Ok(vec![]);
            }
            let scan_result = upperbound_page_entries
                [lowerbound_within_page_index..upperbound_within_page_index as usize + 1]
                .to_vec();
            return Ok(scan_result);
        }

        let mut results: Vec<Entry> = Vec::new();

        //Add lowerbound entries if there are any (lowerbound_within_page_index is inside its entries array)
        //EDGE CASE: lowerbound index is "after" last element, that means our lowerbound entry is contained
        //           in the first index of the next page (if it is there)
        if lowerbound_within_page_index < lowerbound_page_entries.len() {
            results.extend_from_slice(&lowerbound_page_entries[lowerbound_within_page_index..]);
        }

        for i in (lowerbound_page_index + 1)..upperbound_page_index {
            let page_entries = get_entries(i)?;

            results.extend(page_entries);
        }

        //Add upperbound entries if there are any (upperbound_within_page_index is inside its entries array)
        //EDGE CASE: upperbound index is "before" first element, that means our upperbound entry is contained
        //           in the last index of the page prior to this one (if it is there)
        if upperbound_within_page_index >= 0 {
            results.extend_from_slice(
                &upperbound_page_entries[..upperbound_within_page_index as usize + 1],
            );
        }

        Ok(results)
    }
    ///Gets the number of entries in an sst
    fn len(&self, db_name: &str, level: Level, run: Run) -> io::Result<Size> {
        array_sst::Sst.len(db_name, level, run)
    }

    ///Compact all SST runs in a level into a single SST run, build B-tree nodes (if applicable),
    /// and update entry_counts to reflect that
    fn compact(
        &self,
        db_name: &str,
        level: Level,
        entry_counts: &mut Vec<Size>,
        discard_tombstones: bool,
        mut buffer_pool: Option<&mut BufferPool>,
    ) -> io::Result<()> {
        if entry_counts.len() < 2 {
            return Ok(()); //Nothing to compact
        }

        //Step 1: compact file containing entries
        array_sst::Sst.compact(
            db_name,
            level,
            entry_counts,
            discard_tombstones,
            buffer_pool.as_deref_mut(),
        )?;

        //remove existing B-tree files
        for path_result in fs::read_dir(filename::lsm_level_directory(db_name, level))? {
            let path = path_result?.path();
            if let Some(file_extension) = path.extension() {
                if file_extension == filename::BTREE_FILE_EXTENSION {
                    // fs::remove_file(path)?;
                    file_interface::remove_file(
                        path.as_os_str().to_str().unwrap(),
                        buffer_pool.as_deref_mut(),
                    )? //TODO: change function to use path directly
                }
            }
        }

        let run = 0;
        //Step 2: if our new file takes up more than a page, build inner B-tree nodes
        if entry_counts.is_empty() || num_pages(entry_counts[run]) < 2 {
            return Ok(());
        }

        let num_entries = entry_counts[run];
        let num_pages = num_pages(num_entries);

        let get_key = |page_index, index_within_page| -> io::Result<Key> {
            let page = get_sst_page(db_name, level, run, page_index, None)?;
            let (key, ..) = serde_entry::deserialize_entry_within_page(&page, index_within_page).unwrap_or_else(|why| panic!("Failed to deserialize key at page: {page_index} index: {index_within_page}, reason: {why}"));
            Ok(key)
        };

        let mut delimeter_buffer: DelimeterBuffer = (0..tree_depth(num_entries))
            .map(|_depth| (Vec::with_capacity(fanout()), 0))
            .collect();

        let path = filename::sst_btree_path(db_name, level, run);
        let mut file = direct_io::create(&path)?;

        for page_index in 0..num_pages {
            //need to handle last page differently
            let is_last_page = page_index == num_pages - 1;
            let last_element_index = if is_last_page {
                (num_entries - 1) % num_entries_per_page()
            } else {
                num_entries_per_page() - 1
            };
            let delimeter = get_key(page_index, last_element_index)?;
            delimeter_buffer_insert(
                &mut file,
                &mut delimeter_buffer,
                tree_depth(num_entries) - 1,
                num_entries,
                delimeter,
                is_last_page,
            )?;
        }

        Ok(())
    }
}

///Recursively build inner B-tree nodes in a scalable way (only needs <tree depth> * fanout memory).
/// Requires inserting last value of every page in an SST (one scan)
fn delimeter_buffer_insert(
    mut file: &mut std::fs::File,
    buffer: &mut DelimeterBuffer,
    depth: Depth,
    num_entries: Size,
    key: Key,
    force_flush: bool,
) -> io::Result<()> {
    let (delimeters, curr_node) = &mut buffer[depth];

    delimeters.push(key);

    //when we reach enough delimeters to write a node (or if we want to force a write),
    // write all but the last (handled by serialize_into) and move the last value into the upper level,
    // where it will be used to write nodes at that level (when that level fills up)
    if delimeters.len() >= fanout() || force_flush {
        seek_node(file, depth, *curr_node, num_entries)?;
        serde_btree::serialize_into(&mut file, delimeters)?;

        //largest key is moved to a higher level node, where it is used as a delimeter there
        let largest_key = delimeters.last().unwrap().to_owned(); //NOTE: should be able to unwrap because of the length check earlier

        delimeters.clear(); //we no longer need these delimeters in our buffer
        *curr_node += 1;
        assert!(*curr_node <= num_nodes(depth, num_entries));

        if depth > 0 {
            delimeter_buffer_insert(
                file,
                buffer,
                depth - 1,
                num_entries,
                largest_key,
                force_flush,
            )?;
        }
    }
    Ok(())
}

mod tests {
    #[allow(unused_imports)]
    use super::*;
    #[allow(unused_imports)]
    use crate::util::testing::setup_and_test_and_cleaup;

    #[test]
    fn test_simple_compaction_btree_nodes() {
        //test if we properly build the inner nodes when compacting
        //we don't need to test if the entries are compacted properly since it's handled by array_sst
        let db_name = "btree_simple_sst_compaction";
        const LEVEL: Level = 0;
        let mut test = || {
            let btree_sst = Sst {};
            let num_entries_per_sst = fanout() * num_entries_per_page();
            let iter = 0..num_entries_per_sst as Key; //needs #fanout nodes + 1 root
            let entries0: Vec<Entry> = iter.to_owned().map(|key| (key, 0)).collect();
            let entries1: Vec<Entry> = iter.map(|key| (entries0.len() as Key + key, 1)).collect();

            let mut expected_result = vec![];
            expected_result.extend(entries0.to_owned());
            expected_result.extend(entries1.to_owned());

            btree_sst.write(db_name, LEVEL, 0, &entries0).unwrap();
            btree_sst.write(db_name, LEVEL, 1, &entries1).unwrap();

            let mut entry_counts = vec![entries0.len(), entries1.len()];
            btree_sst
                .compact(db_name, LEVEL, &mut entry_counts, false, None)
                .unwrap();

            let key_range = (entries0.first().unwrap().0, entries1.last().unwrap().0);
            let result = btree_sst
                .scan(db_name, LEVEL, 0, key_range, entry_counts[0], None)
                .unwrap();

            assert_eq!(result.len(), expected_result.len());
            assert_eq!(result, expected_result);
        };
        setup_and_test_and_cleaup(db_name, LEVEL, &mut test);
    }

    #[test]
    fn test_multi_compaction_btree_nodes() {
        //test if we properly build the inner nodes when compacting
        //we don't need to test if the entries are compacted properly since it's handled by array_sst
        let db_name = "btree_multi_sst_compaction";
        const LEVEL: Level = 0;
        let mut test = || {
            let btree_sst = Sst {};
            let num_entries_per_sst = fanout() * num_entries_per_page();
            let num_runs: Run = 5;

            let mut entries = Vec::<Vec<Entry>>::new();
            let mut entry_counts = Vec::<Size>::new();
            for run in 0..num_runs {
                let run_entries: Vec<Entry> = (0..num_entries_per_sst as Key)
                    .map(|key| (key + (run * num_entries_per_sst) as Key, run as Value))
                    .collect();
                entry_counts.push(run_entries.len());

                btree_sst.write(db_name, LEVEL, run, &run_entries).unwrap();

                entries.push(run_entries);
            }

            let expected_result = entries.iter().fold(vec![], |mut acc, entries| {
                acc.extend(entries.iter());
                acc
            });

            btree_sst
                .compact(db_name, LEVEL, &mut entry_counts, false, None)
                .unwrap();

            let key_range = (
                entries[0].first().unwrap().0,
                entries[num_runs - 1].last().unwrap().0,
            );
            let result = btree_sst
                .scan(db_name, LEVEL, 0, key_range, entry_counts[0], None)
                .unwrap();

            assert_eq!(result.len(), expected_result.len());
            assert_eq!(result, expected_result);
        };
        setup_and_test_and_cleaup(db_name, LEVEL, &mut test);
    }
}
