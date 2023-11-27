use std::io; //can swap "serde_entry" with "bincode" to swap functions

use crate::sst::btree_util::num_leaves;
use crate::sst::sst_util::get_sst_page;
use crate::util::algorithm::{
    binary_search_entries, binary_search_leftmost, binary_search_rightmost,
};
use crate::{
    buffer_pool::BufferPool,
    file_io::{direct_io, serde_btree, serde_entry},
    sst::btree_util::num_nodes,
    util::{
        btree_info::fanout,
        filename,
        system_info::{num_entries_per_page, ENTRY_SIZE},
        types::{Key, Level, Run, Size, Value},
    },
};

use super::btree_util::{
    btree_navigate, get_last_in_each_chunk, has_inner_nodes, seek_node, tree_depth,
};
use super::{array_sst, SortedStringTable};

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
        entries: &[(Key, Value)], //assumes this is sorted properly
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
    fn read(&self, db_name: &str, level: Level, run: Run) -> io::Result<Vec<(Key, Value)>> {
        array_sst::Sst.read(db_name, level, run)
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

        let page = get_sst_page(db_name, level, run, page_index, buffer_pool)?;

        let entries = serde_entry::deserialize(&page).unwrap_or_else(|_| panic!("Failed to deserialize page during final step of get operation, name: {db_name}, level: {level}, run: {run}, page index: {page_index}, num_entries: {num_entries}"));

        Ok(binary_search_entries(&entries, key))
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

        let lowerbound_page = get_sst_page(
            db_name,
            level,
            run,
            lowerbound_page_index,
            buffer_pool.as_deref_mut(),
        )?;
        let lowerbound_page_entries = serde_entry::deserialize(&lowerbound_page)
            .unwrap_or_else(|_| panic!("Failed to deserialize lowerbound page, name: {db_name}, level: {level}, run: {run}, page index: {lowerbound_page_index} num_entries: {num_entries}"));
        let lowerbound_keys: Vec<Key> = lowerbound_page_entries
            .iter()
            .map(|(key, _)| *key)
            .collect(); //TODO: write a specific binary search for entries instead of creating mapped array
        let lowerbound_within_page_index = binary_search_leftmost(&lowerbound_keys, key1);

        let upperbound_page_index = btree_navigate(
            db_name,
            level,
            run,
            key2,
            num_entries,
            buffer_pool.as_deref_mut(),
        )?;

        let (upperbound_keys, upperbound_page_entries) = if upperbound_page_index
            == lowerbound_page_index
        {
            (lowerbound_keys, lowerbound_page_entries.to_owned()) //TODO: avoid copying this second item
        } else {
            let upperbound_page = get_sst_page(
                db_name,
                level,
                run,
                upperbound_page_index,
                buffer_pool.as_deref_mut(),
            )?;
            let upperbound_page_entries = serde_entry::deserialize(&upperbound_page)
                .unwrap_or_else(|_| panic!("Failed to deserialize upperbound page, name: {db_name}, level: {level}, run: {run}, page index: {upperbound_page_index} num_entries: {num_entries}"));
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

        let mut results: Vec<(Key, Value)> = Vec::new();

        //Add lowerbound entries if there are any (lowerbound_within_page_index is inside its entries array)
        //EDGE CASE: lowerbound index is "after" last element, that means our lowerbound entry is contained
        //           in the first index of the next page (if it is there)
        if lowerbound_within_page_index < lowerbound_page_entries.len() {
            results.extend_from_slice(&lowerbound_page_entries[lowerbound_within_page_index..]);
        }

        for i in (lowerbound_page_index + 1)..upperbound_page_index {
            let page = get_sst_page(db_name, level, run, i, buffer_pool.as_deref_mut())?;
            let page_entries = &serde_entry::deserialize(&page).unwrap_or_else(|_| panic!("Unable to deserialize page during scan, level: {level}, run: {run} page_index: {i}"));

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
        let byte_count = direct_io::open_read(&filename::sst_path(db_name, level, run))?
            .metadata()?
            .len();
        Ok(byte_count as Size / ENTRY_SIZE)
    }

    fn compact(&self, _db_name: &str, _level: Level, _entry_counts: &[Size]) -> Size {
        unimplemented!()
    }
}
