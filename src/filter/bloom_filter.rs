use crate::{
    filter::bloom_util::{bitmap_len, BYTE_SIZE},
    util::{
        hash::BloomHasher,
        types::{Entry, Key},
    },
};

use super::bloom_util::{bit_index, num_hash_functions};

pub struct BloomFilter {
    pub bitmap: Vec<u8>,
    num_hash_functions: usize,
}

#[allow(dead_code)]
impl BloomFilter {
    pub fn new(num_entries: usize, bits_per_entry: usize) -> Self {
        Self {
            num_hash_functions: num_hash_functions(bits_per_entry),
            bitmap: Self::create_bitmap(num_entries, bits_per_entry),
        }
    }
    pub fn from_entries(entries: &Vec<Entry>, bits_per_entry: usize) -> Self {
        let mut filter = Self {
            num_hash_functions: num_hash_functions(bits_per_entry),
            bitmap: Self::create_bitmap(entries.len(), bits_per_entry),
        };
        filter.insert_entries(entries);
        filter
    }

    ///Add an element to the Bloom Filter
    pub fn insert(&mut self, key: Key) {
        for i in 0..self.num_hash_functions as u64 {
            let (byte_index, bit_index) = self.hash_to_index(key, i);
            self.bitmap[byte_index] |= 1 << bit_index;
        }
    }

    pub fn insert_entries(&mut self, entries: &Vec<Entry>) {
        for (key, ..) in entries {
            self.insert(*key);
        }
    }

    ///Check if an element may be in the Bloom Filter
    pub fn contains(&self, key: Key) -> bool {
        for i in 0..self.num_hash_functions as u64 {
            let (byte_index, bit_index) = self.hash_to_index(key, i);
            if (self.bitmap[byte_index] & (1 << bit_index)) == 0 {
                return false;
            }
        }
        true
    }

    ///Reset the Bloom Filter, for testing
    fn reset(&mut self) {
        self.bitmap = vec![0; self.bitmap.len()];
    }

    fn create_bitmap(num_entries: usize, bits_per_entry: usize) -> Vec<u8> {
        let num_bytes = bitmap_len(num_entries, bits_per_entry); //round up to nearest number of bytes
        vec![0; num_bytes]
    }

    fn hash_to_index(&self, key: Key, seed: u64) -> (usize, usize) {
        let index = BloomHasher::hash_key_to_index(key, seed, self.bitmap.len() * BYTE_SIZE);
        bit_index(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_simple() {
        // Create a new Bloom filter
        let mut bloom_filter = BloomFilter::new(10, 5);

        // Insert some elements into the Bloom filter
        bloom_filter.insert(1);
        bloom_filter.insert(2);
        bloom_filter.insert(3);

        // Check positive
        assert!(bloom_filter.contains(1));
        assert!(bloom_filter.contains(2));
        assert!(bloom_filter.contains(3));

        // Check negative
        assert!(!bloom_filter.contains(4));
    }

    #[test]
    fn test_bloom_filter_false_positive() {
        // Create a new Bloom filter
        let mut bloom_filter = BloomFilter::new(1, 1);
        // Assuming no collisions here, maybe noy true
        bloom_filter.insert(1);
        bloom_filter.insert(2);
        bloom_filter.insert(3);
        bloom_filter.insert(4);
        bloom_filter.insert(5);
        bloom_filter.insert(6);
        bloom_filter.insert(7);
        bloom_filter.insert(8);

        // Check false positive
        assert!(bloom_filter.contains(9));
    }

    #[test]
    fn test_bloom_filter_reset() {
        // Create a new Bloom filter
        let mut bloom_filter = BloomFilter::new(10, 5);

        bloom_filter.insert(1);
        bloom_filter.reset();

        // Check negative
        assert!(!bloom_filter.contains(1));
    }

    #[test]
    fn test_from_entries() {
        let entries = vec![(0, 0), (1, 1), (32, 32)];
        let bloom_filter = BloomFilter::from_entries(&entries, 5);

        assert!(bloom_filter.contains(0));
        assert!(bloom_filter.contains(32));

        // Check negative
        assert!(!bloom_filter.contains(2));
    }
}
