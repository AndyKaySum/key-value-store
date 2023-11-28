use crate::util::bloom_hasher::BloomHasher;
use std::cmp;

trait BloomFilterTrait {
    fn set<T: AsRef<[u8]>>(&mut self, item: T);
    fn has<T: AsRef<[u8]>>(&self, item: T) -> bool;
    fn reset(&mut self);
}

struct BloomFilter {
    bit_vec: Vec<u8>,
    size: usize,
    num_hash_functions: usize,
}

impl BloomFilter {
    pub fn new(size: usize, num_items: usize) -> BloomFilter {
        let hash_functions = Self::optimal_hash_num(size as u64, num_items) as usize;
        let byte_size = (size + 7) / 8;

        BloomFilter {
            bit_vec: vec![0; byte_size],
            size,
            num_hash_functions: hash_functions,
        }
    }

    // create a new bloom filter from a bit vector/sst
    pub fn from_bit_vec(bit_vec: Vec<u8>) -> BloomFilter {
        let size = bit_vec.len();
        BloomFilter {
            bit_vec,
            size,
            num_hash_functions: 0,
        }
    }

    fn optimal_hash_num(bitmap_size: u64, items_count: usize) -> u32 {
        let m = bitmap_size as f64;
        let n = items_count as f64;

        // Calculate the optimal number of hash functions (m/n) * ln(2)
        let hash_num = (m / n * std::f64::consts::LN_2).ceil() as u32;

        // Fix lower bound 1
        cmp::max(hash_num, 1)
    }

    // Helper to get the byte and bit index
    fn get_byte_and_bit_index(&self, hash_value: u64) -> (usize, u8) {
        let byte_index = (hash_value as usize % self.size) / 8;
        let bit_index = (hash_value as u8) % 8;
        (byte_index, bit_index)
    }
}

impl BloomFilterTrait for BloomFilter {
    // Add an element to the Bloom Filter
    fn set<T: AsRef<[u8]>>(&mut self, item: T) {
        for i in 0..self.num_hash_functions as u64 {
            let hash_value = BloomHasher::hash(item.as_ref(), i as u64);
            let (byte_index, bit_index) = self.get_byte_and_bit_index(hash_value);
            self.bit_vec[byte_index] |= 1 << bit_index;
        }
    }

    // Check if an element may be in the Bloom Filter
    fn has<T: AsRef<[u8]>>(&self, item: T) -> bool {
        for i in 0..self.num_hash_functions as u64 {
            let hash_value = BloomHasher::hash(item.as_ref(), i as u64);
            let (byte_index, bit_index) = self.get_byte_and_bit_index(hash_value);
            if (self.bit_vec[byte_index] & (1 << bit_index)) == 0 {
                return false;
            }
        }
        true
    }

    // Reset the Bloom Filter
    fn reset(&mut self) {
        let byte_size = (self.size + 7) / 8;
        self.bit_vec = vec![0; byte_size];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_simple() {
        // Create a new Bloom filter with a capacity of 100 elements and a false positive rate of 0.1%
        let mut bloom_filter = BloomFilter::new_for_fpr(50, 0.001);

        // Insert some elements into the Bloom filter
        bloom_filter.set("one");
        bloom_filter.set("two");
        bloom_filter.set("three");

        // Check positive
        assert!(bloom_filter.has("one"));
        assert!(bloom_filter.has("two"));
        assert!(bloom_filter.has("three"));

        // Check negative
        assert!(!bloom_filter.has("four"));
    }
}
