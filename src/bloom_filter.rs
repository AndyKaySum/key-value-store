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
    pub fn new(size: usize, num_entries: usize) -> BloomFilter {
        let hash_functions = Self::optimal_hash_num(size as u64, num_entries) as usize;
        let byte_size = (size + 7) / 8;

        BloomFilter {
            bit_vec: vec![0; byte_size],
            size,
            num_hash_functions: hash_functions,
        }
    }

    // create a new bloom filter with specific bits per entry and number of entries num_entries
    pub fn new_with_bits_per_entry(bits_per_entry: usize, num_entries: usize) -> BloomFilter {
        let size = bits_per_entry * num_entries;
        BloomFilter::new(size, num_entries)
    }

    // create a new bloom filter from a bit vector
    pub fn new_from_bit_vec(bit_vec: Vec<u8>) -> BloomFilter {
        let size = bit_vec.len();
        BloomFilter {
            bit_vec,
            size,
            num_hash_functions: 0,
        }
    }

    fn optimal_hash_num(bitmap_size: u64, num_entries: usize) -> u32 {
        let m = bitmap_size as f64;
        let n = num_entries as f64;

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
        // Create a new Bloom filter
        let mut bloom_filter = BloomFilter::new(10, 50);

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

    #[test]
    fn test_bloom_filter_false_positive() {
        // Create a new Bloom filter
        let mut bloom_filter = BloomFilter::new(1, 10);
        // Assuming no collisions here, maybe noy true
        bloom_filter.set("one");
        bloom_filter.set("two");
        bloom_filter.set("three");
        bloom_filter.set("four");
        bloom_filter.set("five");
        bloom_filter.set("six");
        bloom_filter.set("seven");
        bloom_filter.set("eight");
        
        // Check false positive
        assert!(bloom_filter.has("nine"));
    }

    #[test]
    fn test_bloom_filter_reset() {
        // Create a new Bloom filter
        let mut bloom_filter = BloomFilter::new(10, 50);

        bloom_filter.set("one");
        bloom_filter.reset();

        // Check negative
        assert!(!bloom_filter.has("one"));
    }
}
