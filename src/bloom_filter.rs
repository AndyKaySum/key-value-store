use crate::util::bloom_hasher::BloomHasher;
use std::cmp;

trait BloomFilterTrait {
    fn set<T: AsRef<[u8]>>(&mut self, item: T);
    fn has<T: AsRef<[u8]>>(&self, item: T) -> bool;
    fn reset(&mut self);
}

struct BloomFilter {
    bit_vec: Vec<bool>,
    size: usize,
    num_hash_functions: usize,
}

impl BloomFilter {
    pub fn new(size: usize, num_items: usize) -> BloomFilter {
        let hash_functions = Self::optimal_hash_num(size as u64, num_items) as usize;
        BloomFilter {
            bit_vec: vec![false; size],
            size,
            num_hash_functions: hash_functions,
        }
    }

    // create a new bloom filter from a bit vector/sst
    pub fn from_bit_vec(bit_vec: Vec<bool>) -> BloomFilter {
        let size = bit_vec.len();
        BloomFilter {
            bit_vec,
            size,
            num_hash_functions: 0,
        }
    }

    // Create a new Bloom Filter from sst page
    // pub fn from_sst_page(page: Vec<u8>) -> BloomFilter {
    //     // TODO
    // }

    // Create a new Bloom Filter with a specified false positive rate
    pub fn new_for_fpr(items_count: usize, fp_p: f64) -> Self {
        let bitmap_size = Self::compute_bitmap_size(items_count, fp_p);
        BloomFilter::new(bitmap_size, items_count)
    }

    fn optimal_hash_num(bitmap_size: u64, items_count: usize) -> u32 {
        let m = bitmap_size as f64;
        let n = items_count as f64;

        // Calculate the optimal number of hash functions k = (m/n) * ln(2)
        let hash_num = (m / n * std::f64::consts::LN_2).ceil() as u32;

        // Fix lower bound 1
        cmp::max(hash_num, 1)
    }

    // Should we be using this hash function?
    // fn hash<T: AsRef<[u8]>>(&self, item: T, seed: u64) -> usize {
    //     let hash_value = Xxh3::hash_with_seed(item, seed);
    //     (hash_value as usize) % self.size
    // }

    // Compute a recommended bitmap size for num_items items
    // and a fpr rate of false positives.
    pub fn compute_bitmap_size(num_items: usize, fpr: f64) -> usize {
        assert!(num_items > 0);
        assert!(fpr > 0.0 && fpr < 1.0);
        let log2 = std::f64::consts::LN_2;
        let log2_2 = log2 * log2;
        ((num_items as f64) * f64::ln(fpr) / (-8.0 * log2_2)).ceil() as usize
    }
}

impl BloomFilterTrait for BloomFilter {
    // Add an element to the Bloom Filter
    fn set<T: AsRef<[u8]>>(&mut self, item: T) {
        for i in 0..self.num_hash_functions as u64 {
            let hash_value = BloomHasher::hash(item.as_ref(), i as u64);
            let index = hash_value as usize % self.size;
            self.bit_vec[index] = true;
        }
    }

    // Check if an element may be in the Bloom Filter
    fn has<T: AsRef<[u8]>>(&self, item: T) -> bool {
        for i in 0..self.num_hash_functions as u64 {
            let hash_value = BloomHasher::hash(item.as_ref(), i as u64);
            let index = hash_value as usize % self.size;
            if !self.bit_vec[index] {
                return false;
            }
        }
        true
    }

    // Reset the Bloom Filter
    fn reset(&mut self) {
        self.bit_vec = vec![false; self.size];
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
