use crate::util::bloom_hasher::BloomHasher;

trait BloomFilterTrait {
    fn put<T: AsRef<[u8]>>(&mut self, item: T);
    fn has<T: AsRef<[u8]>>(&self, item: T) -> bool;
    fn reset(&mut self);
}

struct BloomFilter {
    bit_vec: Vec<bool>,
    size: usize,
    num_hash_functions: usize,
}

impl BloomFilterTrait for BloomFilter {
    // Create a new Bloom Filter
    pub fn new(size: usize, num_items: usize) -> BloomFilter {
        let hash_functions = Self::optimal_hash_num(size as u64, num_items);
        BloomFilter {
            bit_vec: vec![false; size],
            size,
            hash_functions,
        }
    }

    // create a new bloom filter from a bit vector/sst
    pub fn from_bit_vec(bit_vec: Vec<bool>) -> BloomFilter {
        BloomFilter {
            bit_vec,
            size: bit_vec.len(),
            num_hash_functions: 0,
        }
    }

    // Create a new Bloom Filter from sst page
    pub fn from_sst_page(page: Vec<u8>) -> BloomFilter {
        // TODO
    }

    // Create a new Bloom Filter with a specified false positive rate
    pub fn new_for_fpr(items_count: usize, fp_p: f64) -> Self {
        let bitmap_size = Self::compute_bitmap_size(items_count, fp_p);
        BloomFilter::new(bitmap_size, items_count)
    }

    fn optimal_hash_num(bitmap_size: u64, items_count: usize) -> u32 {
        let m = bitmap_size as f64;
        let n = items_count as f64;
    
        // Calculate the optimal number of hash functions k = (m/n) * ln(2)
        let hash_num = (m / n * f64::consts::LN_2).ceil() as u32;
    
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
        let log2 = f64::consts::LN_2;
        let log2_2 = log2 * log2;
        ((num_items as f64) * f64::ln(fpr) / (-8.0 * log2_2)).ceil() as usize
    }

    // Add an element to the Bloom Filter
    pub fn put<T: AsRef<[u8]>>(&mut self, item: T) {
        for i in 0..self.num_hash_functions as u64 {
            let index = BloomHasher.hash(&item, i);
            self.bit_vec[index] = true;
        }
    }

    // Check if an element might be in the Bloom Filter
    pub fn has<T: AsRef<[u8]>>(&self, item: T) -> bool {
        for i in 0..self.num_hash_functions as u64 {
            let index = BloomHasher.hash(&item, i);
            if !self.bit_vec[index] {
                return false;
            }
        }
        true
    }

    // Reset the Bloom Filter
    pub fn reset(&mut self) {
        self.bit_vec = vec![false; self.size];
    }
}