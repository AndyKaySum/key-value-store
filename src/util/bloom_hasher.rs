use xxhash_rust::xxh3::xxh32;

pub struct BloomHasher {
    seed: u64,
}

impl BloomHasher {
    // hash function static method
    pub fn hash(&self, bytes: &[u8]) -> u64 {
        xxh32::hash_with_seed(bytes, self.seed)
    }
}