use xxhash_rust::xxh3::xxh3_64_with_seed;

pub struct BloomHasher;

impl BloomHasher {
    // hash function static method
    pub fn hash(bytes: &[u8], seed: u64) -> u64 {
        xxh3_64_with_seed(bytes, seed)
    }
}
