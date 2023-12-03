use super::types::Key;
use xxhash_rust::xxh3::xxh3_64_with_seed;

pub fn hash(bytes: &[u8], seed: u64) -> u64 {
    xxh3_64_with_seed(bytes, seed)
}
pub fn hash_to_index<T: AsRef<[u8]>>(item: T, seed: u64, arr_len: usize) -> usize {
    hash(item.as_ref(), seed) as usize % arr_len
}
pub fn hash_key(key: Key, seed: u64) -> u64 {
    hash(&key.to_le_bytes(), seed)
}
pub fn hash_key_to_index(key: Key, seed: u64, arr_len: usize) -> usize {
    hash_key(key, seed) as usize % arr_len
}

#[derive(Debug, Default)]
pub struct FastHasher {
    state: u64,
    seed: u64,
}

impl FastHasher {
    pub fn new(seed: u64) -> Self {
        Self { state: 0, seed }
    }
}

impl std::hash::Hasher for FastHasher {
    fn write(&mut self, bytes: &[u8]) {
        self.state = hash(bytes, self.seed);
    }
    fn finish(&self) -> u64 {
        self.state
    }
}
