use super::types::Key;

pub type FastHasher = twox_hash::XxHash;

#[derive(Debug)]
pub struct BloomHasher {}

impl BloomHasher {
    pub fn hash(bytes: &[u8], seed: u64) -> u64 {
        //twox_hash::xxh3::hash64_with_seed(bytes, seed)//NOTE: this function won't have a consistent hash across sessions, so we have to use another lib to do this
        xxhash_rust::xxh3::xxh3_64_with_seed(bytes, seed)
    }
    pub fn hash_to_index<T: AsRef<[u8]>>(item: T, seed: u64, arr_len: usize) -> usize {
        Self::hash(item.as_ref(), seed) as usize % arr_len
    }
    pub fn hash_key(key: Key, seed: u64) -> u64 {
        Self::hash(&key.to_le_bytes(), seed)
    }
    pub fn hash_key_to_index(key: Key, seed: u64, arr_len: usize) -> usize {
        Self::hash_key(key, seed) as usize % arr_len
    }
}
