# Key-Value Store

A high-performance key-value store built in Rust with Log-Structured Merge (LSM) tree architecture.

## Features

- In-memory memtable for fast writes (implemented as AVL tree)
- Sorted String Tables (SST) on disk with multiple implementation options
  - Array SST: simple sorted files
  - B-tree SST: optimized for faster reads
- Configurable buffer pool for page caching using extendible hash table
- Bloom filters to optimize negative lookups
- Multiple compaction policies:
  - None: no compaction
  - Leveled: compaction by level
  - Tiered: compaction by size ratio
  - [Dostoevsky](https://scholar.harvard.edu/files/stratos/files/dostoevskykv.pdf): hybrid policy using tiered for lower levels and leveled for highest level
- Core operations:
  - `get`: Retrieve a value by key
  - `put`: Insert or update a key-value pair
  - `scan`: Range scan for key-value pairs within a range
  - `delete`: Remove a key-value pair

## Installation

Make sure you have Rust and Cargo installed. Then:

```bash
git clone <repository-url>
cd key-value-store
cargo build --release
```

## Usage

### Basic Example

```rust
use key_value::db::Database;

// Open or create a database
let mut db = Database::open("my_database");

// Customize configuration (optional)
let db = db.set_memtable_capacity_mb(1)
          .set_buffer_pool_capacity_mb(10)
          .set_enable_bloom_filter(true)
          .set_bloom_filter_bits_per_entry(5);

// Basic operations
db.put(1, 100);                  // Insert key-value pair
let value = db.get(1);           // Retrieve value (returns Option<i64>)
let range = db.scan(1, 10);      // Scan for keys in range [1, 10]
db.delete(1);                    // Delete key

// Close database (or let it drop automatically)
db.close();
```

## Configuration Options

The database is highly configurable with the following options:

### Memtable Configuration

- `memtable_capacity()`: Get maximum entries in memtable
- `set_memtable_capacity(capacity)`: Set max entries in memtable
- `set_memtable_capacity_mb(capacity_mb)`: Set max size in MB

### SST Configuration

- `sst_size_ratio()`: Get size ratio between SST levels
- `set_sst_size_ratio(ratio)`: Set size ratio for different levels
- `sst_implementation()`: Get current SST implementation
- `set_sst_implementation(impl)`: Set implementation (Array or Btree)
- `sst_search_algorithm()`: Get search algorithm for SSTs
- `set_sst_search_algorithm(algo)`: Set search algorithm (Default or BinarySearch)

### Buffer Pool Configuration

- `enable_buffer_pool()`: Check if buffer pool is enabled
- `set_enable_buffer_pool(enable)`: Enable/disable buffer pool
- `buffer_pool_capacity()`: Get max entries in buffer pool
- `set_buffer_pool_capacity(capacity)`: Set max entries
- `set_buffer_pool_capacity_mb(capacity_mb)`: Set max size in MB
- `buffer_pool_initial_size()`: Get initial size before expansion
- `set_buffer_pool_initial_size(size)`: Set initial size
- `set_buffer_pool_initial_size_mb(size_mb)`: Set initial size in MB

### Compaction Configuration

- `compaction_policy()`: Get current compaction policy
- `set_compaction_policy(policy)`: Set policy (None, Leveled, Tiered, Dostoevsky)

### Bloom Filter Configuration

- `enable_bloom_filter()`: Check if bloom filter is enabled
- `set_enable_bloom_filter(enable)`: Enable/disable bloom filter
- `bloom_filter_bits_per_entry()`: Get bits per entry in bloom filter
- `set_bloom_filter_bits_per_entry(bits)`: Set bits per entry

## Architecture

### Overview

The database follows an LSM tree architecture with the following components:

1. **Memtable**: An in-memory AVL tree that stores recently written data
2. **Sorted String Tables (SSTs)**: On-disk sorted files organized in levels
3. **Buffer Pool**: Caches frequently accessed disk pages
4. **Bloom Filters**: Probabilistic data structure to optimize negative lookups

### Data Flow

1. `put` operations insert data into the memtable
2. When memtable reaches capacity, it's flushed to disk as an SST file
3. `get` operations search the memtable first, then SSTs from youngest to oldest
4. `scan` operations search all sources and merge results to provide ordered data
5. Compaction merges multiple SSTs to maintain performance

### Implementation Details

#### Memtable (AVL Tree)
- Implemented as a self-balancing AVL tree for efficient in-memory storage
- Supports fast insertion, lookup, and in-order traversal operations
- Trades slightly slower writes for marginally faster reads compared to red-black trees

#### SST Implementations
1. **Array SST**
   - Key-value pairs stored contiguously in little-endian binary format
   - Binary search used for lookups
   - Simple implementation with good performance for smaller datasets

2. **B-tree SST**
   - Separated into two files: one for leaf nodes containing entries, one for inner nodes
   - Maximizes fanout by storing only delimiters in inner nodes
   - Enables faster lookups by reducing tree height
   - Designed as a complete B-tree for efficient navigation

#### Buffer Pool
- Implemented using an extendible hash table for page caching
- Uses xxHash64 for high-performance uniform hashing
- Hybrid Clock/LRU eviction policy:
  - Clock algorithm operates at the bucket level
  - LRU implemented within buckets for efficient page replacement
  - Accessed pages moved to the end of their bucket's list
  - Evicts least recently used page from buckets with access bit 0

#### Bloom Filters
- Probabilistic data structure for efficient negative lookups
- Uses up to M*ln2 hash functions (M = bits per entry)
- Implemented with xxHash64 using different seed values for each hash function
- Enables skipping SST searches when a key definitely doesn't exist

#### Compaction
- Merge sorting algorithm based on 2-pass external sorting
- For B-trees, implements a scalable node-building algorithm that:
  - Requires only one scan of the sorted file
  - Uses $O(\text{levels} \times \text{fanout})$ memory
  - Costs $O(N/B)$ I/Os to scan the file and $O(\text{inner nodes})$ I/Os to write

### Performance Considerations

- Memtable size affects write amplification and flush frequency
- Buffer pool size affects read performance for repeated queries
- Bloom filter bits per entry affects false positive rate
- Compaction policy affects read/write performance trade-offs
- B-tree vs Array SST choice affects read performance especially for larger datasets

## Running Tests

Run all tests:

```bash
cargo test
```

Run experiments:

```bash
cargo run --bin experiments --release
```

## Limitations and Considerations

- Changing certain database settings after data has been written may lead to undefined behavior:
  - Changing bloom filter bits per entry after creation will lead to incorrect queries
  - Changing SST implementation or disabling bloom filters may leave unused files
- The database uses `i64::MIN` as a tombstone value for deleted keys
- Database names must be valid directory names without whitespace 