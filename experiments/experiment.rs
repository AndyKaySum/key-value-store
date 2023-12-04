use csc443_project::{
    db::Database,
    util::{
        system_info::ENTRY_SIZE,
        testing,
        types::{Key, SstSearchAlgorithm, Value},
    },
};

use crate::benchmarker::Benchmarker;

const MEMTABLE_MB_SIZE: usize = 1;
const BUFFER_POOL_INITIAL_MB_SIZE: usize = 2;
const BUFFER_POOL_CAPACITY_MB_SIZE: usize = 10;
const BLOOM_FILTER_BITS_PER_ENTRY: usize = 5;

pub fn run(
    database_alterations: Box<dyn FnMut(Database) -> Database>,
) -> (Vec<usize>, [Vec<f64>; 3]) {
    let bytes_per_mb = 2_usize.pow(20);
    let window_duration_sec = 10;
    let num_trials = 10;

    let db_mb_sizes: Vec<usize> = (0..11).map(|value: u32| 2_usize.pow(value)).collect();
    let db_byte_sizes: Vec<usize> = db_mb_sizes
        .iter()
        .map(|value: &usize| value * bytes_per_mb)
        .collect();
    let num_elements_in_smallest_db = (db_byte_sizes.first().unwrap() / ENTRY_SIZE) as Key; //For experiments where we want to use keys in the db
                                                                                            // let num_elements_in_largest_db = (db_byte_sizes.last().unwrap() / ENTRY_SIZE) as Key; //For experiments where we want to use keys that are not in the db

    println!("Experiment details: ");
    println!("number of trials for each experiment: {num_trials}");
    println!("Window duration: {window_duration_sec} seconds");
    println!("Experiment sizes (MB): {:?}", db_mb_sizes);
    println!("NOTE: results are in operations per second\n");

    let mut bm = Benchmarker::new(
        Box::new(database_alterations),
        db_byte_sizes,
        window_duration_sec,
        num_trials,
    );

    //get range ensures that all get inputs are keys that actually exist in the db
    let get_range_lower = 0;
    let get_range_upper = num_elements_in_smallest_db;
    println!(
        "Get experiment, get random value in range {}..{}",
        get_range_lower, get_range_upper
    );
    let get_experiment = &mut |db: &mut Database, key: &Key, _value: &Value| {
        db.get(*key);
    };
    let get_experiment_input_range = (get_range_lower, get_range_upper);
    let get_results = bm.run_experiment(get_experiment, &get_experiment_input_range);
    println!("{:?}", get_results);

    //scan range ensures that entire scan size is within db
    let scan_size = 100;
    let scan_range_lower = 0;
    let scan_range_upper = num_elements_in_smallest_db - scan_size;
    println!(
        "Scan experiment, scan size:{scan_size} in range {}..{}",
        scan_range_lower, scan_range_upper
    );
    let scan_experiment = &mut |db: &mut Database, key: &Key, _value: &Value| {
        db.scan(*key, *key + scan_size);
    };
    let scan_experiment_input_range = (scan_range_lower, scan_range_upper);
    let scan_results = bm.run_experiment(scan_experiment, &scan_experiment_input_range);
    println!("{:?}", scan_results);

    //put range has a low probability of adding something already in the memtable
    let put_range_lower = Key::MIN + 1;
    let put_range_upper = Key::MAX;

    println!(
        "put experiment, put random (key, value) in range {}..{} (key range only, value can be anything)",
        put_range_lower, put_range_upper
    );
    let put_experiment = &mut |db: &mut Database, key: &Key, value: &Value| {
        db.put(*key, *value);
    };
    let put_experiment_input_range = (put_range_lower, put_range_upper);
    let put_results = bm.run_reset_experiment(put_experiment, &put_experiment_input_range);
    println!("{:?}\n", put_results);

    (db_mb_sizes, [get_results, scan_results, put_results])
}

pub fn run_and_save(database_alterations: Box<dyn FnMut(Database) -> Database>, filename: &str) {
    let (db_mb_sizes, [get_results, scan_results, put_results]) = run(database_alterations);

    let mut output = "size, get, scan, put\n".to_string();
    for (i, size) in db_mb_sizes.iter().enumerate() {
        let line = format!(
            "{}, {}, {}, {}\n",
            size, get_results[i], scan_results[i], put_results[i]
        );
        output.push_str(&line);
    }

    std::fs::write(format!("{filename}.csv"), output)
        .unwrap_or_else(|_| panic!("Unable to write file for {}", filename));
}

pub fn common_database_alterations(db: Database) -> Database {
    db.set_memtable_capacity_mb(MEMTABLE_MB_SIZE)
        .set_buffer_pool_capacity_mb(BUFFER_POOL_CAPACITY_MB_SIZE)
        .set_buffer_pool_initial_size_mb(BUFFER_POOL_INITIAL_MB_SIZE)
        .set_bloom_filter_bits_per_entry(BLOOM_FILTER_BITS_PER_ENTRY)
}

pub fn part1() {
    /*
        Design an experiment comparing your binary search to B-tree search in terms of query
        throughput (on the y-axis) as you increase the data size (on the x-axis). This
        experiment should be done with uniformly randomly distributed point queries and data.
        The buffer pool should be enabled in this experiment, and the data should grow beyond
        the maximum buffer pool size so that evictions kick in. Explain your findings.
    */
    println!("Part 1: Experiment");
    println!("Memtable Size: {} MB\n", MEMTABLE_MB_SIZE);

    let database_alterations = |db: Database| -> Database {
        common_database_alterations(testing::part1_db_alterations(db))
    };

    run_and_save(Box::new(database_alterations), "part1_experiments");
}

pub fn part2() {
    /*
        Design an experiment comparing your binary search to B-tree search in
        terms of query throughput (on the y-axis) as you increase the data size (on the x-axis).
        This experiment should be done with uniformly randomly distributed point queries and data.
        The buffer pool should be enabled in this experiment, and the data should grow beyond the maximum
        buffer pool size so that evictions kick in. Explain your findings.
    */
    println!("Part 2: Experiment (b-tree)");
    println!("Memtable Size: {} MB", MEMTABLE_MB_SIZE);
    println!(
        "Buffer pool initial size: {} MB",
        BUFFER_POOL_INITIAL_MB_SIZE
    );
    println!(
        "Buffer pool capacity: {} MB\n",
        BUFFER_POOL_CAPACITY_MB_SIZE
    );

    let btree_database_alterations = |db: Database| -> Database {
        common_database_alterations(testing::part2_db_alterations(db))
            .set_sst_search_algorithm(SstSearchAlgorithm::Default)
    };
    run_and_save(
        Box::new(btree_database_alterations),
        "part2_btree_experiments",
    );

    println!("Part 2: Experiment (binary search)");
    println!("Memtable Size: {} MB", MEMTABLE_MB_SIZE);
    println!(
        "Buffer pool initial size: {} MB",
        BUFFER_POOL_INITIAL_MB_SIZE
    );
    println!(
        "Buffer pool capacity: {} MB\n",
        BUFFER_POOL_CAPACITY_MB_SIZE
    );

    let binary_search_database_alterations = |db: Database| -> Database {
        common_database_alterations(testing::part2_db_alterations(db))
            .set_sst_search_algorithm(SstSearchAlgorithm::BinarySearch)
    };
    run_and_save(
        Box::new(binary_search_database_alterations),
        "part2_binary_search_experiments",
    );
}

pub fn part3() {
    /*
        Measure insertion, get, and scan throughput for your implementation over time as the data
        size grows. Describe your experimental setup and make sure all relevant variables are
        controlled. Please fix the buffer pool size to 10 MB, the Bloom filters to use 5 bits per entry,
        and the memtable to 1 MB. Run this experiment as you insert 1 GB of data. Measure get and
        scan throughput at regular intervals as you insert this data. If you did any of the bonus
        tasks, please make sure to report how they are used in the experiment.
    */

    println!("Part 3: Experiment");
    println!("Memtable Size: {} MB", MEMTABLE_MB_SIZE);
    println!(
        "Buffer pool initial size: {} MB",
        BUFFER_POOL_INITIAL_MB_SIZE
    );
    println!("Buffer pool capacity: {} MB", BUFFER_POOL_CAPACITY_MB_SIZE);
    println!(
        "Bloom filter bits per entry: {}\n",
        BLOOM_FILTER_BITS_PER_ENTRY
    );

    let database_alterations = |db: Database| -> Database {
        common_database_alterations(testing::part3_db_alterations(db))
    };

    run_and_save(Box::new(database_alterations), "part3_experiments");
}
