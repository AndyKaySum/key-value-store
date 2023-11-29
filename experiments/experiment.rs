use csc443_project::{
    db::Database,
    util::{
        system_info::ENTRY_SIZE,
        types::{Key, Value},
    },
};

use crate::benchmarker::Benchmarker;

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
    let num_elements_in_largest_db = (db_byte_sizes.last().unwrap() / ENTRY_SIZE) as Key; //For experiments where we want to use keys that are not in the db

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
    let get_experiment = &mut |db: &mut Database, key: &Key| {
        db.get(*key);
    };
    let get_experiment_input: Vec<Key> = (get_range_lower..get_range_upper).collect();
    let get_results = bm.run_experiment(get_experiment, &get_experiment_input);
    println!("{:?}", get_results);

    //scan range ensures that entire scan size is within db
    let scan_size = 100;
    let scan_range_lower = 0;
    let scan_range_upper = num_elements_in_smallest_db - scan_size;
    println!(
        "Scan experiment, scan size:{scan_size} in range {}..{}",
        scan_range_lower, scan_range_upper
    );
    let scan_experiment = &mut |db: &mut Database, (key1, key2): &(Key, Key)| {
        db.scan(*key1, *key2);
    };
    let scan_ranges: Vec<(Key, Key)> = (scan_range_lower..scan_range_upper)
        .map(|value| (value, value + scan_size))
        .collect();
    let scan_results = bm.run_experiment(scan_experiment, &scan_ranges);
    println!("{:?}", scan_results);

    //put range ensures that we are inserting new values
    let put_range_lower = -num_elements_in_largest_db;
    let put_range_upper = 0;
    println!(
        "put experiment, put random (value, value*2) as (key, value) in range {}..{}",
        put_range_lower, put_range_upper
    );
    let put_experiment = &mut |db: &mut Database, (key, value): &(Key, Value)| {
        db.put(*key, *value);
    };
    let put_experiment_input: Vec<(Key, Value)> = (put_range_lower..put_range_upper)
        .map(|value| (value, value * 2))
        .collect();
    let put_results = bm.run_reset_experiment(put_experiment, &put_experiment_input);
    println!("{:?}", put_results);

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
