use csc443_project::{
    db::Database,
    util::types::{Key, Value},
};

use crate::benchmarker::Benchmarker;

pub fn run(
    database_alterations: Box<dyn FnMut(Database) -> Database>,
) -> (Vec<usize>, [Vec<f64>; 3]) {
    let bytes_per_mb = 2_usize.pow(20);
    let window_duration_sec = 4;
    let num_trials = 10;

    let db_mb_sizes: Vec<usize> = (0..11).map(|value: u32| 2_usize.pow(value)).collect();
    let db_byte_sizes = db_mb_sizes
        .iter()
        .map(|value: &usize| value * bytes_per_mb)
        .collect();
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

    println!("Get experiment, get random value in range 0..9999999");
    let get_experiment = &mut |db: &mut Database, key: &Key| {
        db.get(*key);
    };
    let get_experiment_input: Vec<Key> = (0..9999999).collect();
    let get_results = bm.run_experiment(get_experiment, &get_experiment_input);
    println!("{:?}", get_results);

    println!("Scan experiment, full db scans");
    let scan_experiment = &mut |db: &mut Database, (key1, key2): &(Key, Key)| {
        db.scan(*key1, *key2);
    };
    let scan_ranges: Vec<(Key, Key)> = vec![(Key::MIN, Key::MAX)];
    let scan_results = bm.run_experiment(scan_experiment, &scan_ranges);
    println!("{:?}", scan_results);

    println!("put experiment, put random (value, value*2) as (key, value) in range -99999999..9999999");
    let put_experiment = &mut |db: &mut Database, (key, value): &(Key, Value)| {
        db.put(*key, *value);
    };
    let put_experiment_input: Vec<(Key, Value)> =
        (-99999999..9999999).map(|value| (value, value * 2)).collect();
    let put_results = bm.run_reset_experiment(put_experiment, &put_experiment_input);
    println!("{:?}", put_results);

    (db_mb_sizes, [get_results, scan_results, put_results])
}
