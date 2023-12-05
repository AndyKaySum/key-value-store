use csc443_project::{
    ceil_div,
    db::Database,
    util::{
        system_info::ENTRY_SIZE,
        types::{Entry, Key, Size, Value},
    },
};
use rand::{seq::SliceRandom, thread_rng, Rng};
use std::{hint::black_box, time::Instant};

const NS_PER_SEC: u128 = 1_000_000_000; //For conversions from sec to nanosec

///Inserts num_bytes worth of entries, returns entries added (in order that they were added)
fn fill_db_with_size(db: &mut Database, num_bytes: Size) -> Vec<Entry> {
    let num_entries = ceil_div!(num_bytes, ENTRY_SIZE); //ceil divison
    let range = 0..num_entries;

    let mut entries = Vec::<Entry>::new();

    let mut rng = thread_rng();

    //generate keys and random values
    for i in range.into_iter() {
        entries.push((i as Key, rng.gen()))
    }

    //random insertion order to prevent insertion bias
    entries.shuffle(&mut rng);

    for (key, value) in entries.iter() {
        db.put(*key, *value);
    }

    entries
}

///Runs num_trials number of iterations, each iteration we count how many operations (with a random input) can be done within the window_duration.
/// Returns Average number of operations per second
#[allow(clippy::unit_arg)] //Get rid of black box warning
fn bench_throughput(
    db: &mut Database,
    experiment: &mut dyn FnMut(&mut Database, &Key, &Value),
    experiment_key_range: &(Key, Key),
    window_duration_sec: u128,
    num_trials: usize,
) -> f64 {
    let (lower, upper) = *experiment_key_range;
    let window_nano_sec = window_duration_sec * NS_PER_SEC;
    let mut opcount_each_trial = vec![0; num_trials];

    for op_count in opcount_each_trial.iter_mut() {
        let mut total_duration = 0;
        let mut rng = rand::thread_rng();
        while total_duration < window_nano_sec {
            //select random input for experiment
            let input_key = rng.gen_range(lower..upper);
            let input_value = rng.gen_range(Value::MIN + 1..=Value::MAX); //NOTE: We do MIN + 1, because MIN is not a valid value or key to enter into the DB

            let start = Instant::now();
            black_box(experiment(db, &input_key, &input_value)); //black box prevents our experiment from being optimized away
            let duration = start.elapsed().as_nanos();

            *op_count += 1;
            total_duration += duration;
        }
    }
    let total_opcount: u128 = opcount_each_trial.iter().sum();
    let avg_opcount = total_opcount as f64 / num_trials as f64;
    avg_opcount / window_duration_sec as f64
}

///Creates a db and runs experiment as many times as possible within window_duration, repeats num_trials times and
/// return the avg number of operations per second
fn bench_throughput_on_db_size(
    database_size_bytes: Size,
    database_alterations: &mut dyn FnMut(Database) -> Database,
    experiment: &mut dyn FnMut(&mut Database, &Key, &Value),
    experiment_key_range: &(Key, Key),
    window_duration_sec: u128,
    num_trials: usize,
) -> f64 {
    let experiment_dir = "experiment_database_bandwidth_temp_directory";
    let db_name = format!("{experiment_dir}/test");
    if std::path::Path::new(experiment_dir).exists() {
        std::fs::remove_dir_all(experiment_dir).unwrap(); //remove previous directory if panicked during tests and didn't clean up
    }
    std::fs::create_dir_all(experiment_dir).unwrap();

    let mut db = database_alterations(Database::open(&db_name));

    fill_db_with_size(&mut db, database_size_bytes);
    let ops_per_sec = bench_throughput(
        &mut db,
        experiment,
        experiment_key_range,
        window_duration_sec,
        num_trials,
    );

    //cleanup
    db.close();
    std::fs::remove_dir_all(experiment_dir).unwrap();

    ops_per_sec
}

///creates a new db instance each trial, returns avg ops/sec
fn bench_throughput_on_db_size_reset_each(
    database_size_bytes: Size,
    database_alterations: &mut dyn FnMut(Database) -> Database,
    experiment: &mut dyn FnMut(&mut Database, &Key, &Value),
    experiment_key_range: &(Key, Key),
    window_duration_sec: u128,
    num_trials: usize,
) -> f64 {
    let mut total_ops_per_sec = 0.0;
    for _ in 0..num_trials {
        total_ops_per_sec += bench_throughput_on_db_size(
            database_size_bytes,
            database_alterations,
            experiment,
            experiment_key_range,
            window_duration_sec,
            1,
        )
    }
    total_ops_per_sec / num_trials as f64
}

pub struct Benchmarker {
    pub database_alterations: Box<dyn FnMut(Database) -> Database>,
    pub db_byte_sizes: Vec<usize>,
    pub window_duration_sec: u128, //metric to measure bandwidth, (ops/(sec*window_size))
    pub num_trials: usize,
}

impl Benchmarker {
    pub fn new(
        database_alterations: Box<dyn FnMut(Database) -> Database>,
        db_byte_sizes: Vec<usize>,
        window_duration_sec: u128,
        num_trials: usize,
    ) -> Self {
        Self {
            database_alterations,
            db_byte_sizes,
            window_duration_sec,
            num_trials,
        }
    }
    ///Runs experiment, returns a vector of avg ops/sec
    pub fn run_experiment(
        &mut self,
        experiment: &mut dyn FnMut(&mut Database, &Key, &Value),
        experiment_key_range: &(Key, Key),
    ) -> Vec<f64> {
        let mut results = Vec::<f64>::new();
        for database_size_bytes in &self.db_byte_sizes {
            let data = bench_throughput_on_db_size(
                *database_size_bytes,
                &mut self.database_alterations,
                experiment,
                experiment_key_range,
                self.window_duration_sec,
                self.num_trials,
            );
            results.push(data);
        }
        results
    }
    ///Runs experiment, remaking the database each time, returns a vector of avg ops/sec
    pub fn run_reset_experiment(
        &mut self,
        experiment: &mut dyn FnMut(&mut Database, &Key, &Value),
        experiment_key_range: &(Key, Key),
    ) -> Vec<f64> {
        let mut results = Vec::<f64>::new();
        for database_size_bytes in &self.db_byte_sizes {
            let data = bench_throughput_on_db_size_reset_each(
                *database_size_bytes,
                &mut self.database_alterations,
                experiment,
                experiment_key_range,
                self.window_duration_sec,
                self.num_trials,
            );
            results.push(data);
        }
        results
    }
}
