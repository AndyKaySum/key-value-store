use std::fs::File;
use std::io::{BufReader, BufRead, SeekFrom, Seek};

pub struct SSTable {
    data_file: File,
}

impl SSTable {
    pub fn new(path: &str) -> SSTable {
        let data_file = File::open(path).unwrap();
        SSTable { data_file }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let mut reader = BufReader::new(&self.data_file);
        let mut line = String::new();
        if reader.seek(SeekFrom::Start(0)).is_err() {
            println!("Error seeeking to start of SST file: {:?}", self.data_file);
            return None;
        }
        reader.seek(SeekFrom::Start(0)).unwrap();
        while reader.read_line(&mut line).unwrap() > 0 {
            let parts: Vec<&str> = line.trim_end().split('\t').collect();
            if parts[0] == key {
                return Some(parts[1].to_string());
            }
            line.clear();
        }
        None
    }
}