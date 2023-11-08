use std::fs::File;
use std::io::{self, Read, Result };
use std::io::Write;
use std::io::{BufReader, BufRead, SeekFrom, Seek};
#[derive(Debug)]
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
pub fn put_int(&self, pairs: Vec<(i64, i64)>, sst_num: u64) {
    let file_path = format!("memtable_{}.dat", sst_num); // Replace with your actual file path
    match File::create(&file_path) {
        Ok(mut f) => {
            for &(key, value) in pairs.iter() {
                f.write_all(&key.to_le_bytes());
                f.write_all(&value.to_le_bytes());
            }
        },
        Err(e) => {
            eprintln!("Failed to create file: {}", e);
            return;
        }
    };
}
pub fn get_int_at_index(&self, sst_num: u64, index: i64) -> io::Result<Option<(i64, i64)>> {
    let file_path = format!("sst_{}.dat", sst_num);
    let mut file = File::open(&file_path)?;
    let seek_pos = index * 16;
    file.seek(SeekFrom::Start(seek_pos as u64))?;
    let mut buffer = [0; 16];
    if file.read(&mut buffer)? == 16 {
        let (key_bytes, value_bytes) = buffer.split_at(8);
        let key = i64::from_le_bytes(key_bytes.try_into().unwrap());
        let value = i64::from_le_bytes(value_bytes.try_into().unwrap());
        Ok(Some((key, value)))
    } else {
        Ok(None)
    }
}


pub fn get_size(&self, sst_num: u64) -> Result<i64> {
    let file_path = format!("sst_{}.dat", sst_num);
    let file = File::open(&file_path)?;
    let metadata = file.metadata()?;
    let file_length = metadata.len();
    let num_pairs = file_length / 16;
    Ok(num_pairs as i64)
}
pub fn binary_search_range_scan(&self, target_key1: i64, target_key2: i64) -> std::io::Result<Vec<(i64, i64)>> {
    let mut start_index = 0;
    let mut end_index = self.get_size(0).unwrap() -1;
    let mut vec = Vec::new();
    let mut vec_left = Vec::new(); 
    let mut vec_right = Vec::new(); 
    while start_index <= end_index {
        let mid_index = (start_index + end_index) / 2;
        let result = self.get_int_at_index(0, mid_index)?;

        match result {
            Some((key, value)) => {
                if key >= target_key1 && key <= target_key2 {
                    vec.push((key, value));

                    let mut left_index = mid_index  - 1;
                    let mut right_index = mid_index + 1;

                    while left_index >= start_index && left_index < mid_index {
                        if let Some((key, value)) = self.get_int_at_index(0, left_index)? {
                            if key >= target_key1 {
                                vec_left.push((key, value));
                            } else {
                                break;
                            }
                        }
                        left_index = left_index -1; 
                    }

                    while right_index <= end_index {
                        if let Some((key, value)) = self.get_int_at_index(0, right_index)? {
                            if key <= target_key2 {
                                vec_right.push((key, value));
                            } else {
                                break;
                            }
                        }
                        right_index += 1;
                    }

                    vec_left.reverse();
                    vec_left.push(vec[0]);  // push the mid_element
                    vec_left.append(&mut vec_right);
                    return Ok(vec_left);
                } else if key < target_key1 {
                    start_index = mid_index + 1;
                } else {
                    end_index = if mid_index > 0 { mid_index - 1 } else { i64::MAX };
                }
            },
            None => {
                vec_left.reverse();
                vec_left.push(vec[0]);  // push the mid_element
                vec_left.append(&mut vec_right);
                return Ok(vec_left);
            }
        }

        if start_index > end_index {
            break;
        }
    }

    Ok(vec)
}

pub fn binary_search(&self,target_key:i64) -> std::io::Result<Option<(i64, i64)>> {
    let mut start_index = 0;
    let mut end_index = self.get_size(0).unwrap();
    while start_index <= end_index {
        let mut mid_index = (start_index + end_index) / 2;
        let result = self.get_int_at_index(0, mid_index)?;
    
        match result {
            Some((key, value)) => {
                if key == target_key {
                    // Found
                    return Ok(Some((key, value)))
                } else if key < target_key {
                    start_index = mid_index + 1;
                } else {
                    end_index = if mid_index > 0 { mid_index - 1 } else { 0 };
                }
            },
            None => {
                // Key not found
                return Ok(None);  // if not found
            }
        }
        // return None 
    }
    return Ok(None);

}




    
}


mod tests {
    use super::*;

    #[test]
    fn test_put_int() {
        // let mut sst: SSTable<i64, i64> = SSTable::new("test_db".to_string(), 0, 10);
        let sstable1_path = format!("memtable_{}.sst", 0);
        let sstable1 = SSTable::new(&sstable1_path);
        let vec = vec![(1, 2), (3, 4), (5, 3), (7,8)];
        let vec2 = vec![(1, 2)];
        sstable1.put_int(vec, 0); 
        // let result = db.scan("a".to_string(), "c".to_string());
        assert_eq!(sstable1.get_int_at_index(0,2).unwrap().unwrap(),(5, 3));
        assert_eq!(sstable1.get_size(0).unwrap(),4);
        assert_eq!(sstable1.binary_search(1).unwrap(), Some((1, 2)));
        assert_eq!(sstable1.binary_search_range_scan(1,7).unwrap(), vec![(1, 2), (3, 4), (5, 3), (7,8)])
    }

}