use std::io;
use std::{fs::File, io::Write};

use crate::util::system_info::ENTRY_SIZE;
use crate::util::types::{Key, Value};

use super::direct_io::read_page;
use super::serde_util::nearest_min_write_size_multiple;

///Returns a little endian buffer representation of entry array
pub fn serialize(entries: &[(Key, Value)]) -> Vec<u8> {
    let buffer_size = entries.len() * ENTRY_SIZE;
    let mut buffer: Vec<u8> = Vec::with_capacity(buffer_size); //capacity to prevent reallcation on push
    for (key, value) in entries {
        let key_bytes = key.to_le_bytes();
        let value_bytes = value.to_le_bytes();
        buffer.extend_from_slice(&key_bytes);
        buffer.extend_from_slice(&value_bytes);
    }
    buffer
}

pub fn serialize_into_no_resize(writer: &mut File, entries: &[(Key, Value)]) -> io::Result<usize> {
    let mut buffer = serialize(entries);
    let buffer_len = buffer.len();
    //Direct IO requires that we write some multiple of a minimum write size
    //we will use the page size (mimimum write size is smaller for some machines, mine is 512 bytes for example),
    //and then resize the file to be the actual number of bytes written
    buffer.resize(nearest_min_write_size_multiple(buffer_len), 0);
    writer.write_all(&buffer)?;
    Ok(buffer_len)
}

pub fn serialize_into(writer: &mut File, entries: &[(Key, Value)]) -> io::Result<()> {
    let buffer_len = serialize_into_no_resize(writer, entries)?;
    writer.set_len(buffer_len as u64)
}

///Deserializes initial bytes of buffer into key-value pair.
/// NOTE: the buffer must be large enough to fit a key and value type. Following bytes afterwards are ignored
pub fn deserialize_entry(buffer: &[u8]) -> Result<(Key, Value), std::array::TryFromSliceError> {
    let (key_slice, value_slice) = buffer.split_at(std::mem::size_of::<Key>());

    let key_bytes: [u8; std::mem::size_of::<Key>()] = key_slice.try_into()?;
    let value_bytes: [u8; std::mem::size_of::<Value>()] = value_slice.try_into()?;

    let key = Key::from_le_bytes(key_bytes);
    let value = Value::from_le_bytes(value_bytes);

    Ok((key, value))
}

///Deserializes entire buffer
pub fn deserialize(buffer: &[u8]) -> Result<Vec<(Key, Value)>, String> {
    let num_entries = buffer.len() / ENTRY_SIZE;

    //make sure buffer has correct number of bytes to deserialize, should be a multiple of entry_size/byte_size
    if num_entries * ENTRY_SIZE != buffer.len() {
        return Err(format!(
            "{} is an invalid buffer size for deserialization, needs to be a multiple of {}",
            buffer.len(),
            ENTRY_SIZE
        ));
    }

    let mut entries: Vec<(Key, Value)> = Vec::with_capacity(num_entries);

    //loop over groupings of bytes (size of an entry), convert them to key and value tuple, add to our vec
    for byte_chunk in buffer.chunks(ENTRY_SIZE) {
        let entry = deserialize_entry(byte_chunk).unwrap(); //NOTE: can unwrap because of the check at the start of the fn
        entries.push(entry);
    }
    Ok(entries)
}

///deserialize with a custom buffer size
pub fn buffered_deserialize_from(
    reader: &mut impl std::io::Read,
    buffer_size: usize,
) -> io::Result<Vec<(Key, Value)>> {
    if buffer_size <= 0 {
        return Ok(vec![]);
    }
    let mut buffer: Vec<u8> = vec![0; buffer_size];
    let bytes_read = reader.read(&mut buffer)?;
    Ok(deserialize(&buffer[..bytes_read]).unwrap()) //NOTE: can unwrap because buffer size determines if we get an error, which we control
}

///deserializes entire file
pub fn deserialize_from(reader: &mut File) -> io::Result<Vec<(Key, Value)>> {
    buffered_deserialize_from(
        reader,
        nearest_min_write_size_multiple(reader.metadata()?.len() as usize),
    )
}

///deserializes entire page
pub fn deserialize_page(
    reader: &mut (impl std::io::Read + std::io::Seek),
    page_index: usize,
) -> io::Result<Vec<(Key, Value)>> {
    let buffer = read_page(reader, page_index)?;
    deserialize(&buffer)
        .map_err(|why| panic!("Failed to deserialize page {page_index}, reason: {why}"))
}

///deserialize a single entry within a page. NOTE: entry index is equal to the index it would have if it were in a (key,value) tuple array
pub fn deserialize_entry_within_page(
    buffer: &[u8],
    entry_index: usize,
) -> Result<(Key, Value), std::array::TryFromSliceError> {
    if buffer.len() < (entry_index + 1) * ENTRY_SIZE {
        panic!(
            "Entry is outside of buffer, buffer size: {}, entry offset: {}",
            buffer.len(),
            entry_index * ENTRY_SIZE
        );
    }
    let index = entry_index * ENTRY_SIZE; //index within buffer
    deserialize_entry(&buffer[index..index + ENTRY_SIZE])
}

#[test]
fn test_serde() {
    let entries: [(Key, Value); 3] = [(2, 1), (-23, 323), (12353242346, -21312345434)];
    let buffer = serialize(&entries);
    let entries_deserialized = deserialize(&buffer).unwrap();

    assert_eq!(Vec::from(entries), entries_deserialized);
}
