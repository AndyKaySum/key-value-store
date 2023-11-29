use std::io;

use crate::util::{
    btree_info::{fanout, node_size, NODE_ELEMENT_SIZE},
    types::Key,
};

///Returns a little endian buffer representation of B-tree inner node. NOTE: this function writes min(fanout, length) - 1 elements. Ie, this function assumes that the last element is not actually meant to be included in this node
/// The extra space (where last element would be) is used for metadata (# elements in node)
pub fn serialize(node_elements: &[Key]) -> Vec<u8> {
    let mut buffer: Vec<u8> = Vec::with_capacity(node_size()); //capacity to prevent reallcation on push
    let node_element_chunk = node_elements.iter().take(fanout()); //Elements needed for B-tree inner node
    let node_len = node_element_chunk.len() - 1;
    for key in node_element_chunk.take(node_len) {
        let key_bytes = key.to_le_bytes();
        buffer.extend_from_slice(&key_bytes);
    }

    //fill up remaining space with zeros
    let metadata_size = std::mem::size_of_val(&node_len);
    buffer.resize(node_size() - metadata_size, 0);

    //write metadata at end of buffer
    let node_len_bytes = node_len.to_le_bytes();
    buffer.extend_from_slice(&node_len_bytes);

    buffer
}

pub fn serialize_into(writer: &mut dyn std::io::Write, node_elements: &[Key]) -> io::Result<()> {
    let buffer = serialize(node_elements);
    //Direct IO requires that we write some multiple of a minimum write size
    //buffer should always be the size of a page (node_size), so this should be okay as is
    writer.write_all(&buffer)?;
    Ok(())
}

///Deserializes initial bytes of buffer into key-node_index pair.
/// NOTE: the buffer must be large enough to fit a key and node index type. Following bytes afterwards are ignored
pub fn deserialize_element(buffer: &[u8]) -> Result<Key, std::array::TryFromSliceError> {
    // let (key_slice, value_slice) = buffer.split_at(std::mem::size_of::<Key>());

    let key_bytes: [u8; std::mem::size_of::<Key>()] = buffer.try_into()?;
    // let value_bytes: [u8; std::mem::size_of::<Node>()] = value_slice.try_into()?;

    let key = Key::from_le_bytes(key_bytes);
    // let value = Node::from_le_bytes(value_bytes);

    // Ok((key, value))
    Ok(key)
}

pub fn deserialize_node_metadata(buffer: &[u8]) -> Result<usize, std::array::TryFromSliceError> {
    let num_elements_bytes: [u8; std::mem::size_of::<Key>()] =
        buffer[buffer.len() - std::mem::size_of::<usize>()..].try_into()?;
    Ok(usize::from_le_bytes(num_elements_bytes))
}

///Deserializes entire buffer
pub fn deserialize(buffer: &[u8]) -> Result<Vec<Key>, String> {
    let node_size = node_size();

    //make sure buffer has correct number of bytes to deserialize, should be a multiple of entry_size/byte_size
    if node_size != buffer.len() {
        return Err(format!(
            "{} is an invalid buffer size for deserialization, needs to be {}",
            buffer.len(),
            node_size
        ));
    }

    let num_entries = deserialize_node_metadata(buffer).unwrap(); //NOTE: the check above should guarantee that this does not error

    let mut node_elements: Vec<Key> = Vec::with_capacity(num_entries);

    //loop over groupings of bytes (size of an node element), convert them to key and node_index tuple, add to our vec
    //NOTE: we only loop over the amount of grouping specified in the metadata, otherwise we'll read in data that is not valid
    for byte_chunk in buffer.chunks(NODE_ELEMENT_SIZE).take(num_entries) {
        let entry = deserialize_element(byte_chunk).unwrap(); //NOTE: can unwrap because of the check at the start of the fn
        node_elements.push(entry);
    }
    Ok(node_elements)
}

// pub fn serialize_tree_metadata(metadata: &BtreeMetadata) -> Vec<u8> {
//     metadata.to_le_bytes()
// }

// pub fn serialize_tree_metadata_into(writer: &mut File, metadata: &BtreeMetadata) -> io::Result<()> {
//     let mut buffer = serialize_tree_metadata(metadata);
//     let buffer_len = buffer.len();
//     //Direct IO requires that we write some multiple of a minimum write size
//     //we will use the page size (mimimum write size is smaller for some machines, mine is 512 bytes for example),
//     //and then resize the file to be the actual number of bytes written
//     buffer.resize(nearest_min_write_size_multiple(buffer_len), 0);
//     writer.write_all(&buffer)?;
//     writer.set_len(buffer_len as u64)?;
//     Ok(())
// }

// pub fn deserialize_tree_metadata(buffer: &[u8]) -> Result<BtreeMetadata, std::array::TryFromSliceError> {
//     BtreeMetadata::from_le_bytes(buffer)
// }

#[test]
fn test_serde() {
    let entries: [Key; 3] = [2, -23, 12353242346];
    let buffer = serialize(&entries);
    assert_eq!(buffer.len(), node_size());
    let entries_deserialized = deserialize(&buffer).unwrap();

    assert_eq!(entries[..2].to_vec(), entries_deserialized);
}

#[test]
fn test_serde_large() {
    let entries: Vec<Key> = (-1000..1000).collect();
    let buffer = serialize(&entries);
    assert_eq!(buffer.len(), node_size());
    let entries_deserialized = deserialize(&buffer).unwrap();

    assert_eq!(entries[..fanout() - 1].to_vec(), entries_deserialized);
}
