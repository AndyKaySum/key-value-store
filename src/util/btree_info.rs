use super::{
    system_info::page_size,
    types::{Key, Page},
};

///Size of B-tree node in bytes
pub const NODE_ELEMENT_SIZE: usize = std::mem::size_of::<Key>();

pub const ROOT_PAGE_OFFSET: Page = 0;

pub fn node_size() -> usize {
    page_size()
}

pub fn fanout() -> usize {
    node_size() / NODE_ELEMENT_SIZE
}

// #[derive(Debug, PartialEq)]
// pub struct BtreeMetadata {
//     pub num_entries: Size, //number of entries in the tree
//     pub allocated_num_entries: Size //number of elements worth of file space that was allocated for this tree (important for navigating tree)
// }

// impl BtreeMetadata {
//     pub fn new(num_entries: Size, allocated_num_entries: Size) -> Self {
//         Self {
//             num_entries,
//             allocated_num_entries
//         }
//     }
//     ///Convert BtreeMetadata to a vec of little endian bytes
//     pub fn to_le_bytes(&self) -> Vec<u8> {
//         let mut buffer: Vec<u8> = Vec::with_capacity(std::mem::size_of_val(self)); //capacity to prevent reallcation on push (or save space)

//         buffer.extend_from_slice(&self.num_entries.to_le_bytes());
//         buffer.extend_from_slice(&self.allocated_num_entries.to_le_bytes());

//         buffer
//     }
//     ///Convert little endian byte buffer to BtreeMetadata struct instance. NOTE: this function ignores bytes after size_of(BtreeMetadata)
//     pub fn from_le_bytes(buffer: &[u8]) -> Result<Self, std::array::TryFromSliceError> {
//         let (num_entries_slice, num_allocated_slice) = buffer.split_at(std::mem::size_of::<Size>());

//         let num_entries_bytes: [u8; std::mem::size_of::<Size>()] = num_entries_slice.try_into()?;
//         let num_allocated_bytes: [u8; std::mem::size_of::<Size>()] = num_allocated_slice.try_into()?;

//         let num_entries = Size::from_le_bytes(num_entries_bytes);
//         let allocated_num_entries = Size::from_le_bytes(num_allocated_bytes);

//         Ok(Self::new(num_entries, allocated_num_entries))
//     }
// }

// #[test]
// fn test_btree_metadata_le_bytes() {
//     let metadata = BtreeMetadata::new(23, 34);
//     let metadata_bytes = metadata.to_le_bytes();
//     assert_eq!(BtreeMetadata::from_le_bytes(&metadata_bytes).unwrap(), metadata)
// }
