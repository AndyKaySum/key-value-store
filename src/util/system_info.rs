use super::types::{Key, Value};

pub const ENTRY_SIZE: usize = std::mem::size_of::<Key>() + std::mem::size_of::<Value>();

pub fn page_size() -> usize {
    //NOTE: using a libary for this might be overkill, maybe just fix value to 4k isntead (consider this later)
    page_size::get()
}

pub fn num_entries_per_page() -> usize {
    page_size() / ENTRY_SIZE
}

pub fn mimimum_write_size() -> usize {
    page_size() / 8 //TODO: test on various systems, may need to change based on OS or system
}
