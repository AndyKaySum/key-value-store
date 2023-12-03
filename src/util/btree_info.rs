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
