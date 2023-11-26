use crate::{ceil_div, util::system_info};

pub fn nearest_min_write_size_multiple(size: usize) -> usize {
    let min_write_size = system_info::mimimum_write_size();
    let multiplier = ceil_div!(size, min_write_size);
    min_write_size * multiplier
}
