use super::types::{Level, Page, Run};

pub fn sst(level: &Level, run: &Run) -> String {
    format!("{level}_{run}")
}

pub fn sst_page(level: &Level, run: &Run, page_index: &Page) -> String {
    format!("{level}_{run}_{page_index}")
}
