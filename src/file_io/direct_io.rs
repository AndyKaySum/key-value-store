use std::{
    fs::{File, OpenOptions},
    io,
    path::Path,
};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

use crate::util::{system_info, types::Page};

#[cfg(unix)]
fn direct_io_flags() -> i32 {
    // Unix-specific code
    extern crate libc;
    //NOTE: O_DIRECT is not available on apple silicon, seems like they can't do direct I/O (need to confirm)
    libc::O_DIRECT
}

#[cfg(windows)]
fn direct_io_flags() -> u32 {
    // Windows-specific code
    extern crate winapi;
    use winapi::um::winbase::{FILE_FLAG_NO_BUFFERING, FILE_FLAG_WRITE_THROUGH};
    FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH //direct io flags for windows
}

fn open_options() -> OpenOptions {
    OpenOptions::new()
        .custom_flags(direct_io_flags())
        .to_owned()
}

///Opens (creates if doesn't exist) file with read and write permissions using direct I/O
pub fn create(path: &str) -> io::Result<File> {
    open_options()
        .read(true)
        .write(true)
        .create(true)
        .open(path)
}

///Opens file with read and write permissions using direct I/O
pub fn open(path: &str) -> io::Result<File> {
    open_options().read(true).write(true).open(path)
}

///Opens file with read only permissions
pub fn open_read(path: &str) -> io::Result<File> {
    open_options().read(true).open(path)
}

///Opens file write only permissions using direct I/O
pub fn open_write(path: &str) -> io::Result<File> {
    open_options().write(true).open(path)
}

///Opens file append only permissions  using direct I/O
pub fn open_append(path: &str) -> io::Result<File> {
    open_options().append(true).open(path)
}

pub fn path_exists(path: &str) -> bool {
    Path::new(path).exists()
}

///deserialize with a custom buffer size
pub fn read_page(
    reader: &mut (impl std::io::Read + std::io::Seek),
    page_index: Page,
) -> io::Result<Vec<u8>> {
    let mut buffer: Vec<u8> = vec![0; system_info::page_size()];
    reader.seek(io::SeekFrom::Start(
        (page_index * system_info::page_size()) as u64,
    ))?;
    let bytes_read = reader.read(&mut buffer)?;
    buffer.truncate(bytes_read);
    Ok(buffer)
}
