use std::old_io::fs::File;
use std::old_io as io;
use std::old_path::Path;
use std::cell::UnsafeCell;
use super::pager::{PageReference, Pager};
use super::byteutils;

const HEADER_SIZE: usize = 16;
/// Min page size: 2^6 = 64
const MIN_PAGE_SIZE_VALUE: u8 = 6;
/// Max page size: 2^16 = 65536
const MAX_PAGE_SIZE_VALUE: u8 = 16;

#[derive(Debug)]
pub enum PagerStreamErr {
    IoError(io::IoError),
    TriedToCreateOnNonEmptyStream,
    BadPageSize(usize),
    BadHeader,
    BadStreamSize(u64),
    PageDoesNotExist(u64)
}

pub type PagerStreamResult<T> = Result<T, PagerStreamErr>;

fn io_error(err: io::IoError) -> PagerStreamErr {
    PagerStreamErr::IoError(err)
}

pub struct PagerStream<S>
where S: io::Reader + io::Writer + io::Seek
{
    /// The stream is contained in an UnsafeCell to allow I/O access inside &self methods.
    stream: UnsafeCell<S>,
    page_size: usize,
    page_count: u64,

    /// A pre-allocated buffer for `mut_page()`. Keeping it here will avoid expensive reallocations.
    /// The buffer is contained in an UnsafeCell to allow usage with the stream, which needs to be simultaneously borrowed.
    mut_page_buffer: UnsafeCell<Box<[u8]>>
}

impl<S> PagerStream<S>
where S: io::Reader + io::Writer + io::Seek
{
    pub fn open(mut stream: S) -> PagerStreamResult<PagerStream<S>> {
        // Get stream size
        try!(stream.seek(0, io::SeekStyle::SeekEnd).map_err(io_error));
        let stream_size = try!(stream.tell().map_err(io_error));

        // Seek to the beginning
        try!(stream.seek(0, io::SeekStyle::SeekSet).map_err(io_error));

        // Read header
        let mut header_bytes = [0; HEADER_SIZE];
        try!(stream.read_at_least(HEADER_SIZE, &mut header_bytes).map_err(io_error));
        let header = try!(Header::parse(&header_bytes));

        let page_count = match (stream_size / header.page_size as u64, stream_size % header.page_size as u64) {
            (page_count, 0) => page_count,
            (0, _) | (_, _) => return Err(PagerStreamErr::BadStreamSize(stream_size)),
        };

        Ok(PagerStream {
            stream: UnsafeCell::new(stream),
            page_size: header.page_size,
            page_count: page_count,
            mut_page_buffer: UnsafeCell::new(vec![0; header.page_size].into_boxed_slice())
        })
    }

    pub fn create(mut stream: S, page_size: usize) -> PagerStreamResult<PagerStream<S>> {
        // Get stream size
        try!(stream.seek(0, io::SeekStyle::SeekEnd).map_err(io_error));
        let stream_size = try!(stream.tell().map_err(io_error));
        if stream_size != 0 {
            return Err(PagerStreamErr::TriedToCreateOnNonEmptyStream);
        }

        let header = Header {
            page_size: page_size,
            change_counter: 0,
            freelist_head: 0
        };
        let mut header_bytes = [0; HEADER_SIZE];
        try!(header.serialize(&mut header_bytes));

        // Write the header
        try!(stream.write_all(&header_bytes).map_err(io_error));

        // Pad the rest of the page with zeros
        let padding = vec![0; page_size - header_bytes.len()];
        try!(stream.write_all(padding.as_slice()).map_err(io_error));

        Ok(PagerStream {
            stream: UnsafeCell::new(stream),
            page_size: page_size,
            page_count: 1,
            mut_page_buffer: UnsafeCell::new(vec![0; page_size].into_boxed_slice())
        })
    }

    fn stream(&self) -> &mut S {
        use std::mem;
        unsafe { mem::transmute(self.stream.get()) }
    }

    fn mut_page_buffer(&self) -> &mut [u8] {
        use std::mem;
        let buffer_box: &mut Box<[u8]> = unsafe { mem::transmute(self.mut_page_buffer.get()) };
        &mut *buffer_box
    }
}

pub fn open_from_path(path: &Path) -> PagerStreamResult<PagerStream<File>> {
    use std::old_io::{FileAccess, FileMode};

    let file = try!(File::open_mode(path, FileMode::Append, FileAccess::ReadWrite).map_err(io_error));

    PagerStream::open(file)
}

pub fn create_from_path(path: &Path, page_size: usize) -> PagerStreamResult<PagerStream<File>> {
    use std::old_io::{FileAccess, FileMode};

    let file = try!(File::open_mode(path, FileMode::Truncate, FileAccess::ReadWrite).map_err(io_error));

    PagerStream::create(file, page_size)
}

struct Header {
    page_size: usize,
    change_counter: u64,
    freelist_head: u64
}

impl Header {
    fn parse(header: &[u8; HEADER_SIZE]) -> PagerStreamResult<Header> {
        use std::cmp::Ord;

        fn check_range<T>(value: T, min: T, max: T) -> PagerStreamResult<T>
        where T: Ord
        {
            if (value <= max) && (value >= min) { Ok(value) }
            else { Err(PagerStreamErr::BadHeader) }
        }

        let page_size: usize = 1 << try!(check_range(header[0], MIN_PAGE_SIZE_VALUE, MAX_PAGE_SIZE_VALUE));
        let change_counter: u64 = byteutils::read_u64_le(&header[1..9]);
        let freelist_head: u64 = byteutils::read_u64_le(&header[9..17]);

        Ok(Header {
            page_size: page_size,
            change_counter: change_counter,
            freelist_head: freelist_head
        })
    }

    fn serialize(&self, buffer: &mut [u8; HEADER_SIZE]) -> PagerStreamResult<()> {
        use std::num::Int;

        for x in buffer.iter_mut() { *x = 0; }

        if self.page_size.count_ones() != 1 {
            return Err(PagerStreamErr::BadPageSize(self.page_size));
        }
        let page_size_shl = self.page_size >> self.page_size.trailing_zeros();

        buffer[0] = page_size_shl as u8;
        byteutils::write_u64_le(self.change_counter, &mut buffer[1..9]);
        byteutils::write_u64_le(self.freelist_head, &mut buffer[9..17]);

        Ok(())
    }
}


impl<S> Pager for PagerStream<S>
where S: io::Reader + io::Writer + io::Seek
{
    type Err = PagerStreamErr;

    fn max_page_size(&self) -> usize { self.page_size }

    fn check_page_id(&self, page_id: u64) -> PagerStreamResult<()> {
        if page_id == 0 || page_id >= self.page_count {
            Err(PagerStreamErr::PageDoesNotExist(page_id))
        } else {
            Ok(())
        }
    }

    unsafe fn new_page_uninitialized<F>(&mut self, f: F) -> PagerStreamResult<u64>
    where F: FnOnce(&mut [u8])
    {
        let page_id = self.page_count;

        {
            let stream = self.stream();
            let buffer = self.mut_page_buffer();

            try!(stream.seek(0, io::SeekStyle::SeekEnd).map_err(io_error));

            f(buffer);

            // Write the new page
            try!(stream.write_all(buffer.as_slice()).map_err(io_error));
        }

        self.page_count += 1;

        Ok(page_id)
    }

    fn mut_page<F, R>(&mut self, page_id: u64, f: F) -> PagerStreamResult<R>
    where F: FnOnce(&mut [u8]) -> R
    {
        try!(self.check_page_id(page_id));

        let stream = self.stream();
        let buffer = self.mut_page_buffer();

        // Seek to the requested page
        let page_offset: u64 = page_id * self.page_size as u64;
        try!(stream.seek(page_offset as i64, io::SeekStyle::SeekSet).map_err(io_error));
        try!(stream.read_at_least(self.page_size, buffer).map_err(io_error));

        // Mutate the page buffer
        let result = f(buffer);

        // Write the mutated page back
        try!(stream.seek(page_offset as i64, io::SeekStyle::SeekSet).map_err(io_error));
        try!(stream.write_all(buffer).map_err(io_error));

        Ok(result)
    }

    fn get_page<'a, 'b>(&'a self, page_id: u64, buffer: &'b mut Vec<u8>) -> PagerStreamResult<PageReference<'a, 'b>>
    {
        use std::slice;

        try!(self.check_page_id(page_id));
        let stream = self.stream();

        // Seek to the requested page
        let page_offset: u64 = page_id * self.page_size as u64;
        try!(stream.seek(page_offset as i64, io::SeekStyle::SeekSet).map_err(io_error));

        // Ensure the buffer has enough capacity to store `self.page_size` contiguous bytes.
        if self.page_size > buffer.capacity() {
            let reserve = self.page_size - buffer.capacity();
            buffer.reserve(reserve);
        }

        unsafe {
            // Set the buffer length to 0, in case the I/O operations fail.
            // If I/O fails, the buffer will appear empty to the caller.
            buffer.set_len(0);

            let buffer_ptr = buffer.as_mut_slice().as_mut_ptr();
            let buffer_slice = slice::from_raw_parts_mut(buffer_ptr, self.page_size);
            try!(stream.read_at_least(self.page_size, buffer_slice).map_err(io_error));
            buffer.set_len(self.page_size);
        }

        Ok(PageReference::Mutable(buffer.as_mut_slice()))
    }

    fn increment_change_counter(&mut self) -> PagerStreamResult<()> {
        self.mut_page(0, |buffer| {
            let old_change_counter = byteutils::read_u64_le(&buffer[1..9]);
            let new_change_counter = old_change_counter + 1;

            byteutils::write_u64_le(new_change_counter, &mut buffer[1..9]);
        })
    }

    fn mark_page_as_removed(&mut self, page_id: u64) -> PagerStreamResult<()> {
        // TODO: implement
        Ok(())
    }
}
