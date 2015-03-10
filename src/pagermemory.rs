use super::pager;
use super::pager::{PageReference, Pager};
use std::os;
use std::collections::{HashMap, VecDeque};

#[derive(Debug)]
pub enum PagerMemoryErr {
    MapError(os::MapError),
    PageDoesNotExist(u64)
}

pub type PagerMemoryResult<T> = Result<T, PagerMemoryErr>;

fn map_error(err: os::MapError) -> PagerMemoryErr {
    PagerMemoryErr::MapError(err)
}

/// The backing store for the memory pager is `mmap()`, or the operating
/// system's equivilent to `mmap()`.
/// The page size is queried from the operating system to improve memory
/// caching.
pub struct PagerMemory {
    max_page_size: usize,
    pages: HashMap<u64, os::MemoryMap>,
    next_page_id: u64,
    unused_pages: VecDeque<os::MemoryMap>
}

impl PagerMemory {
    pub fn new() -> PagerMemory {
        PagerMemory {
            // TODO: query maximum memory map length, somehow.
            // For now, it's safe to assume it's probably 4096
            max_page_size: 4096,
            pages: HashMap::new(),
            next_page_id: 0,
            unused_pages: VecDeque::new()
        }
    }
}

impl Pager for PagerMemory {
    type Err = PagerMemoryErr;

    fn max_page_size(&self) -> usize { self.max_page_size }

    fn check_page_id(&self, page_id: u64) -> PagerMemoryResult<()> {
        if self.pages.contains_key(&page_id) {
            Ok(())
        } else {
            Err(PagerMemoryErr::PageDoesNotExist(page_id))
        }
    }

    unsafe fn new_page_uninitialized<F>(&mut self, f: F) -> PagerMemoryResult<u64>
    where F: FnOnce(&mut [u8])
    {
        let mut memory_map = match self.unused_pages.pop_back() {
            Some(v) => v,
            None => {
                use std::os::MapOption::*;
                let m = try!(os::MemoryMap::new(pager::MIN_PAGE_SIZE, &[MapReadable, MapWritable]).map_err(map_error));
                assert!(m.len() <= self.max_page_size);
                m
            }
        };

        f(memory_map_as_mut_slice(&mut memory_map));

        let page_id = self.next_page_id;

        match self.pages.insert(page_id, memory_map) {
            None => (),
            Some(_) => unreachable!()
        }

        self.next_page_id += 1;

        Ok(page_id)
    }

    fn mut_page<F, R>(&mut self, page_id: u64, f: F) -> PagerMemoryResult<R>
    where F: FnOnce(&mut [u8]) -> R
    {
        let memory_map = match self.pages.get_mut(&page_id) {
            Some(m) => m,
            None => return Err(PagerMemoryErr::PageDoesNotExist(page_id))
        };

        let result = f(memory_map_as_mut_slice(memory_map));

        Ok(result)
    }

    fn get_page<'a, 'b>(&'a self, page_id: u64, buffer: &'b mut Vec<u8>) -> PagerMemoryResult<PageReference<'a, 'b>>
    {
        let memory_map = match self.pages.get(&page_id) {
            Some(m) => m,
            None => return Err(PagerMemoryErr::PageDoesNotExist(page_id))
        };

        let src = memory_map_as_slice(memory_map);

        Ok(PageReference::Immutable(src))
    }

    fn increment_change_counter(&mut self) -> PagerMemoryResult<()> {
        // do nothing
        Ok(())
    }

    fn mark_page_as_removed(&mut self, page_id: u64) -> PagerMemoryResult<()> {
        match self.pages.remove(&page_id) {
            Some(memory_map) => {
                self.unused_pages.push_back(memory_map);
                Ok(())
            },
            None => Err(PagerMemoryErr::PageDoesNotExist(page_id))
        }
    }
}

fn memory_map_as_slice<'a>(memory_map: &'a os::MemoryMap) -> &'a [u8] {
    use std::slice;
    unsafe {
        slice::from_raw_parts(memory_map.data(), memory_map.len())
    }
}

fn memory_map_as_mut_slice<'a>(memory_map: &'a mut os::MemoryMap) -> &'a mut [u8] {
    use std::slice;
    unsafe {
        slice::from_raw_parts_mut(memory_map.data(), memory_map.len())
    }
}
