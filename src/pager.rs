use std::fmt::Debug;

pub const MIN_PAGE_SIZE: usize = 1 << 6;

pub enum PageReference<'a, 'b> {
    Immutable(&'a [u8]),
    Mutable(&'b mut [u8])
}

pub trait Pager {
    type Err: Debug;

    fn max_page_size(&self) -> usize;

    /// Checks if the page id exists inside the Pager.
    ///
    /// If the page id doesn't exist, an implementation-defined error is returned.
    fn check_page_id(&self, page_id: u64) -> Result<(), Self::Err>;

    /// Allocates a new page. All bytes in the page are uninitialized.
    ///
    /// The buffer may contain remnants of previous pager operations, so
    /// reading said data may make potential bugs in the database more
    /// unpredictable and harder to identify.
    ///
    /// Returns a unique page id. The page id can be any value except for 0.
    unsafe fn new_page_uninitialized<F>(&mut self, f: F) -> Result<u64, Self::Err>
    where F: FnOnce(&mut [u8]);

    /// Allocates a new page. All bytes in the page are initialized to zeros.
    ///
    /// Returns a unique page id. The page id can be any value except for 0.
    fn new_page<F>(&mut self, f: F) -> Result<u64, Self::Err>
    where F: FnOnce(&mut [u8])
    {
        unsafe {
            self.new_page_uninitialized(|buffer| {
                for x in buffer.iter_mut() { *x = 0; }
                f(buffer);
            })
        }
    }

    fn mut_page<F, R>(&mut self, page_id: u64, f: F) -> Result<R, Self::Err>
    where F: FnOnce(&mut [u8]) -> R;

    fn get_page<'a, 'b>(&'a self, page_id: u64, buffer: &'b mut Vec<u8>) -> Result<PageReference<'a, 'b>, Self::Err>;

    fn increment_change_counter(&mut self) -> Result<(), Self::Err>;

    fn mark_page_as_removed(&mut self, page_id: u64) -> Result<(), Self::Err>;
}
