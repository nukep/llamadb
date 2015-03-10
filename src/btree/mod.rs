mod bound;
mod cell;
mod page;
pub use self::bound::*;

use super::pager::Pager;

mod btree_consts {
    pub const MAX_DEPTH: usize = 256;
}

#[derive(Debug)]
pub enum BTreeError<P: Pager> {
    PagerError(P::Err),
    BTreePageError(self::page::BTreePageErr)
}

fn pager_error<P: Pager>(err: P::Err) -> BTreeError<P> {
    BTreeError::PagerError(err)
}

pub struct BTreeCollection<'a, P: Pager + 'a> {
    pager: &'a P
}

impl<'a, P: Pager + 'a> BTreeCollection<'a, P> {
    pub fn new<'l>(pager: &'l P) -> BTreeCollection<'l, P> {
        BTreeCollection {
            pager: pager
        }
    }

    pub fn get_btree(&self, page_id: u64) -> Result<BTree<P>, BTreeError<P>>
    {
        try!(self.pager.check_page_id(page_id).map_err(pager_error::<P>));

        Ok(BTree {
            pager: self.pager,
            root_page_id: page_id
        })
    }
}

pub struct BTreeCollectionMut<'a, P: Pager + 'a> {
    pager: &'a mut P
}

impl<'a, P: Pager + 'a> BTreeCollectionMut<'a, P> {
    pub fn new<'l>(pager: &'l mut P) -> BTreeCollectionMut<'l, P> {
        BTreeCollectionMut {
            pager: pager
        }
    }

    pub fn new_btree(&mut self, cell_length: u16) -> Result<u64, BTreeError<P>>
    {
        let page_id = try!(self.pager.new_page(|buffer| {
            let write_result = page::BTreePageWrite {
                root: true,
                leaf: true,
                page_cell_count: 0,
                cell_length: cell_length,
                right_page: None
            }.write(&mut buffer[0..16]);
        }).map_err(pager_error::<P>));

        Ok(page_id)
    }

    pub fn get_btree(&mut self, page_id: u64) -> Result<BTree<P>, BTreeError<P>>
    {
        try!(self.pager.check_page_id(page_id).map_err(pager_error::<P>));

        Ok(BTree {
            pager: self.pager,
            root_page_id: page_id
        })
    }

    pub fn mut_btree(&mut self, page_id: u64) -> Result<BTreeMut<P>, BTreeError<P>>
    {
        try!(self.pager.check_page_id(page_id).map_err(pager_error::<P>));

        Ok(BTreeMut {
            pager: self.pager,
            root_page_id: page_id
        })
    }

    pub fn remove_btree(&mut self, page_id: u64) -> Result<(), BTreeError<P>>
    {
        try!(self.pager.check_page_id(page_id).map_err(pager_error::<P>));
        try!(self.pager.mark_page_as_removed(page_id).map_err(pager_error::<P>));

        unimplemented!()
    }
}

/// The B+Tree is assumed to be ordered byte-wise.
pub struct BTree<'a, P: Pager + 'a> {
    pager: &'a P,
    root_page_id: u64
}

impl<'a, P: Pager + 'a> BTree<'a, P> {
    pub fn find_keys(&self, min: Bound, max: Bound, order: Order) -> Result<BTreeKeyIter, BTreeError<P>>
    {
        // Assume Ascending order
        if order == Order::Descending { unimplemented!() }



        unimplemented!()
    }
}

pub struct BTreeMut<'a, P: Pager + 'a> {
    pager: &'a mut P,
    root_page_id: u64
}

impl<'a, P: Pager + 'a> BTreeMut<'a, P> {
    pub fn insert_key(&mut self, key: &[u8]) -> Result<(), BTreeError<P>>
    {
        // Non-root page:
        // If the page overflows, split it so that the first half remains in the
        // current page, and the second half is put in a new page.
        // Insert the middle key into the parent page.
        //
        // Root page:
        // If the page overflows, split it into two new pages.
        // Clear the current page and insert the middle key into the current page.
        //
        // On split, if the cell count is odd,
        unimplemented!()
    }

    pub fn update_keys<F>(&mut self, min: Bound, max: Bound, transform: F) -> Result<(), BTreeError<P>>
    where F: FnMut(&[u8], &mut Vec<u8>)
    {
        unimplemented!()
    }

    pub fn remove_keys(&mut self, min: Bound, max: Bound) -> Result<(), BTreeError<P>>
    {
        unimplemented!()
    }
}


pub struct BTreeKeyIter;
