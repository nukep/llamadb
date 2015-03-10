use byteutils;

#[derive(Debug, PartialEq)]
pub enum BTreePageErr {
    PageLengthWrong(usize),
    HeaderLengthWrong(usize),
    InteriorMustContainRightPage
}

mod consts {
    pub const MIN_PAGE_LENGTH: usize = 1 << 6;

    pub const ROOT_PAGE: u8 = 0b0000_0010;
    pub const LEAF_PAGE: u8 = 0b0000_0001;
}

#[derive(Debug, PartialEq)]
pub struct BTreePageRead<'a> {
    pub root: bool,
    pub leaf: bool,
    pub page_cell_count: u16,
    pub cell_length: u16,
    pub right_page: Option<u64>,
    pub data: &'a [u8]
}

impl<'a> BTreePageRead<'a> {
    pub fn read(data: &'a [u8]) -> Result<BTreePageRead<'a>, BTreePageErr> {
        use std::num::Int;

        // Ensure the page length is a power of two and is the minimum page length
        if !(data.len().count_ones() == 1 && data.len() >= consts::MIN_PAGE_LENGTH) {
            return Err(BTreePageErr::PageLengthWrong(data.len()));
        }

        let flags = data[0];
        let page_cell_count = byteutils::read_u16_le(&data[4..6]);
        let cell_length = byteutils::read_u16_le(&data[6..8]);

        let root = flags & consts::ROOT_PAGE != 0;
        let leaf = flags & consts::LEAF_PAGE != 0;

        // TODO: check for more invariants, such as page_cell_count and cell_length

        let right_page = match byteutils::read_u64_le(&data[8..16]) {
            0 => {
                // Make sure that this is a leaf node.
                // "0" indicates the last leaf node of the B+Tree.
                if leaf {
                    None
                } else {
                    return Err(BTreePageErr::InteriorMustContainRightPage);
                }
            },
            right_page => Some(right_page)
        };

        Ok(BTreePageRead {
            root: root,
            leaf: leaf,
            page_cell_count: page_cell_count,
            cell_length: cell_length,
            right_page: right_page,
            data: &data[16..]
        })
    }

    pub fn to_write(&self) -> BTreePageWrite {
        BTreePageWrite {
            root: self.root,
            leaf: self.leaf,
            page_cell_count: self.page_cell_count,
            cell_length: self.cell_length,
            right_page: self.right_page,
        }
    }
}

pub struct BTreePageWrite {
    pub root: bool,
    pub leaf: bool,
    pub page_cell_count: u16,
    pub cell_length: u16,
    pub right_page: Option<u64>
}

impl BTreePageWrite {
    pub fn write(&self, data: &mut [u8]) -> Result<(), BTreePageErr> {
        if data.len() != 16 {
            return Err(BTreePageErr::HeaderLengthWrong(data.len()));
        }

        if !self.leaf && self.right_page.is_none() {
            return Err(BTreePageErr::InteriorMustContainRightPage);
        }

        // TODO: check for more invariants, such as page_cell_count and cell_length

        let right_page = match self.right_page {
            None => 0,
            Some(page) => page
        };

        let flags = {
            let mut f = 0;
            if self.root { f |= consts::ROOT_PAGE }
            if self.leaf { f |= consts::LEAF_PAGE }
            f
        };

        data[0] = flags;
        data[1] = 0;
        data[2] = 0;
        data[3] = 0;
        byteutils::write_u16_le(self.page_cell_count, &mut data[4..6]);
        byteutils::write_u16_le(self.cell_length, &mut data[6..8]);
        byteutils::write_u64_le(right_page, &mut data[8..16]);

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{BTreePageRead, BTreePageWrite};

    #[test]
    fn test_btree_page_readwrite() {
        let header_buf = [
            0x02,
            0, 0, 0,
            5, 0,
            24, 0,
            0x02, 0x04, 0x06, 0x08, 0x0A, 0x0C, 0x0E, 0x10
        ];

        let mut page_buf: Vec<u8> = header_buf.to_vec();
        page_buf.extend(0..128-16);

        let page = BTreePageRead::read(page_buf.as_slice()).unwrap();
        assert_eq!(page, BTreePageRead {
            root: true,
            leaf: false,
            page_cell_count: 5,
            cell_length: 24,
            right_page: Some(0x100E0C0A08060402),
            data: &page_buf.as_slice()[16..]
        });

        let mut write_header_buf = [0; 16];
        page.to_write().write(&mut write_header_buf);
        assert_eq!(header_buf, write_header_buf);
    }
}
