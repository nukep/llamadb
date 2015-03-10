use byteutils;

#[derive(Debug, PartialEq)]
pub enum BTreeCellErr {
    CellLengthTooSmall(usize),
    PayloadLengthTooSmall(u32)
}

#[derive(Debug, PartialEq)]
pub struct BTreeCell<'a> {
    pub left_page: u64,
    pub payload_length: u32,
    pub overflow_page: Option<u64>,
    pub in_page_payload: &'a [u8]
}

impl<'a> BTreeCell<'a> {
    /// Returns None if the data is corrupt
    pub fn read(data: &'a [u8]) -> Result<BTreeCell<'a>, BTreeCellErr> {
        if data.len() < 24 {
            return Err(BTreeCellErr::CellLengthTooSmall(data.len()));
        }

        let left_page = byteutils::read_u64_le(&data[0..8]);
        let payload_length = byteutils::read_u32_le(&data[8..12]);

        if payload_length < 4 {
            return Err(BTreeCellErr::PayloadLengthTooSmall(payload_length));
        }

        if payload_length as usize > data.len() - 12 {
            let overflow_page = byteutils::read_u64_le(&data[12..20]);
            Ok(BTreeCell {
                left_page: left_page,
                payload_length: payload_length,
                overflow_page: Some(overflow_page),
                in_page_payload: &data[20..]
            })
        } else {
            Ok(BTreeCell {
                left_page: left_page,
                payload_length: payload_length,
                overflow_page: None,
                in_page_payload: &data[12..12+payload_length as usize]
            })
        }
    }
}

#[cfg(test)]
mod test {
    use super::BTreeCell;

    #[test]
    fn test_btree_cell_unused() {
        // Cell with unused data
        assert_eq!(BTreeCell::read(&[
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
            5, 0, 0, 0,
            9, 8, 7, 6, 5,
            0, 0, 0, 0, 0, 0, 0     // unused data (and padding to 24 bytes)
        ]).unwrap(), BTreeCell {
            left_page: 0x0807060504030201,
            payload_length: 5,
            overflow_page: None,
            in_page_payload: &[9, 8, 7, 6, 5]
        });
    }

    #[test]
    fn test_btree_cell_overflow() {
        // Cell with overflow
        assert_eq!(BTreeCell::read(&[
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
            27, 0, 0, 0,
            0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
            9, 8, 7, 6, 5, 4, 3, 2, 1, 0
        ]).unwrap(), BTreeCell {
            left_page: 0x0807060504030201,
            payload_length: 27,
            overflow_page: Some(0x100F0E0D0C0B0A09),
            in_page_payload: &[9, 8, 7, 6, 5, 4, 3, 2, 1, 0]
        });
    }

    #[test]
    fn test_btree_cell_corrupt() {
        use super::BTreeCellErr::*;

        // Cell length is too small
        assert_eq!(BTreeCell::read(&[
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
            5, 0, 0, 0,
            9, 8, 7, 6, 5
        ]).unwrap_err(), CellLengthTooSmall(17));

        // Payload length is too small
        assert_eq!(BTreeCell::read(&[
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
            3, 0, 0, 0,
            9, 8, 7,
            0, 0, 0, 0, 0, 0, 0, 0, 0   // unused data (and padding to 24 bytes)
        ]).unwrap_err(), PayloadLengthTooSmall(3));
    }
}
