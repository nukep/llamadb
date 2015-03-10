pub enum Bound<'a> {
    Included(&'a [u8]),
    Excluded(&'a [u8]),
    Unbounded
}

#[derive(PartialEq)]
pub enum Order {
    Ascending,
    Descending
}
