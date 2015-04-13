use types::DbType;
use std::borrow::Cow;

pub trait ColumnValueOps: Sized {
    fn from_string_literal(s: Cow<str>) -> Result<Self, Cow<str>>;
    fn from_number_literal(s: Cow<str>) -> Result<Self, Cow<str>>;

    fn from_f64(value: f64) -> Self;
    fn to_f64(self) -> Result<f64, ()>;

    fn from_u64(value: u64) -> Self;
    fn to_u64(self) -> Result<u64, ()>;

    fn null() -> Self { ColumnValueOps::from_3vl(0) }

    fn from_bytes(dbtype: DbType, bytes: Cow<[u8]>) -> Result<Self, ()>;
    fn to_bytes(self, dbtype: DbType) -> Result<Box<[u8]>, ()>;
    fn get_dbtype(&self) -> DbType;

    /// Used for predicate logic (such as the entire WHERE expression).
    fn tests_true(&self) -> bool;

    /// Must return one of the following:
    ///
    /// * -1 for false
    /// * 0 for null
    /// * +1 for true
    fn to_3vl(&self) -> i8;
    fn from_3vl(value: i8) -> Self;

    fn cast(self, dbtype: DbType) -> Option<Self>;
    fn concat(&self, rhs: &Self) -> Self;
    fn equals(&self, rhs: &Self) -> Self;
    fn not_equals(&self, rhs: &Self) -> Self;

    fn is_null(&self) -> bool { self.to_3vl() == 0 }

    fn not(&self) -> Self {
        ColumnValueOps::from_3vl(-self.to_3vl())
    }

    fn and(&self, rhs: &Self) -> Self{
        let (l, r) = (self.to_3vl(), rhs.to_3vl());

        ColumnValueOps::from_3vl(if l < r { l } else { r })
    }

    fn or(&self, rhs: &Self) -> Self {
        let (l, r) = (self.to_3vl(), rhs.to_3vl());

        ColumnValueOps::from_3vl(if l > r { l } else { r })
    }
}
