use types::DbType;
use std::borrow::Cow;

pub trait ColumnValueOps: Sized {
    fn from_string_literal(s: Cow<str>) -> Result<Self, Cow<str>>;
    fn from_number_literal(s: Cow<str>) -> Result<Self, Cow<str>>;

    fn from_f64(value: f64) -> Self;
    fn to_f64(self) -> Result<f64, ()>;

    fn from_u64(value: u64) -> Self;
    fn to_u64(self) -> Result<u64, ()>;

    fn from_bytes(dbtype: DbType, bytes: Cow<[u8]>) -> Result<Self, ()>;
    fn to_bytes(self, dbtype: DbType) -> Result<Box<[u8]>, ()>;
    fn get_dbtype(&self) -> DbType;

    /// Must return one of the following:
    ///
    /// * -1 for false
    /// * 0 for null
    /// * +1 for true
    fn to_3vl(&self) -> i8;
    fn from_3vl(value: i8) -> Self;

    /// * None: self or rhs is NULL, or comparison is otherwise invalid
    /// * -1: self < rhs
    /// * 0: self == rhs
    /// * 1: self > rhs
    fn compare(&self, rhs: &Self) -> Option<i8>;
    fn cast(self, dbtype: DbType) -> Option<Self>;
    fn concat(&self, rhs: &Self) -> Self;
    fn add(&self, rhs: &Self) -> Self;
    fn sub(&self, rhs: &Self) -> Self;
    fn mul(&self, rhs: &Self) -> Self;
    fn div(&self, rhs: &Self) -> Self;
    fn negate(&self) -> Self;
}

pub trait ColumnValueOpsExt: ColumnValueOps {
    fn null() -> Self { ColumnValueOps::from_3vl(0) }

    fn equals(&self, rhs: &Self) -> Self {
        ColumnValueOps::from_3vl(match self.compare(rhs) {
            None => 0,
            Some(0) => 1,
            Some(_) => -1
        })
    }

    fn not_equals(&self, rhs: &Self) -> Self {
        ColumnValueOps::from_3vl(match self.compare(rhs) {
            None => 0,
            Some(0) => -1,
            Some(_) => 1
        })
    }

    fn less_than(&self, rhs: &Self) -> Self {
        ColumnValueOps::from_3vl(match self.compare(rhs) {
            None => 0,
            Some(-1) => 1,
            Some(_) => -1
        })
    }

    fn greater_than(&self, rhs: &Self) -> Self {
        ColumnValueOps::from_3vl(match self.compare(rhs) {
            None => 0,
            Some(1) => 1,
            Some(_) => -1
        })
    }

    fn less_than_or_equal(&self, rhs: &Self) -> Self {
        ColumnValueOps::from_3vl(match self.compare(rhs) {
            None => 0,
            Some(-1) | Some(0) => 1,
            Some(_) => -1
        })
    }

    fn greater_than_or_equal(&self, rhs: &Self) -> Self {
        ColumnValueOps::from_3vl(match self.compare(rhs) {
            None => 0,
            Some(0) | Some(1) => 1,
            Some(_) => -1
        })
    }

    fn is_null(&self) -> bool { self.to_3vl() == 0 }

    fn tests_true(&self) -> bool { self.to_3vl() == 1 }

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

impl<T: ColumnValueOps> ColumnValueOpsExt for T { }
