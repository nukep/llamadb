use types::DbType;
use std::borrow::Cow;

pub trait ColumnValueOps: Sized {
    fn from_string_literal(s: Cow<str>) -> Result<Self, Cow<str>>;
    fn from_number_literal(s: Cow<str>) -> Result<Self, Cow<str>>;

    fn from_bytes(dbtype: DbType, bytes: Cow<[u8]>) -> Result<Self, ()>;
    fn to_bytes(self, dbtype: DbType) -> Result<Box<[u8]>, ()>;
    fn get_dbtype(&self) -> DbType;

    /// Used for predicate logic (such as the entire WHERE expression).
    fn tests_true(&self) -> bool;

    fn cast(self, dbtype: DbType) -> Option<Self>;
    fn equals(&self, rhs: &Self) -> Self;
    fn not_equals(&self, rhs: &Self) -> Self;
    fn and(&self, rhs: &Self) -> Self;
    fn or(&self, rhs: &Self) -> Self;
    fn concat(&self, rhs: &Self) -> Self;
}
