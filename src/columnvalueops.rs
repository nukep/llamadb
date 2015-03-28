use std::borrow::Cow;

pub trait ColumnValueOps: Sized {
    fn from_string_literal(s: Cow<str>) -> Result<Self, Cow<str>>;
    fn from_number_literal(s: Cow<str>) -> Result<Self, Cow<str>>;

    /// Used for predicate logic (such as the entire WHERE expression).
    fn tests_true(&self) -> bool;

    fn equals(&self, rhs: &Self) -> Self;
    fn not_equals(&self, rhs: &Self) -> Self;
    fn and(&self, rhs: &Self) -> Self;
    fn or(&self, rhs: &Self) -> Self;
    fn concat(&self, rhs: &Self) -> Self;
}
