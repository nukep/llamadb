use databaseinfo::DatabaseInfo;
use std::borrow::Cow;
use std::cmp::Eq;
use std::hash::Hash;

pub trait DatabaseStorage {
    type Info: DatabaseInfo;

    fn scan_table<'a>(&'a self, table: &'a <Self::Info as DatabaseInfo>::Table)
    -> Box<Group<ColumnValue=<Self::Info as DatabaseInfo>::ColumnValue> + 'a>;
}

pub trait Group {
    type ColumnValue: Sized + Clone + Eq + Hash + 'static;

    /// Returns any arbitrary row in the group.
    /// Returns None if the group contains no rows.
    fn get_any_row<'a>(&'a self) -> Option<Cow<'a, [Self::ColumnValue]>>;
    fn iter<'a>(&'a self) -> Box<Iterator<Item=Cow<'a, [Self::ColumnValue]>> + 'a>;
}
