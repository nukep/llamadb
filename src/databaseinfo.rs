use columnvalueops::ColumnValueOps;
use identifier::Identifier;
use types::DbType;
use std::fmt;
use std::cmp::Eq;
use std::hash::Hash;

/// A read-only interface to information about the database schema.
pub trait DatabaseInfo {
    type Table: TableInfo;
    type ColumnValue: ColumnValueOps + fmt::Display + Clone + Eq + Hash + 'static;

    fn find_table_by_name(&self, name: &Identifier) -> Option<&Self::Table>;
}

pub trait TableInfo {
    type Column: ColumnInfo;

    fn get_name(&self) -> &Identifier;
    fn get_column_count(&self) -> u32;
    fn find_column_by_offset(&self, offset: u32) -> Option<&Self::Column>;
    fn find_column_by_name(&self, name: &Identifier) -> Option<&Self::Column>;

    fn get_column_names(&self) -> Vec<Identifier> {
        (0..self.get_column_count()).map(|i| {
            let column = self.find_column_by_offset(i).unwrap();
            column.get_name().clone()
        }).collect()
    }
}

pub trait ColumnInfo {
    fn get_offset(&self) -> u32;
    fn get_name(&self) -> &Identifier;
    fn get_dbtype(&self) -> &DbType;
}
