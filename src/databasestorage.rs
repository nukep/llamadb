use databaseinfo::DatabaseInfo;

pub trait DatabaseStorage {
    type Info: DatabaseInfo;

    fn scan_table<'a>(&'a self, table: &'a <Self::Info as DatabaseInfo>::Table)
    -> Box<Group<ColumnValue=<Self::Info as DatabaseInfo>::ColumnValue> + 'a>;
}

pub trait Group {
    type ColumnValue: Sized + 'static;

    fn iter<'a>(&'a self) -> Box<Iterator<Item=Box<[Self::ColumnValue]>> + 'a>;
}
