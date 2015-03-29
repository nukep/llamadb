use databaseinfo::DatabaseInfo;

pub trait DatabaseStorage {
    type Info: DatabaseInfo;

    fn scan_table<'a>(&'a self, table: &'a <Self::Info as DatabaseInfo>::Table)
    -> Box<Iterator<Item=Box<[<Self::Info as DatabaseInfo>::ColumnValue]>> + 'a>;
}
