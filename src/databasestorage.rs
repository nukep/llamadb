use databaseinfo::DatabaseInfo;

pub trait DatabaseStorage {
    type Info: DatabaseInfo;
    type ScanTableRowIterator: Iterator<Item=Box<[<Self::Info as DatabaseInfo>::ColumnValue]>>;

    fn scan_table(&self, table: &<Self::Info as DatabaseInfo>::Table) -> Self::ScanTableRowIterator;
}
