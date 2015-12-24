use byteutils;
use types::DbType;
use databaseinfo::{ColumnInfo, TableInfo};
use identifier::Identifier;
use std::collections::BTreeSet;
use std::fmt;

pub enum UpdateError {
    ValidationError {
        column_name: Identifier,
    }
}

impl fmt::Display for UpdateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            &UpdateError::ValidationError { ref column_name } => {
                write!(f, "Problem validating column: {}", column_name)
            }
        }
    }
}

#[derive(Debug)]
pub struct Table {
    pub name: Identifier,
    pub columns: Vec<Column>,

    pub next_rowid: u64,
    pub rowid_index: BTreeSet<Vec<u8>>
}

#[derive(Debug)]
pub struct Column {
    pub offset: u32,
    pub name: Identifier,
    pub dbtype: DbType,
    pub nullable: bool
}

impl TableInfo for Table {
    type Column = Column;

    fn get_name(&self) -> &Identifier { &self.name }

    fn get_column_count(&self) -> u32 { self.columns.len() as u32 }

    fn find_column_by_offset(&self, offset: u32) -> Option<&Column> {
        let i = offset as usize;

        if i < self.columns.len() {
            Some(&self.columns[i])
        } else {
            None
        }
    }

    fn find_column_by_name(&self, name: &Identifier) -> Option<&Column> {
        self.columns.iter().find(|c| &c.name == name)
    }
}

impl Table {
    /// rowid is automatically added, and is not included as a specified column
    pub fn insert_row<I>(&mut self, column_data: I) -> Result<(), UpdateError>
    where I: ExactSizeIterator, I: Iterator<Item = (Box<[u8]>, Option<bool>)>
    {
        assert_eq!(self.columns.len(), column_data.len());

        let mut key: Vec<u8> = Vec::new();
        {
            let mut buf = [0; 8];
            byteutils::write_udbinteger(self.next_rowid, &mut buf);
            key.extend_from_slice(&buf);
        }

        let mut lengths = Vec::new();

        trace!("columns: {:?}", self.columns);

        for (column, (data_box, is_null)) in self.columns.iter().zip(column_data) {
            let data: &[u8] = &data_box;

            trace!("column data for {}: {:?}", column.name, data);

            let len = data.len() as u64;

            let append_data = match is_null {
                Some(true) => {
                    assert_eq!(len, 0);
                    key.push(1);

                    false
                },
                Some(false) => {
                    key.push(0);

                    true
                },
                None => true
            };

            if append_data {
                if column.dbtype.is_valid_length(len) {
                    if column.dbtype.is_variable_length() {
                        let mut buf = [0; 8];
                        byteutils::write_udbinteger(len, &mut buf);
                        lengths.extend_from_slice(&buf);
                    }

                    assert_eq!(column.nullable, is_null.is_some());

                    if let Some(is_null) = is_null {
                        if is_null {
                        }

                    }

                    key.extend_from_slice(data);
                } else {
                    return Err(UpdateError::ValidationError {
                        column_name: column.name.clone()
                    });
                }
            }
        }

        key.extend(lengths);

        trace!("inserting row {} into {}", self.next_rowid, self.name);
        trace!("inserting key into {}: {:?}", self.name, key);

        self.rowid_index.insert(key);
        self.next_rowid += 1;
        Ok(())
    }

    pub fn get_columns(&self) -> &Vec<Column> {
        &self.columns
    }
}

impl ColumnInfo for Column {
    fn get_offset(&self) -> u32 { self.offset }
    fn get_name(&self) -> &Identifier { &self.name }
    fn get_dbtype(&self) -> &DbType { &self.dbtype }
}
