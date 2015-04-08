use databaseinfo::{ColumnInfo, DatabaseInfo, TableInfo};
use identifier::Identifier;
use super::columnnames::ColumnNames;

pub enum TableOrSubquery<'a, DB: DatabaseInfo>
where <DB as DatabaseInfo>::Table: 'a
{
    Table {
        source_id: u32,
        table: &'a <DB as DatabaseInfo>::Table
    },
    Subquery {
        source_id: u32,
        out_column_names: ColumnNames
    }
}

pub struct SourceScope<'a, 'b, DB: DatabaseInfo + 'a>
where <DB as DatabaseInfo>::Table: 'a, 'a: 'b
{
    pub parent: Option<&'b SourceScope<'a, 'b, DB>>,
    pub tables: Vec<TableOrSubquery<'a, DB>>,
    pub table_aliases: Vec<Identifier>
}

impl<'a, 'b, DB: DatabaseInfo> SourceScope<'a, 'b, DB>
where <DB as DatabaseInfo>::Table: 'a, 'a: 'b
{
    pub fn get_column_offset(&self, column_name: &Identifier) -> Option<(u32, u32)> {
        let mut candidates: Vec<_> = Vec::new();

        for table in &self.tables {
            match table {
                &TableOrSubquery::Table { source_id, table } => {
                    if let Some(column) = table.find_column_by_name(column_name) {
                        candidates.push((source_id, column.get_offset()))
                    }
                },
                &TableOrSubquery::Subquery { source_id, ref out_column_names, .. } => {
                    candidates.extend(out_column_names.get_column_offsets(column_name).into_iter().map(|offset| {
                        (source_id, offset)
                    }))
                }
            }
        }

        candidates.extend(self.parent.and_then(|parent| {
            parent.get_column_offset(column_name)
        }).into_iter());

        if candidates.len() == 1 {
            Some(candidates[0])
        } else {
            None
        }
    }

    pub fn get_table_column_offset(&self, table_name: &Identifier, column_name: &Identifier) -> Option<(u32, u32)> {
        let mut candidates: Vec<_> = Vec::new();

        let tables = self.table_aliases.iter().enumerate().filter_map(|(i, name)| {
            if name == table_name { Some(&self.tables[i]) }
            else { None }
        });

        for table in tables {
            match table {
                &TableOrSubquery::Table { source_id, table } => {
                    if let Some(column) = table.find_column_by_name(column_name) {
                        candidates.push((source_id, column.get_offset()))
                    }
                },
                &TableOrSubquery::Subquery { source_id, ref out_column_names, .. } => {
                    candidates.extend(out_column_names.get_column_offsets(column_name).into_iter().map(|offset| {
                        (source_id, offset)
                    }))
                }
            }
        }

        candidates.extend(self.parent.and_then(|parent| {
            parent.get_table_column_offset(table_name, column_name)
        }).into_iter());

        if candidates.len() == 1 {
            Some(candidates[0])
        } else {
            None
        }
    }
}
