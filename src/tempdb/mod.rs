//! A temporary in-memory database meant to hold us over until the pager and
//! B+Tree modules are finalized.
//!
//! This module will be removed once the pager and B+Tree are functional.

use std::borrow::Cow;
use std::collections::BTreeSet;

use columnvalueops::ColumnValueOps;
use databaseinfo::{DatabaseInfo, TableInfo, ColumnInfo};
use databasestorage::{Group, DatabaseStorage};
use identifier::Identifier;
use types::{DbType, Variant};
use sqlsyntax::ast;

mod table;
use self::table::Table;

pub struct TempDb {
    tables: Vec<Table>
}

pub enum ExecuteStatementResponse<'a> {
    Created,
    Inserted(u64),
    Select {
        column_names: Box<[String]>,
        rows: Box<Iterator<Item=Box<[Variant]>> + 'a>
    },
    Explain(String)
}

pub type ExecuteStatementResult<'a> = Result<ExecuteStatementResponse<'a>, String>;

impl DatabaseInfo for TempDb {
    type Table = Table;
    type ColumnValue = Variant;

    fn find_table_by_name(&self, name: &Identifier) -> Option<&Table> {
        self.tables.iter().find(|t| &t.name == name)
    }
}

struct ScanGroup<'a> {
    table: &'a Table
}

impl<'a> Group for ScanGroup<'a> {
    type ColumnValue = Variant;

    fn get_any_row<'b>(&'b self) -> Option<Cow<'b, [Variant]>> {
        self.iter().nth(0)
    }

    fn iter<'b>(&'b self) -> Box<Iterator<Item=Cow<'b, [Variant]>> + 'b> {
        let table = self.table;
        let columns: &'b [self::table::Column] = &table.columns;

        Box::new(table.rowid_index.iter().map(move |key_v| {
            use byteutils;
            use std::borrow::IntoCow;

            let raw_key: &[u8] = &key_v;
            trace!("KEY: {:?}", raw_key);

            let variable_column_count = columns.iter().filter(|column| {
                column.dbtype.is_variable_length()
            }).count();

            let variable_lengths: Vec<_> = (0..variable_column_count).map(|i| {
                let o = raw_key.len() - variable_column_count*8 + i*8;
                byteutils::read_udbinteger(&raw_key[o..o+8])
            }).collect();

            trace!("variable lengths: {:?}", variable_lengths);

            let _rowid: u64 = byteutils::read_udbinteger(&raw_key[0..8]);

            let mut variable_length_offset = 0;
            let mut key_offset = 8;

            let v: Vec<Variant> = columns.iter().map(|column| {
                let size = match column.dbtype.get_fixed_length() {
                    Some(l) => l as usize,
                    None => {
                        let l = variable_lengths[variable_length_offset];
                        variable_length_offset += 1;
                        l as usize
                    }
                };

                let bytes = &raw_key[key_offset..key_offset + size];

                trace!("from bytes: {:?}, {:?}", column.dbtype, bytes);
                let value = ColumnValueOps::from_bytes(column.dbtype, bytes.into_cow()).unwrap();
                key_offset += size;
                value
            }).collect();

            v.into_cow()
        }))
    }
}

impl DatabaseStorage for TempDb {
    type Info = TempDb;

    fn scan_table<'a>(&'a self, table: &'a Table)
    -> Box<Group<ColumnValue=Variant> + 'a>
    {
        Box::new(ScanGroup {
            table: table
        })
    }
}

impl TempDb {
    pub fn new() -> TempDb {
        TempDb {
            tables: Vec::new()
        }
    }

    pub fn execute_statement(&mut self, stmt: ast::Statement) -> ExecuteStatementResult {
        match stmt {
            ast::Statement::Create(create_stmt) => {
                match create_stmt {
                    ast::CreateStatement::Table(s) => self.create_table(s)
                }
            },
            ast::Statement::Insert(insert_stmt) => self.insert_into(insert_stmt),
            ast::Statement::Select(select_stmt) => self.select(select_stmt),
            ast::Statement::Explain(explain_stmt) => self.explain(explain_stmt)
        }
    }

    fn create_table(&mut self, stmt: ast::CreateTableStatement) -> ExecuteStatementResult {
        if stmt.table.database_name.is_some() {
            unimplemented!()
        }

        let table_name = Identifier::new(&stmt.table.table_name).unwrap();

        let columns_result: Result<_, String>;
        columns_result = stmt.columns.into_iter().enumerate().map(|(i, column)| {
            let name = Identifier::new(&column.column_name).unwrap();
            let type_name = Identifier::new(&column.type_name).unwrap();
            let type_array_size = match column.type_array_size {
                Some(Some(s)) => {
                    let v = try!(self.parse_number_as_u64(s));
                    Some(Some(v))
                },
                Some(None) => Some(None),
                None => None
            };

            let dbtype = try!(DbType::from_identifier(&type_name, type_array_size).ok_or(format!("{} is not a valid column type", type_name)));

            Ok(table::Column {
                offset: i as u32,
                name: name,
                dbtype: dbtype
            })
        }).collect();

        let columns = try!(columns_result);

        try!(self.add_table(Table {
            name: table_name,
            columns: columns,
            next_rowid: 1,
            rowid_index: BTreeSet::new()
        }));

        Ok(ExecuteStatementResponse::Created)
    }

    fn insert_into(&mut self, stmt: ast::InsertStatement) -> ExecuteStatementResult {
        use std::collections::VecMap;

        trace!("inserting row: {:?}", stmt);

        let mut table = try!(self.get_table_mut(stmt.table));

        let column_types: Vec<DbType> = table.get_columns().iter().map(|c| {
            c.dbtype
        }).collect();

        let ast_index_to_column_index: Vec<u32> = match stmt.into_columns {
            // Column names listed; map specified columns
            Some(v) => try!(v.into_iter().map(|column_name| {
                let ident = Identifier::new(&column_name).unwrap();
                match table.find_column_by_name(&ident) {
                    Some(column) => Ok(column.get_offset()),
                    None => Err(format!("column {} not in table", column_name))
                }
            }).collect()),
            // No column names are listed; map all columns
            None => (0..table.get_column_count()).collect()
        };

        trace!("ast_index_to_column_index: {:?}", ast_index_to_column_index);

        let column_index_to_ast_index: VecMap<usize> = ast_index_to_column_index.iter().enumerate().map(|(ast_index, column_index)| {
            ((*column_index) as usize, ast_index)
        }).collect();

        trace!("column_index_to_ast_index: {:?}", column_index_to_ast_index);

        match stmt.source {
            ast::InsertSource::Values(rows) => {
                let mut count = 0;

                for row in rows {
                    if ast_index_to_column_index.len() != row.len() {
                        return Err(format!("INSERT value contains wrong amount of columns"));
                    }

                    let iter = column_types.iter().enumerate().map(|(i, &dbtype)| {
                        let ast_index = column_index_to_ast_index.get(&i);

                        match ast_index {
                            Some(ast_index) => {
                                // TODO - allocate buffer outside of loop
                                let mut buf = Vec::new();
                                let expr = &row[*ast_index];
                                ast_expression_to_data(expr, dbtype, &mut buf);
                                Cow::Owned(buf)
                            },
                            None => {
                                // use default value for column type
                                dbtype.get_default()
                            }
                        }
                    });

                    try!(table.insert_row(iter).map_err(|e| e.to_string()));
                    count += 1;
                }

                Ok(ExecuteStatementResponse::Inserted(count))
            },
            ast::InsertSource::Select(s) => unimplemented!()
        }
    }

    fn select(&self, stmt: ast::SelectStatement) -> ExecuteStatementResult {
        use queryplan::{ExecuteQueryPlan, QueryPlan};

        let plan = try!(QueryPlan::compile_select(self, stmt).map_err(|e| format!("{}", e)));
        debug!("{}", plan);

        let mut rows = Vec::new();

        let execute = ExecuteQueryPlan::new(self);
        let result = execute.execute_query_plan(&plan.expr, &mut |r| {
            rows.push(r.to_vec().into_boxed_slice());
            Ok(())
        });

        trace!("{:?}", result);

        let column_names: Vec<String> = plan.out_column_names.iter().map(|ident| ident.to_string()).collect();

        Ok(ExecuteStatementResponse::Select {
            column_names: column_names.into_boxed_slice(),
            rows: Box::new(rows.into_iter())
        })
    }

    fn explain(&self, stmt: ast::ExplainStatement) -> ExecuteStatementResult {
        use queryplan::QueryPlan;

        match stmt {
            ast::ExplainStatement::Select(select) => {
                let plan = try!(QueryPlan::compile_select(self, select).map_err(|e| format!("{}", e)));

                Ok(ExecuteStatementResponse::Explain(plan.to_string()))
            }
        }
    }

    fn add_table(&mut self, table: Table) -> Result<(), String> {
        if self.tables.iter().any(|t| t.name == table.name) {
            Err(format!("Table {} already exists", table.name))
        } else {
            debug!("adding table: {:?}", table);
            self.tables.push(table);

            Ok(())
        }
    }

    fn get_table_mut(&mut self, table: ast::Table) -> Result<&mut Table, String> {
        if table.database_name.is_some() {
            unimplemented!()
        }

        let table_name = try!(Identifier::new(&table.table_name).ok_or(format!("Bad table name: {}", table.table_name)));

        match self.tables.iter_mut().find(|t| t.name == table_name) {
            Some(s) => Ok(s),
            None => Err(format!("Could not find table named {}", table_name))
        }
    }

    fn parse_number_as_u64(&self, number: String) -> Result<u64, String> {
        number.parse().map_err(|_| format!("{} is not a valid number", number))
    }
}

fn ast_expression_to_data(expr: &ast::Expression, column_type: DbType, buf: &mut Vec<u8>) {
    use sqlsyntax::ast::Expression::*;
    use std::borrow::IntoCow;

    let value: Variant = match expr {
        &StringLiteral(ref s) => {
            let r: &str = &s;
            ColumnValueOps::from_string_literal(r.into_cow()).unwrap()
        },
        &Number(ref n) => {
            let r: &str = &n;
            ColumnValueOps::from_number_literal(r.into_cow()).unwrap()
        },
        _ => unimplemented!()
    };

    let bytes = value.to_bytes(column_type).unwrap();
    buf.push_all(&bytes);
}
