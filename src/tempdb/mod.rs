//! A temporary in-memory database meant to hold us over until the pager and
//! B+Tree modules are finalized.
//!
//! This module will be removed once the pager and B+Tree are functional.

use std::borrow::Cow;
use std::collections::BTreeSet;

use columnvalueops::{ColumnValueOps, ColumnValueOpsExt};
use databaseinfo::{DatabaseInfo, TableInfo, ColumnInfo};
use databasestorage::{Group, DatabaseStorage};
use identifier::Identifier;
use types::{DbType, Variant};
use sqlsyntax::ast;
use queryplan::{self, ExecuteQueryPlan, QueryPlan};

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

    fn count(&self) -> u64 {
        self.table.rowid_index.len() as u64
    }

    fn iter<'b>(&'b self) -> Box<Iterator<Item=Cow<'b, [Variant]>> + 'b> {
        let table = self.table;
        let columns: &'b [self::table::Column] = &table.columns;

        Box::new(table.rowid_index.iter().map(move |key_v| {
            use byteutils;
            use std::borrow::Cow;

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
                let is_null = if column.nullable {
                    let flag = raw_key[key_offset];
                    key_offset += 1;
                    flag != 0
                } else {
                    false
                };

                if is_null {
                    ColumnValueOpsExt::null()
                } else {
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
                    let value = ColumnValueOps::from_bytes(column.dbtype, Cow::Borrowed(bytes)).unwrap();
                    key_offset += size;
                    value
                }
            }).collect();

            Cow::Owned(v)
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

            let nullable = column.constraints.iter().any(|c| {
                c.constraint == ast::CreateTableColumnConstraintType::Nullable
            });

            Ok(table::Column {
                offset: i as u32,
                name: name,
                dbtype: dbtype,
                nullable: nullable
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
        trace!("inserting row: {:?}", stmt);

        let table_name = stmt.table.table_name;
        let column_types: Vec<(DbType, bool)>;
        let ast_index_to_column_index: Vec<u32>;

        {
            let table = try!(self.get_table_mut(&table_name));

            column_types = table.get_columns().iter().map(|c| {
                (c.dbtype, c.nullable)
            }).collect();

            ast_index_to_column_index = match stmt.into_columns {
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
        }

        match stmt.source {
            ast::InsertSource::Values(rows) => {
                let mut count = 0;

                for row in rows {
                    if ast_index_to_column_index.len() != row.len() {
                        return Err(format!("INSERT value contains wrong amount of columns"));
                    }

                    let mut exprs: Vec<Option<ast::Expression>>;
                    exprs = (0..column_types.len()).map(|_| None).collect();

                    for (i, expr) in row.into_iter().enumerate() {
                        exprs[ast_index_to_column_index[i] as usize] = Some(expr);
                    }

                    // TODO: don't allow expressions that SELECT the same table that's being inserted into
                    let v: Vec<_> = try!({column_types.iter().zip(exprs.into_iter()).map(|(&(dbtype, nullable), expr)| {
                        match expr {
                            Some(expr) => {
                                // TODO - allocate buffer outside of loop
                                let mut buf = Vec::new();

                                let execute = ExecuteQueryPlan::new(self);

                                let sexpr = match queryplan::compile_ast_expression(self, expr).map_err(|e| format!("{}", e)) {
                                    Ok(v) => v,
                                    Err(e) => return Err(e)
                                };
                                let value = try!(execute.execute_expression(&sexpr));

                                let is_null = try!(variant_to_data(value, dbtype, nullable, &mut buf));
                                Ok((buf.into_boxed_slice(), is_null))
                            },
                            None => {
                                // use default value for column type
                                let is_null = if nullable { Some(true) } else { None };
                                Ok((dbtype.get_default().into_owned().into_boxed_slice(), is_null))
                            }
                        }
                    }).collect()});

                    let mut table = try!(self.get_table_mut(&table_name));
                    try!(table.insert_row(v.into_iter()).map_err(|e| format!("{}", e)));
                    count += 1;
                }

                Ok(ExecuteStatementResponse::Inserted(count))
            },
            ast::InsertSource::Select(_s) => unimplemented!()
        }
    }

    fn select(&self, stmt: ast::SelectStatement) -> ExecuteStatementResult {
        let plan = try!(QueryPlan::compile_select(self, stmt).map_err(|e| format!("{}", e)));
        debug!("{}", plan);

        let mut rows = Vec::new();

        let execute = ExecuteQueryPlan::new(self);
        try!(execute.execute_query_plan(&plan.expr, &mut |r| {
            rows.push(r.to_vec().into_boxed_slice());
            Ok(())
        }));

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

    fn get_table_mut(&mut self, table_name: &str) -> Result<&mut Table, String> {
        let table_name = try!(Identifier::new(table_name).ok_or(format!("Bad table name: {}", table_name)));

        match self.tables.iter_mut().find(|t| t.name == table_name) {
            Some(s) => Ok(s),
            None => Err(format!("Could not find table named {}", table_name))
        }
    }

    fn parse_number_as_u64(&self, number: String) -> Result<u64, String> {
        number.parse().map_err(|_| format!("{} is not a valid number", number))
    }
}

fn variant_to_data(value: Variant, column_type: DbType, nullable: bool, buf: &mut Vec<u8>)
-> Result<Option<bool>, String> {
    match (value.is_null(), nullable) {
        (true, true) => Ok(Some(true)),
        (true, false) => {
            Err(format!("cannot insert NULL into column that doesn't allow NULL"))
        },
        (false, nullable) => {
            let bytes = value.to_bytes(column_type).unwrap();
            buf.extend_from_slice(&bytes);

            Ok(if nullable { Some(false) } else { None })
        }
    }
}
