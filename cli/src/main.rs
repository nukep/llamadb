#![feature(collections)]

#[macro_use]
extern crate log;

extern crate linenoise;
extern crate llamadb;

use std::io::Write;

mod prettyselect;
use prettyselect::pretty_select;

fn main() {
    let mut lexer = llamadb::sqlsyntax::lexer::Lexer::new();

    let mut line = String::new();

    let mut db = llamadb::tempdb::TempDb::new();

    let mut out = std::io::stdout();

    loop {
        let prompt = if lexer.tokens.is_empty() && lexer.is_no_state() {
            "llamadb> "
        } else {
            "    ...> "
        };

        let val = linenoise::input(prompt);

        match val {
            None => break,
            Some(input) => {
                lexer.feed_characters(input.chars());
                lexer.feed_character(Some('\n'));

                line.extend(input.chars());
                line.push('\n');

                while let Some(i) = lexer.tokens.iter().position(|token| token == &llamadb::sqlsyntax::lexer::Token::Semicolon) {
                    match execute(&mut out, &mut db, &lexer.tokens[0..i+1]) {
                        Ok(()) => (),
                        Err(message) => println!("{}", message)
                    };

                    let right = lexer.tokens.split_off(i+1);
                    lexer.tokens = right;

                    if !line.is_empty() {
                        line.pop();
                        linenoise::history_add(&line);
                        line.clear();
                    }
                }
            }
        }
    }
}

fn execute(out: &mut Write, db: &mut llamadb::tempdb::TempDb, tokens: &[llamadb::sqlsyntax::lexer::Token])
-> Result<(), String>
{
    use llamadb::tempdb::ExecuteStatementResponse;

    let statement = match llamadb::sqlsyntax::parser::parse_statement(tokens) {
        Ok(stmt) => stmt,
        Err(e) => return Err(format!("syntax error: {}", e))
    };

    let result = match db.execute_statement(statement) {
        Ok(r) => r,
        Err(e) => return Err(format!("execution error: {}", e))
    };

    let write_result = match result {
        ExecuteStatementResponse::Created => {
            writeln!(out, "Created.")
        },
        ExecuteStatementResponse::Inserted(rows) => {
            writeln!(out, "{} rows inserted.", rows)
        },
        ExecuteStatementResponse::Select { column_names, rows } => {
            pretty_select(out, &column_names, rows, 32)
        },
        ExecuteStatementResponse::Explain(plan) => {
            writeln!(out, "{}", plan)
        },
    };

    write_result.unwrap();

    Ok(())
}
