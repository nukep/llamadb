#![feature(collections, into_cow)]
#![allow(unused_variables, dead_code)]

#[macro_use]
extern crate log;

// pub mod btree;
// pub mod pager;
// pub mod pagermemory;
// pub mod pagerstream;
pub mod sqlsyntax;
pub mod tempdb;

mod byteutils;
mod columnvalueops;
mod databaseinfo;
mod databasestorage;
mod identifier;
mod queryplan;
mod types;

// pub use self::pager::Pager;
// pub use self::pagermemory::PagerMemory;
// pub use self::pagerstream::PagerStream;

pub enum SQLError {
}

pub type SQLResult<T> = Result<T, SQLError>;
