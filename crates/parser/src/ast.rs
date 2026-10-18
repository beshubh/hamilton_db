//! The statements hamilton_db understands.
//!
//! This is deliberately much smaller than sqlparser's AST: only what the
//! current milestone supports, using the database's own [`Column`] and
//! [`Value`] types. Names are not resolved against the catalog here; that is
//! the binder's job.

use common::{Column, Value};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Statement {
    CreateTable(CreateTable),
    Insert(Insert),
    Select(Select),
}

/// `CREATE TABLE name (col type [NULL | NOT NULL], ...)`
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTable {
    pub name: String,
    /// Columns are nullable unless declared `NOT NULL`.
    pub columns: Vec<Column>,
}

/// `INSERT INTO table VALUES (...), (...)`
///
/// Each row holds one literal per column, in table order. Row widths and
/// types are not checked against the table yet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Insert {
    pub table: String,
    pub rows: Vec<Vec<Value>>,
}

/// `SELECT * FROM table`
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Select {
    pub table: String,
}
