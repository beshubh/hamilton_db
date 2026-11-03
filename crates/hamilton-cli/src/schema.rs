//! The server's tables, as [`wire::SCHEMA_REQUEST`] reports them: for `\d`,
//! and for completion and highlighting, which read a copy refreshed after
//! each input rather than ask the server on every keystroke.

use std::io;
use std::sync::{Arc, RwLock};

use wire::QueryResult;

/// The tables, in name order.
#[derive(Debug, Default, PartialEq)]
pub struct Schema {
    pub tables: Vec<Table>,
}

#[derive(Debug, PartialEq)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    /// In the order they were created.
    pub indexes: Vec<Index>,
}

#[derive(Debug, PartialEq)]
pub struct Column {
    pub name: String,
    /// As SQL writes it, such as `VARCHAR(16)`.
    pub type_name: String,
    pub nullable: bool,
}

#[derive(Debug, PartialEq)]
pub struct Index {
    pub name: String,
    /// `PRIMARY KEY`, `UNIQUE`, `UNIQUE NULLS NOT DISTINCT`, or empty.
    pub kind: String,
    /// The key columns, joined by `", "`.
    pub columns: String,
}

/// The schema completion and highlighting see.
pub type SharedSchema = Arc<RwLock<Schema>>;

impl Schema {
    pub fn table(&self, name: &str) -> Option<&Table> {
        self.tables.iter().find(|t| t.name == name)
    }

    /// Reads the reply to a schema request.
    pub fn from_results(results: &[QueryResult]) -> io::Result<Self> {
        let [
            QueryResult::Rows { rows: columns, .. },
            QueryResult::Rows { rows: indexes, .. },
        ] = results
        else {
            return Err(bad("expected two row sets"));
        };
        let mut schema = Schema::default();
        for row in columns {
            let [table, name, type_name, nullable] = cells(row)?;
            if schema.tables.last().is_none_or(|t| t.name != table) {
                schema.tables.push(Table {
                    name: table.to_owned(),
                    columns: Vec::new(),
                    indexes: Vec::new(),
                });
            }
            schema.tables.last_mut().unwrap().columns.push(Column {
                name: name.to_owned(),
                type_name: type_name.to_owned(),
                nullable: nullable == "t",
            });
        }
        for row in indexes {
            let [name, table, kind, columns] = cells(row)?;
            let table = schema
                .tables
                .iter_mut()
                .find(|t| t.name == table)
                .ok_or_else(|| bad(format!("index {name} is on unknown table {table}")))?;
            table.indexes.push(Index {
                name: name.to_owned(),
                kind: kind.to_owned(),
                columns: columns.to_owned(),
            });
        }
        Ok(schema)
    }
}

fn cells(row: &[Option<String>]) -> io::Result<[&str; 4]> {
    let cells: Vec<&str> = row.iter().flatten().map(String::as_str).collect();
    cells
        .try_into()
        .map_err(|_| bad(format!("expected 4 values, not {row:?}")))
}

fn bad(message: impl Into<String>) -> io::Error {
    let message = format!("bad schema from the server: {}", message.into());
    io::Error::new(io::ErrorKind::InvalidData, message)
}
