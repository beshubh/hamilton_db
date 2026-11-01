//! The connection to the server, and what its replies mean for printing.

use std::io;

use wire::backend::{ErrorFields, Field};
use wire::{Client, ClientError, QueryResult, oid};

use crate::schema::Schema;

/// What a statement produced, for printing.
#[derive(Debug, PartialEq)]
pub enum Output {
    /// The rows of a SELECT, `None` being NULL.
    Rows {
        columns: Vec<OutputColumn>,
        rows: Vec<Vec<Option<String>>>,
    },
    /// How many rows an INSERT, UPDATE or DELETE changed.
    Count { command: String, count: i64 },
    /// A statement that reports only that it ran.
    Done(String),
}

#[derive(Debug, PartialEq)]
pub struct OutputColumn {
    pub name: String,
    /// Numbers line up on the right.
    pub numeric: bool,
}

impl OutputColumn {
    pub fn new(name: &str, numeric: bool) -> Self {
        Self {
            name: name.to_owned(),
            numeric,
        }
    }
}

pub struct Connection {
    client: Client,
    /// `host:port`, for the banner and prompt.
    addr: String,
}

impl Connection {
    pub fn connect(host: &str, port: u16) -> Result<Self, ClientError> {
        let user = std::env::var("USER").unwrap_or_else(|_| "hamilton".to_owned());
        Ok(Self {
            client: Client::connect((host, port), &user)?,
            addr: format!("{host}:{port}"),
        })
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Runs `sql`, one statement, returning what it produced or the error it
    /// failed with. Fails only if the connection does.
    pub fn execute(&mut self, sql: &str) -> io::Result<Result<Vec<Output>, ErrorFields>> {
        let response = self.client.simple_query(sql).map_err(lost)?;
        if let Some(error) = response.error {
            return Ok(Err(error));
        }
        Ok(Ok(response
            .results
            .into_iter()
            .filter_map(output)
            .collect()))
    }

    pub fn schema(&mut self) -> io::Result<Schema> {
        let response = self
            .client
            .simple_query(wire::SCHEMA_REQUEST)
            .map_err(lost)?;
        if let Some(error) = response.error {
            return Err(io::Error::other(format!(
                "reading the schema failed: {error}"
            )));
        }
        Schema::from_results(&response.results)
    }
}

fn lost(err: io::Error) -> io::Error {
    io::Error::new(
        err.kind(),
        format!("lost the connection to the server: {err}"),
    )
}

fn output(result: QueryResult) -> Option<Output> {
    Some(match result {
        QueryResult::Rows { fields, rows, .. } => rows_output(fields, rows),
        QueryResult::Command(tag) => command_output(tag),
        QueryResult::Empty => return None,
    })
}

fn rows_output(fields: Vec<Field>, mut rows: Vec<Vec<Option<String>>>) -> Output {
    // Booleans come as `t` and `f`, and print as `true` and `false`.
    for (i, field) in fields.iter().enumerate() {
        if field.type_oid == oid::BOOL {
            for value in rows.iter_mut().filter_map(|row| row[i].as_mut()) {
                *value = if value == "t" { "true" } else { "false" }.to_owned();
            }
        }
    }
    let numeric = |field: &Field| matches!(field.type_oid, oid::INT4 | oid::INT8 | oid::FLOAT8);
    let columns = fields
        .iter()
        .map(|f| OutputColumn::new(&f.name, numeric(f)))
        .collect();
    Output::Rows { columns, rows }
}

/// A tag that ends in a count, as `UPDATE 2` and `INSERT 0 2` do, is a
/// count of rows changed by its first word.
fn command_output(tag: String) -> Output {
    let words: Vec<&str> = tag.split_whitespace().collect();
    match words.as_slice() {
        [command, .., count] => match count.parse() {
            Ok(count) => Output::Count {
                command: command.to_string(),
                count,
            },
            Err(_) => Output::Done(tag),
        },
        _ => Output::Done(tag),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_tags() {
        let count = |command: &str, count| Output::Count {
            command: command.to_owned(),
            count,
        };
        assert_eq!(command_output("INSERT 0 3".into()), count("INSERT", 3));
        assert_eq!(command_output("DELETE 1".into()), count("DELETE", 1));
        assert_eq!(
            command_output("CREATE TABLE".into()),
            Output::Done("CREATE TABLE".into())
        );
        assert_eq!(command_output("BEGIN".into()), Output::Done("BEGIN".into()));
    }
}
