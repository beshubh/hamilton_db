//! Printing statement results the way `psql` does.

use std::fmt;

use common::{DataType, Value};

use crate::database::Output;

impl fmt::Display for Output {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Output::Rows { columns, rows } => {
                let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
                let cells: Vec<Vec<String>> = rows
                    .iter()
                    .map(|row| row.iter().map(cell).collect())
                    .collect();
                let widths: Vec<usize> = names
                    .iter()
                    .enumerate()
                    .map(|(i, name)| {
                        cells
                            .iter()
                            .map(|row| row[i].chars().count())
                            .chain([name.chars().count()])
                            .max()
                            .unwrap_or(0)
                    })
                    .collect();

                // Names are centered, like psql.
                let header: Vec<String> = names
                    .iter()
                    .zip(&widths)
                    .map(|(name, &width)| format!("{name:^width$}"))
                    .collect();
                writeln!(f, " {}", header.join(" | ").trim_end())?;
                let rule: Vec<String> = widths.iter().map(|&w| "-".repeat(w + 2)).collect();
                writeln!(f, "{}", rule.join("+"))?;
                for row in &cells {
                    let row: Vec<String> = row
                        .iter()
                        .zip(columns)
                        .zip(&widths)
                        .map(|((cell, column), &width)| {
                            if is_numeric(column.data_type) {
                                format!("{cell:>width$}")
                            } else {
                                format!("{cell:<width$}")
                            }
                        })
                        .collect();
                    writeln!(f, " {}", row.join(" | ").trim_end())?;
                }
                let plural = if rows.len() == 1 { "" } else { "s" };
                write!(f, "({} row{plural})", rows.len())
            }
            Output::Count { command, count } => write!(f, "{command} {count}"),
            Output::Done(command) => f.write_str(command),
        }
    }
}

/// NULL prints as an empty cell, as in psql, so it can't be mistaken for
/// the string 'NULL'.
fn cell(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        value => value.to_string(),
    }
}

fn is_numeric(data_type: DataType) -> bool {
    matches!(data_type, DataType::Int | DataType::BigInt)
}

#[cfg(test)]
mod tests {
    use common::Column;

    use super::*;

    #[test]
    fn rows_as_a_table() {
        let output = Output::Rows {
            columns: vec![
                Column::new("id", DataType::Int, false),
                Column::new("name", DataType::Varchar(8), true),
            ],
            rows: vec![
                vec![Value::Int(1), Value::from("ann")],
                vec![Value::Int(200), Value::Null],
            ],
        };
        assert_eq!(
            output.to_string(),
            [
                " id  | name",
                "-----+------",
                "   1 | ann",
                " 200 |",
                "(2 rows)",
            ]
            .join("\n")
        );
    }

    #[test]
    fn no_rows() {
        let output = Output::Rows {
            columns: vec![Column::new("a", DataType::Bool, true)],
            rows: vec![],
        };
        assert_eq!(output.to_string(), " a\n---\n(0 rows)");
    }

    #[test]
    fn counts_and_commands() {
        let insert = Output::Count {
            command: "INSERT",
            count: 1,
        };
        assert_eq!(insert.to_string(), "INSERT 1");
        assert_eq!(Output::Done("CREATE TABLE").to_string(), "CREATE TABLE");
    }
}
