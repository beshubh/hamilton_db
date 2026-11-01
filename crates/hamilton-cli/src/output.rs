//! Printing statement results: plain, the way `psql` does, or for a
//! terminal, in a box with color.

use std::fmt::{self, Write};
use std::time::Duration;

use nu_ansi_term::{Color, Style};

use crate::connection::{Output, OutputColumn};

impl fmt::Display for Output {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Output::Rows { columns, rows } => {
                // NULL prints as an empty cell, as in psql, so it can't be
                // mistaken for the string 'NULL'.
                let table = plain(&Grid::new(columns, rows, ""));
                write!(f, "{table}\n{}", row_count(rows.len()))
            }
            Output::Count { command, count } => write!(f, "{command} {count}"),
            Output::Done(command) => f.write_str(command),
        }
    }
}

/// Renders results and errors, in color or not.
pub struct Printer {
    pub color: bool,
    /// Whether to say how long each statement took.
    pub timing: bool,
}

impl Printer {
    pub fn output(&self, output: &Output, elapsed: Duration) -> String {
        let elapsed = Some(elapsed).filter(|_| self.timing);
        if !self.color {
            let mut text = output.to_string();
            if let Some(elapsed) = elapsed {
                write!(text, "\nTime: {:.3} ms", elapsed.as_secs_f64() * 1000.0).unwrap();
            }
            return text;
        }
        let mut text = match output {
            Output::Rows { columns, rows } => {
                let table = boxed(&Grid::new(columns, rows, "NULL"));
                format!("{table}\n{}", dim().paint(row_count(rows.len())))
            }
            Output::Count { command, count } => {
                format!(
                    "{} {} {count}",
                    check(),
                    Style::new().bold().paint(command.as_str())
                )
            }
            Output::Done(command) => format!(
                "{} {}",
                check(),
                Style::new().bold().paint(command.as_str())
            ),
        };
        if let Some(elapsed) = elapsed {
            write!(text, "{}", dim().paint(format!(" · {}", duration(elapsed)))).unwrap();
        }
        text
    }

    pub fn error(&self, message: impl fmt::Display) -> String {
        if self.color {
            format!("{} {message}", Color::Red.bold().paint("error:"))
        } else {
            format!("error: {message}")
        }
    }

    /// A line of commentary, such as "Timing is on.", dimmed in color.
    pub fn note(&self, message: &str) -> String {
        if self.color {
            dim().paint(message).to_string()
        } else {
            message.to_owned()
        }
    }

    /// A table without the row count, for describing the database.
    pub fn table(&self, columns: &[OutputColumn], rows: &[Vec<Option<String>>]) -> String {
        let grid = Grid::new(columns, rows, "");
        if self.color {
            boxed(&grid)
        } else {
            plain(&grid)
        }
    }
}

/// Rows laid out in columns: their cells as text and the width of each
/// column.
struct Grid<'a> {
    names: Vec<&'a str>,
    cells: Vec<Vec<String>>,
    /// Which cells are NULL.
    nulls: Vec<Vec<bool>>,
    widths: Vec<usize>,
    numeric: Vec<bool>,
}

impl<'a> Grid<'a> {
    fn new(columns: &'a [OutputColumn], rows: &[Vec<Option<String>>], null: &str) -> Self {
        let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        let cells: Vec<Vec<String>> = rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|value| value.as_deref().unwrap_or(null).to_owned())
                    .collect()
            })
            .collect();
        let widths = names
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
        let nulls = rows
            .iter()
            .map(|row| row.iter().map(Option::is_none).collect())
            .collect();
        let numeric = columns.iter().map(|c| c.numeric).collect();
        Self {
            names,
            cells,
            nulls,
            widths,
            numeric,
        }
    }

    /// The cells of row `row`, padded to their column's width: numbers to
    /// the right, everything else to the left.
    fn row(&self, row: usize) -> Vec<String> {
        self.cells[row]
            .iter()
            .zip(&self.numeric)
            .zip(&self.widths)
            .map(|((cell, &numeric), &width)| {
                if numeric {
                    format!("{cell:>width$}")
                } else {
                    format!("{cell:<width$}")
                }
            })
            .collect()
    }
}

/// The grid as psql prints it, with names centered over the columns.
fn plain(grid: &Grid) -> String {
    let header: Vec<String> = grid
        .names
        .iter()
        .zip(&grid.widths)
        .map(|(name, &width)| format!("{name:^width$}"))
        .collect();
    let rule: Vec<String> = grid.widths.iter().map(|&w| "-".repeat(w + 2)).collect();
    let mut lines = vec![
        format!(" {}", header.join(" | ").trim_end()),
        rule.join("+"),
    ];
    for row in 0..grid.cells.len() {
        lines.push(format!(" {}", grid.row(row).join(" | ").trim_end()));
    }
    lines.join("\n")
}

/// The grid in a box with rounded corners. NULLs are dimmed.
fn boxed(grid: &Grid) -> String {
    let border = Color::DarkGray.normal();
    let bar = border.paint("│").to_string();
    let rule = |left: &str, middle: &str, right: &str| {
        let lines: Vec<String> = grid.widths.iter().map(|&w| "─".repeat(w + 2)).collect();
        border
            .paint(format!("{left}{}{right}", lines.join(middle)))
            .to_string()
    };
    let line = |cells: Vec<String>| format!("{bar} {} {bar}", cells.join(&format!(" {bar} ")));

    let header = grid
        .names
        .iter()
        .zip(&grid.widths)
        .map(|(name, &width)| {
            Style::new()
                .bold()
                .paint(format!("{name:^width$}"))
                .to_string()
        })
        .collect();
    let mut lines = vec![rule("╭", "┬", "╮"), line(header), rule("├", "┼", "┤")];
    for (row, nulls) in grid.nulls.iter().enumerate() {
        let cells = grid
            .row(row)
            .into_iter()
            .zip(nulls)
            .map(|(cell, &null)| {
                if null {
                    dim().italic().paint(cell).to_string()
                } else {
                    cell
                }
            })
            .collect();
        lines.push(line(cells));
    }
    lines.push(rule("╰", "┴", "╯"));
    lines.join("\n")
}

fn row_count(rows: usize) -> String {
    let plural = if rows == 1 { "" } else { "s" };
    format!("({rows} row{plural})")
}

fn duration(elapsed: Duration) -> String {
    let ms = elapsed.as_secs_f64() * 1000.0;
    if ms < 1000.0 {
        format!("{ms:.2} ms")
    } else {
        format!("{:.2} s", ms / 1000.0)
    }
}

fn check() -> String {
    Color::Green.bold().paint("✓").to_string()
}

fn dim() -> Style {
    Color::DarkGray.normal()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn columns() -> Vec<OutputColumn> {
        vec![
            OutputColumn::new("id", true),
            OutputColumn::new("name", false),
        ]
    }

    fn rows() -> Vec<Vec<Option<String>>> {
        vec![
            vec![Some("1".into()), Some("ann".into())],
            vec![Some("200".into()), None],
        ]
    }

    #[test]
    fn rows_as_a_table() {
        let output = Output::Rows {
            columns: columns(),
            rows: rows(),
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
    fn rows_in_a_box() {
        let (columns, rows) = (columns(), rows());
        let boxed = boxed(&Grid::new(&columns, &rows, "NULL"));
        let plain = String::from_utf8(strip_ansi_escapes::strip(boxed)).unwrap();
        assert_eq!(
            plain,
            [
                "╭─────┬──────╮",
                "│ id  │ name │",
                "├─────┼──────┤",
                "│   1 │ ann  │",
                "│ 200 │ NULL │",
                "╰─────┴──────╯",
            ]
            .join("\n")
        );
    }

    #[test]
    fn no_rows() {
        let output = Output::Rows {
            columns: vec![OutputColumn::new("a", false)],
            rows: vec![],
        };
        assert_eq!(output.to_string(), " a\n---\n(0 rows)");
    }

    #[test]
    fn counts_and_commands() {
        let insert = Output::Count {
            command: "INSERT".into(),
            count: 1,
        };
        assert_eq!(insert.to_string(), "INSERT 1");
        assert_eq!(
            Output::Done("CREATE TABLE".into()).to_string(),
            "CREATE TABLE"
        );
    }

    #[test]
    fn plain_printer_with_timing() {
        let printer = Printer {
            color: false,
            timing: true,
        };
        let done = Output::Done("CREATE TABLE".into());
        assert_eq!(
            printer.output(&done, Duration::from_micros(1500)),
            "CREATE TABLE\nTime: 1.500 ms"
        );
    }
}
