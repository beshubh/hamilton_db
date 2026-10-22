mod database;
mod output;

use std::io::{self, BufRead, Write};

use database::Database;

const PROMPT: &str = "hamilton> ";
const CONTINUATION_PROMPT: &str = "       -> ";

/// The database file used when none is given.
const DEFAULT_DB_FILE: &str = "hamilton.db";

fn main() -> anyhow::Result<()> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_DB_FILE.to_owned());
    let db = Database::open(&path)?;
    println!("hamilton_db: {path} (\\q to quit)");

    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut buffer = StatementBuffer::default();

    loop {
        let prompt = if buffer.is_empty() {
            PROMPT
        } else {
            CONTINUATION_PROMPT
        };
        write!(stdout, "{prompt}")?;
        stdout.flush()?;

        let mut line = String::new();
        if stdin.lock().read_line(&mut line)? == 0 {
            // EOF (Ctrl-D).
            writeln!(stdout)?;
            break;
        }

        if buffer.is_empty() && is_quit_command(line.trim()) {
            break;
        }

        for statement in buffer.push_line(&line) {
            execute(&db, &statement, &mut stdout)?;
        }
    }

    db.close()
}

fn is_quit_command(line: &str) -> bool {
    matches!(line, "\\q" | "exit" | "quit")
}

/// Runs the statement and prints its result, or the error that stopped it.
fn execute(db: &Database, statement: &str, out: &mut impl Write) -> io::Result<()> {
    match db.execute(statement) {
        Ok(outputs) => {
            for output in outputs {
                writeln!(out, "{output}\n")?;
            }
        }
        Err(err) => writeln!(out, "error: {err:#}\n")?,
    }
    Ok(())
}

/// Accumulates input lines and splits them into `;`-terminated statements,
/// ignoring semicolons inside single-quoted string literals.
#[derive(Default)]
struct StatementBuffer {
    pending: String,
    in_string: bool,
}

impl StatementBuffer {
    fn is_empty(&self) -> bool {
        self.pending.trim().is_empty()
    }

    fn push_line(&mut self, line: &str) -> Vec<String> {
        let mut statements = Vec::new();
        for c in line.chars() {
            match c {
                '\'' => {
                    // A doubled quote ('') toggles twice, so escapes need no special case.
                    self.in_string = !self.in_string;
                    self.pending.push(c);
                }
                ';' if !self.in_string => {
                    let statement = self.pending.trim();
                    if !statement.is_empty() {
                        statements.push(statement.to_string());
                    }
                    self.pending.clear();
                }
                _ => self.pending.push(c),
            }
        }
        statements
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_statement() {
        let mut buf = StatementBuffer::default();
        assert_eq!(buf.push_line("SELECT * FROM t;\n"), vec!["SELECT * FROM t"]);
        assert!(buf.is_empty());
    }

    #[test]
    fn statement_spanning_lines() {
        let mut buf = StatementBuffer::default();
        assert!(buf.push_line("CREATE TABLE t (\n").is_empty());
        assert!(!buf.is_empty());
        assert_eq!(
            buf.push_line("  id INT);\n"),
            vec!["CREATE TABLE t (\n  id INT)"]
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn multiple_statements_on_one_line() {
        let mut buf = StatementBuffer::default();
        assert_eq!(
            buf.push_line("INSERT INTO t VALUES (1); SELECT * FROM t;\n"),
            vec!["INSERT INTO t VALUES (1)", "SELECT * FROM t"]
        );
    }

    #[test]
    fn semicolon_inside_string_literal() {
        let mut buf = StatementBuffer::default();
        assert_eq!(
            buf.push_line("INSERT INTO t VALUES (1, 'a;b''c');\n"),
            vec!["INSERT INTO t VALUES (1, 'a;b''c')"]
        );
    }

    #[test]
    fn empty_statements_are_skipped() {
        let mut buf = StatementBuffer::default();
        assert!(buf.push_line(";;\n").is_empty());
        assert!(buf.is_empty());
    }
}
