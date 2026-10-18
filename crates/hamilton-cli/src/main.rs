use std::io::{self, BufRead, Write};

const PROMPT: &str = "hamilton> ";
const CONTINUATION_PROMPT: &str = "       -> ";

fn main() -> anyhow::Result<()> {
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
            execute(&statement, &mut stdout)?;
        }
    }

    Ok(())
}

fn is_quit_command(line: &str) -> bool {
    matches!(line, "\\q" | "exit" | "quit")
}

/// Parses the statement and prints what was understood. Execution comes
/// once the binder and planner exist.
fn execute(statement: &str, out: &mut impl Write) -> io::Result<()> {
    match parser::parse(statement) {
        Ok(parsed) => {
            for stmt in parsed {
                writeln!(out, "{stmt:?}")?;
            }
        }
        Err(err) => writeln!(out, "error: {err}")?,
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
