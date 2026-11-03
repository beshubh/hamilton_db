mod complete;
mod connection;
mod highlight;
mod lexer;
mod meta;
mod output;
mod prompt;
mod repl;
mod schema;
#[cfg(test)]
#[path = "../tests/util/server.rs"]
mod test_util;

use std::io::{self, BufRead, IsTerminal, Write};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use anyhow::{Context, bail};
use connection::{Connection, OutputColumn};
use meta::Command;
use nu_ansi_term::{Color, Style};
use output::Printer;
use schema::{Schema, SharedSchema};

/// hamilton-server's default port.
const DEFAULT_PORT: u16 = 6942;

const USAGE: &str = "\
usage: hamilton-cli [-h HOST] [-p PORT]

Connects to a running hamilton-server (start one with `hamilton-server`)
and runs SQL typed in, or piped in.

  -h, --host HOST   server host (default localhost)
  -p, --port PORT   server port (default 6942)";

struct Args {
    host: String,
    port: u16,
}

fn parse_args() -> anyhow::Result<Args> {
    let mut args = Args {
        host: "localhost".to_owned(),
        port: DEFAULT_PORT,
    };
    let mut iter = std::env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            "-h" | "--host" => args.host = iter.next().context("--host needs a value")?,
            "-p" | "--port" => {
                let port = iter.next().context("--port needs a value")?;
                args.port = port.parse().with_context(|| format!("bad port {port:?}"))?;
            }
            flag if flag.starts_with('-') => bail!("unknown option {flag}\n\n{USAGE}"),
            file => bail!(
                "hamilton-cli connects to a server and doesn't open database files; \
                 serve {file} with `hamilton-server {file}`, then connect"
            ),
        }
    }
    Ok(args)
}

fn main() -> anyhow::Result<()> {
    let args = parse_args()?;
    let conn = Connection::connect(&args.host, args.port).with_context(|| {
        format!(
            "could not connect to hamilton-server at {}:{}; is it running?",
            args.host, args.port
        )
    })?;
    if io::stdin().is_terminal() && io::stdout().is_terminal() {
        let printer = Printer {
            color: true,
            timing: true,
        };
        repl::run(Session::new(conn, printer))?;
    } else {
        run_script(io::stdin().lock(), conn)?;
    }
    Ok(())
}

/// Runs SQL piped in, printing plain results. Statements end at `;`, except
/// that the last needs none.
fn run_script(input: impl BufRead, conn: Connection) -> anyhow::Result<()> {
    let printer = Printer {
        color: false,
        timing: false,
    };
    let mut session = Session::new(conn, printer);
    let mut out = io::stdout();
    let mut pending = String::new();
    for line in input.lines() {
        let line = line?;
        if pending.trim().is_empty() && meta::parse(&line).is_some() {
            if let Outcome::Quit = session.run(&line, &mut out)? {
                return Ok(());
            }
            continue;
        }
        pending.push_str(&line);
        pending.push('\n');
        if lexer::ends_statement(&pending) {
            session.run(&pending, &mut out)?;
            pending.clear();
        }
    }
    session.run(&pending, &mut out)?;
    Ok(())
}

/// What to do after an input has run.
pub enum Outcome {
    Done { failed: bool },
    Clear,
    Quit,
}

/// Runs inputs, a command or some SQL statements at a time, on the server.
pub struct Session {
    conn: Connection,
    printer: Printer,
    /// What completion and highlighting know of the server's tables.
    schema: SharedSchema,
}

impl Session {
    pub fn new(conn: Connection, printer: Printer) -> Self {
        Self {
            conn,
            printer,
            schema: Arc::new(RwLock::new(Schema::default())),
        }
    }

    pub fn server(&self) -> &str {
        self.conn.addr()
    }

    pub fn schema(&self) -> &SharedSchema {
        &self.schema
    }

    /// Asks the server for its tables again, since any statement, this
    /// client's or another's, may have made one.
    pub fn refresh_schema(&mut self) -> io::Result<()> {
        let schema = self.conn.schema()?;
        *self.schema.write().unwrap_or_else(|e| e.into_inner()) = schema;
        Ok(())
    }

    /// Runs `input`. Fails if writing to `out` does, or the connection to
    /// the server does.
    pub fn run(&mut self, input: &str, out: &mut impl Write) -> io::Result<Outcome> {
        match meta::parse(input) {
            Some(command) => self.command(command, out),
            None => self.sql(input, out),
        }
    }

    /// Runs each statement in `input` and prints its result, or the error
    /// that stopped it. A failed statement doesn't stop the ones after it.
    fn sql(&mut self, input: &str, out: &mut impl Write) -> io::Result<Outcome> {
        let mut failed = false;
        for statement in lexer::split_statements(input) {
            let start = Instant::now();
            match self.conn.execute(statement)? {
                Ok(outputs) => {
                    let elapsed = start.elapsed();
                    for output in outputs {
                        writeln!(out, "{}\n", self.printer.output(&output, elapsed))?;
                    }
                }
                Err(err) => {
                    writeln!(out, "{}\n", self.printer.error(&err.message))?;
                    failed = true;
                }
            }
        }
        Ok(Outcome::Done { failed })
    }

    fn command(&mut self, command: Command, out: &mut impl Write) -> io::Result<Outcome> {
        let text = match command {
            Command::Quit => return Ok(Outcome::Quit),
            Command::Clear => return Ok(Outcome::Clear),
            Command::Help => self.help(),
            Command::ListTables => {
                let schema = self.conn.schema()?;
                self.list_tables(&schema)
            }
            Command::Describe(name) => {
                let schema = self.conn.schema()?;
                match self.describe(&schema, &name) {
                    Some(text) => text,
                    None => {
                        let error = self.printer.error(format!("no table named \"{name}\""));
                        writeln!(out, "{error}\n")?;
                        return Ok(Outcome::Done { failed: true });
                    }
                }
            }
            Command::Timing => {
                self.printer.timing = !self.printer.timing;
                let state = if self.printer.timing { "on" } else { "off" };
                self.printer.note(&format!("Timing is {state}."))
            }
            Command::Unknown(command) => {
                let error = self
                    .printer
                    .error(format!("unknown command {command}; try \\?"));
                writeln!(out, "{error}\n")?;
                return Ok(Outcome::Done { failed: true });
            }
        };
        writeln!(out, "{text}\n")?;
        Ok(Outcome::Done { failed: false })
    }

    fn list_tables(&self, schema: &Schema) -> String {
        if schema.tables.is_empty() {
            return self
                .printer
                .note("No tables yet. Make one with CREATE TABLE.");
        }
        let columns = [
            OutputColumn::new("table", false),
            OutputColumn::new("columns", true),
            OutputColumn::new("indexes", false),
        ];
        let rows: Vec<Vec<Option<String>>> = schema
            .tables
            .iter()
            .map(|table| {
                let names: Vec<&str> = table.indexes.iter().map(|i| i.name.as_str()).collect();
                vec![
                    Some(table.name.clone()),
                    Some(table.columns.len().to_string()),
                    Some(names.join(", ")),
                ]
            })
            .collect();
        self.printer.table(&columns, &rows)
    }

    /// The columns and indexes of table `name`, or `None` if there is no
    /// such table.
    fn describe(&self, schema: &Schema, name: &str) -> Option<String> {
        let table = schema.table(name)?;
        let columns = [
            OutputColumn::new("column", false),
            OutputColumn::new("type", false),
            OutputColumn::new("nullable", false),
        ];
        let rows: Vec<Vec<Option<String>>> = table
            .columns
            .iter()
            .map(|column| {
                let nullable = if column.nullable { "yes" } else { "no" };
                vec![
                    Some(column.name.clone()),
                    Some(column.type_name.clone()),
                    Some(nullable.to_owned()),
                ]
            })
            .collect();
        let mut text = format!(
            "{}\n{}",
            self.heading(&format!("Table \"{}\"", table.name)),
            self.printer.table(&columns, &rows)
        );
        if !table.indexes.is_empty() {
            text.push_str(&format!("\n{}", self.heading("Indexes")));
        }
        for index in &table.indexes {
            let kind = if index.kind.is_empty() {
                String::new()
            } else {
                format!("{} ", index.kind)
            };
            text.push_str(&format!("\n  {} {kind}({})", index.name, index.columns));
        }
        Some(text)
    }

    fn help(&self) -> String {
        let commands: Vec<String> = meta::COMMANDS
            .iter()
            .map(|(command, about)| {
                let usage = match *command {
                    "\\d" => "\\d [NAME]",
                    command => command,
                };
                format!("  {}{about}", self.key(usage))
            })
            .collect();
        let keys = [
            ("Tab", "complete keywords, tables and columns"),
            ("↑ ↓", "walk through history"),
            ("Ctrl-R", "search history"),
            ("→", "accept the grey suggestion"),
            ("Alt-Enter", "new line without running"),
            ("Ctrl-L", "clear the screen"),
            ("Ctrl-C", "drop what you've typed"),
            ("Ctrl-D", "quit"),
        ]
        .map(|(key, about)| format!("  {}{about}", self.key(key)));
        [
            self.heading("SQL"),
            "  CREATE TABLE, CREATE INDEX, INSERT, SELECT, UPDATE and DELETE.".to_owned(),
            "  A statement runs when it ends with ;, so it can span lines.".to_owned(),
            String::new(),
            self.heading("Commands"),
            commands.join("\n"),
            String::new(),
            self.heading("Keys"),
            keys.join("\n"),
        ]
        .join("\n")
    }

    fn heading(&self, text: &str) -> String {
        if self.printer.color {
            Color::Cyan.bold().paint(text).to_string()
        } else {
            text.to_owned()
        }
    }

    /// `key`, bold in color. Padded first, since escapes would throw off the
    /// width.
    fn key(&self, key: &str) -> String {
        let padded = format!("{key:<12}");
        if self.printer.color {
            Style::new().bold().paint(padded).to_string()
        } else {
            padded
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::TestServer;

    /// What running each input in turn prints, without color or timing.
    fn printed(server: &TestServer, inputs: &[&str]) -> String {
        let conn = Connection::connect("127.0.0.1", server.port).unwrap();
        let printer = Printer {
            color: false,
            timing: false,
        };
        let mut session = Session::new(conn, printer);
        let mut out = Vec::new();
        for input in inputs {
            session.run(input, &mut out).unwrap();
        }
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn prints_results_from_the_server() {
        let server = TestServer::start();
        let out = printed(
            &server,
            &[
                "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(8), ok BOOL);",
                "INSERT INTO t VALUES (1, 'a', true), (20, NULL, false);",
                "SELECT * FROM t; SELECT 1 / 0; UPDATE t SET ok = true;",
            ],
        );
        let rows = [
            " id | name |  ok",
            "----+------+-------",
            "  1 | a    | true",
            " 20 |      | false",
            "(2 rows)",
        ]
        .join("\n");
        let expected = [
            "CREATE TABLE",
            "INSERT 2",
            &rows,
            "error: division by zero",
            "UPDATE 2",
        ];
        assert_eq!(out, expected.map(|block| format!("{block}\n\n")).concat());
    }

    #[test]
    fn describes_tables_from_the_server() {
        let server = TestServer::start();
        let out = printed(
            &server,
            &[
                "\\d",
                "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(16) UNIQUE);",
                "CREATE INDEX users_name ON users (name);",
                "\\d",
                "\\d users",
                "\\d nope",
            ],
        );
        let blocks: Vec<&str> = out.trim_end().split("\n\n").collect();
        assert_eq!(blocks[0], "No tables yet. Make one with CREATE TABLE.");
        assert_eq!(
            blocks[3],
            [
                " table | columns |                indexes",
                "-------+---------+----------------------------------------",
                " users |       2 | users_pkey, users_name_key, users_name",
            ]
            .join("\n")
        );
        assert_eq!(
            blocks[4],
            [
                "Table \"users\"",
                " column |    type     | nullable",
                "--------+-------------+----------",
                " id     | INT         | no",
                " name   | VARCHAR(16) | yes",
                "Indexes",
                "  users_pkey PRIMARY KEY (id)",
                "  users_name_key UNIQUE (name)",
                "  users_name (name)",
            ]
            .join("\n")
        );
        assert_eq!(blocks[5], "error: no table named \"nope\"");
    }

    #[test]
    fn the_schema_cache_follows_the_server() {
        let server = TestServer::start();
        let conn = Connection::connect("127.0.0.1", server.port).unwrap();
        let mut session = Session::new(
            conn,
            Printer {
                color: false,
                timing: false,
            },
        );
        printed(&server, &["CREATE TABLE made_elsewhere (id INT);"]);
        session.refresh_schema().unwrap();
        let schema = session.schema().read().unwrap();
        assert!(schema.table("made_elsewhere").is_some());
    }
}
