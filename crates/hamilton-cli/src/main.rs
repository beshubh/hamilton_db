mod complete;
mod database;
mod highlight;
mod lexer;
mod meta;
mod output;
mod prompt;
mod repl;

use std::io::{self, BufRead, IsTerminal, Write};
use std::time::Instant;

use common::{Column, DataType, Value};
use database::Database;
use meta::Command;
use nu_ansi_term::{Color, Style};
use output::Printer;

/// The database file used when none is given.
const DEFAULT_DB_FILE: &str = "hamilton.db";

fn main() -> anyhow::Result<()> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_DB_FILE.to_owned());
    let db = Database::open(&path)?;
    if io::stdin().is_terminal() && io::stdout().is_terminal() {
        repl::run(&db, &path)?;
    } else {
        run_script(&db, io::stdin().lock())?;
    }
    db.close()
}

/// Runs SQL piped in, printing plain results. Statements end at `;`, except
/// that the last needs none.
fn run_script(db: &Database, input: impl BufRead) -> anyhow::Result<()> {
    let mut session = Session::new(
        db,
        Printer {
            color: false,
            timing: false,
        },
    );
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

/// Runs inputs, a command or some SQL statements at a time, against the
/// database.
pub struct Session<'a> {
    db: &'a Database,
    printer: Printer,
}

impl<'a> Session<'a> {
    pub fn new(db: &'a Database, printer: Printer) -> Self {
        Self { db, printer }
    }

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
            match self.db.execute(statement) {
                Ok(outputs) => {
                    let elapsed = start.elapsed();
                    for output in outputs {
                        writeln!(out, "{}\n", self.printer.output(&output, elapsed))?;
                    }
                }
                Err(err) => {
                    writeln!(out, "{}\n", self.printer.error(format!("{err:#}")))?;
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
            Command::ListTables => self.list_tables(),
            Command::Describe(name) => match self.describe(&name) {
                Some(text) => text,
                None => {
                    let error = self.printer.error(format!("no table named \"{name}\""));
                    writeln!(out, "{error}\n")?;
                    return Ok(Outcome::Done { failed: true });
                }
            },
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

    fn list_tables(&self) -> String {
        let tables = self.db.catalog().tables();
        if tables.is_empty() {
            return self
                .printer
                .note("No tables yet. Make one with CREATE TABLE.");
        }
        let columns = [
            text_column("table"),
            Column::new("columns", DataType::Int, false),
            text_column("indexes"),
        ];
        let rows: Vec<Vec<Value>> = tables
            .iter()
            .map(|table| {
                let indexes = self.db.catalog().table_indexes(&table.name);
                let names: Vec<&str> = indexes.iter().map(|i| i.name.as_str()).collect();
                vec![
                    Value::from(table.name.as_str()),
                    Value::Int(table.schema.columns().len() as i32),
                    Value::from(names.join(", ").as_str()),
                ]
            })
            .collect();
        self.printer.table(&columns, &rows)
    }

    /// The columns and indexes of table `name`, or `None` if there is no
    /// such table.
    fn describe(&self, name: &str) -> Option<String> {
        let catalog = self.db.catalog();
        let table = catalog.get_table(name)?;
        let columns = [
            text_column("column"),
            text_column("type"),
            text_column("nullable"),
        ];
        let rows: Vec<Vec<Value>> = table
            .schema
            .columns()
            .iter()
            .map(|column| {
                let nullable = if column.nullable { "yes" } else { "no" };
                vec![
                    Value::from(column.name.as_str()),
                    Value::from(column.data_type.to_string().as_str()),
                    Value::from(nullable),
                ]
            })
            .collect();
        let mut text = format!(
            "{}\n{}",
            self.heading(&format!("Table \"{}\"", table.name)),
            self.printer.table(&columns, &rows)
        );
        let indexes = catalog.table_indexes(name);
        if !indexes.is_empty() {
            text.push_str(&format!("\n{}", self.heading("Indexes")));
        }
        for index in indexes {
            let key: Vec<&str> = index
                .key_columns
                .iter()
                .map(|&i| table.schema.columns()[i].name.as_str())
                .collect();
            let kind = if index.is_primary_key {
                "PRIMARY KEY "
            } else {
                ""
            };
            text.push_str(&format!("\n  {} {kind}({})", index.name, key.join(", ")));
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

fn text_column(name: &str) -> Column {
    Column::new(name, DataType::Varchar(u32::MAX), false)
}
