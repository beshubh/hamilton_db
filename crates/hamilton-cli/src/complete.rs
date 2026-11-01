//! Tab completion of keywords, functions, backslash commands, and the
//! tables and columns in the catalog.
//!
//! Where the cursor is decides what is offered: table names after FROM,
//! JOIN, INTO and UPDATE; the columns of a table after `name.`; otherwise
//! the columns of the tables the statement mentions, then functions, tables
//! and keywords.

use std::sync::Arc;

use catalog::Catalog;
use common::Column;
use reedline::{Completer, CompletionResult, Span, Suggestion};

use crate::lexer::{FUNCTIONS, KEYWORDS, Token, TokenKind, is_word_char, tokenize};
use crate::meta;

pub struct SqlCompleter {
    catalog: Arc<Catalog>,
}

impl SqlCompleter {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self { catalog }
    }
}

impl Completer for SqlCompleter {
    fn complete(&mut self, line: &str, pos: usize) -> CompletionResult {
        let infos = self.catalog.tables();
        let tables: Vec<Table> = infos
            .iter()
            .map(|t| Table {
                name: &t.name,
                columns: t.schema.columns(),
            })
            .collect();
        CompletionResult::fresh(suggest(&tables, line, pos))
    }
}

/// A table as completion sees it.
pub struct Table<'a> {
    pub name: &'a str,
    pub columns: &'a [Column],
}

/// What may go where the cursor is.
#[derive(Debug, PartialEq, Eq)]
enum Context {
    /// A table name.
    Table,
    /// A name for something new, which nothing can complete.
    NewName,
    Anything,
}

/// The completions for the word that ends at `pos` in `line`.
pub fn suggest(tables: &[Table], line: &str, pos: usize) -> Vec<Suggestion> {
    let before = &line[..pos];
    let start = pos
        - before
            .chars()
            .rev()
            .take_while(|&c| is_word_char(c))
            .map(char::len_utf8)
            .sum::<usize>();
    let word = &line[start..pos];
    let mut out = Suggestions {
        word,
        span: Span::new(start, pos),
        list: Vec::new(),
    };

    if let Some((command, args)) = meta::split(before) {
        if args.is_empty() {
            let typed = command.trim_start();
            let span = Span::new(pos - typed.len(), pos);
            for &(name, about) in meta::COMMANDS {
                if name.starts_with(typed) {
                    out.list.push(suggestion(name, about, span, true));
                }
            }
        } else if matches!(command.trim_start(), "\\d" | "\\dt") {
            out.tables(tables);
        }
        return out.list;
    }

    let tokens = tokenize(line);
    if tokens.iter().any(|t| encloses(t, pos)) {
        return Vec::new();
    }
    let statement = statement_around(&tokens, start);
    let prev: Vec<&Token> = statement
        .iter()
        .filter(|t| t.end() <= start)
        .copied()
        .collect();

    // `name.` asks for the columns of the table called or aliased `name`.
    if let [.., qualifier, dot] = prev.as_slice()
        && dot.is_punct(".")
        && dot.end() == start
    {
        if let Some(table) = resolve(tables, &statement, qualifier.text) {
            out.columns(&[table]);
        }
        return out.list;
    }

    match context(&prev) {
        Context::Table => out.tables(tables),
        Context::NewName => {}
        Context::Anything => {
            let mentioned: Vec<&Table> = tables
                .iter()
                .filter(|table| statement.iter().any(|t| names(t, table.name)))
                .collect();
            if mentioned.is_empty() {
                out.columns(&tables.iter().collect::<Vec<_>>());
            } else {
                out.columns(&mentioned);
            }
            out.functions();
            out.tables(tables);
            out.keywords();
        }
    }
    out.list
}

/// Collects the suggestions that complete `word`.
struct Suggestions<'a> {
    word: &'a str,
    span: Span,
    list: Vec<Suggestion>,
}

impl Suggestions<'_> {
    fn matches(&self, candidate: &str) -> bool {
        let len = self.word.len();
        candidate.len() >= len
            && candidate.is_char_boundary(len)
            && candidate[..len].eq_ignore_ascii_case(self.word)
            && !self.list.iter().any(|s| s.value == candidate)
    }

    fn push(&mut self, value: &str, description: String, space: bool) {
        if self.matches(value) {
            let suggestion = suggestion(value, &description, self.span, space);
            self.list.push(suggestion);
        }
    }

    fn tables(&mut self, tables: &[Table]) {
        for table in tables {
            let count = table.columns.len();
            let plural = if count == 1 { "" } else { "s" };
            self.push(table.name, format!("table · {count} column{plural}"), true);
        }
    }

    fn columns(&mut self, tables: &[&Table]) {
        for table in tables {
            for column in table.columns {
                let null = if column.nullable { "" } else { " NOT NULL" };
                let about = format!("{}{null} · {}", column.data_type, table.name);
                self.push(&column.name, about, false);
            }
        }
    }

    fn functions(&mut self) {
        for &(name, kind) in FUNCTIONS {
            if self.matches(name) {
                let mut suggestion = suggestion(&format!("{name}("), kind, self.span, false);
                suggestion.display_override = Some(format!("{name}()"));
                self.list.push(suggestion);
            }
        }
    }

    fn keywords(&mut self) {
        // Typing in lowercase gets lowercase keywords.
        let lower = self.word.chars().any(|c| c.is_lowercase())
            && !self.word.chars().any(|c| c.is_uppercase());
        for keyword in KEYWORDS {
            let keyword = if lower {
                keyword.to_lowercase()
            } else {
                keyword.to_string()
            };
            self.push(&keyword, "keyword".to_owned(), true);
        }
    }
}

fn suggestion(value: &str, description: &str, span: Span, space: bool) -> Suggestion {
    Suggestion {
        value: value.to_owned(),
        description: Some(description.to_owned()),
        span,
        append_whitespace: space,
        ..Suggestion::default()
    }
}

/// Whether `pos` is inside a string, quoted name or comment, where there is
/// nothing to complete.
fn encloses(token: &Token, pos: usize) -> bool {
    let quoted = matches!(
        token.kind,
        TokenKind::String | TokenKind::QuotedIdentifier | TokenKind::Comment
    );
    // A `--` comment runs to the end of the line, so its end is still in it.
    let open = !token.terminated || token.text.starts_with("--");
    quoted && token.start < pos && (pos < token.end() || open)
}

/// The tokens, other than whitespace and comments, of the statement that
/// the word starting at `start` belongs to.
fn statement_around<'a, 't>(tokens: &'t [Token<'a>], start: usize) -> Vec<&'t Token<'a>> {
    let semicolons = || tokens.iter().filter(|t| t.is_punct(";"));
    let from = semicolons()
        .rev()
        .find(|t| t.end() <= start)
        .map_or(0, Token::end);
    let to = semicolons()
        .find(|t| t.start >= start)
        .map_or(usize::MAX, |t| t.start);
    tokens
        .iter()
        .filter(|t| t.start >= from && t.end() <= to && !t.is_trivia())
        .collect()
}

fn context(prev: &[&Token]) -> Context {
    let Some(last) = prev.last() else {
        return Context::Anything;
    };
    if ["FROM", "JOIN", "INTO", "UPDATE"]
        .iter()
        .any(|k| last.is_keyword(k))
    {
        return Context::Table;
    }
    let create = prev[0].is_keyword("CREATE");
    // CREATE INDEX name ON _
    if create && prev.len() == 4 && prev[1].is_keyword("INDEX") && last.is_keyword("ON") {
        return Context::Table;
    }
    // CREATE TABLE _, CREATE INDEX _
    if create && prev.len() == 2 {
        return Context::NewName;
    }
    // FROM a, _
    if last.is_punct(",")
        && prev
            .iter()
            .rev()
            .find(|t| t.kind == TokenKind::Keyword)
            .is_some_and(|t| t.is_keyword("FROM"))
    {
        return Context::Table;
    }
    Context::Anything
}

/// Whether the token is the name `name`.
fn names(token: &Token, name: &str) -> bool {
    token.kind == TokenKind::Identifier && token.text.eq_ignore_ascii_case(name)
}

/// The table that `name` refers to in the statement: a table of that name,
/// or one the statement aliases as `name`, as in `FROM users u` or
/// `FROM users AS u`.
fn resolve<'a, 'b>(
    tables: &'b [Table<'a>],
    statement: &[&Token],
    name: &str,
) -> Option<&'b Table<'a>> {
    let table = |token: &Token| tables.iter().find(|t| names(token, t.name));
    if let Some(table) = tables.iter().find(|t| t.name.eq_ignore_ascii_case(name)) {
        return Some(table);
    }
    statement.iter().enumerate().find_map(|(i, token)| {
        let alias = match statement.get(i + 1) {
            Some(next) if next.is_keyword("AS") => statement.get(i + 2),
            next => next,
        };
        table(token).filter(|_| alias.is_some_and(|alias| names(alias, name)))
    })
}

#[cfg(test)]
mod tests {
    use common::DataType;

    use super::*;

    fn complete(line: &str) -> Vec<String> {
        let users = [
            Column::new("id", DataType::Int, false),
            Column::new("name", DataType::Varchar(32), true),
        ];
        let orders = [
            Column::new("id", DataType::Int, false),
            Column::new("user_id", DataType::Int, false),
        ];
        let tables = [
            Table {
                name: "orders",
                columns: &orders,
            },
            Table {
                name: "users",
                columns: &users,
            },
        ];
        // The cursor is at `|`, or at the end.
        let pos = line.find('|').unwrap_or(line.len());
        let line = line.replace('|', "");
        suggest(&tables, &line, pos)
            .into_iter()
            .map(|s| s.value)
            .collect()
    }

    #[test]
    fn keywords_in_the_case_typed() {
        assert_eq!(complete("SEL"), ["SELECT"]);
        assert_eq!(complete("sel"), ["select"]);
        assert_eq!(complete("SELECT 1 FR"), ["FROM"]);
    }

    #[test]
    fn tables_after_from_and_friends() {
        assert_eq!(complete("SELECT * FROM "), ["orders", "users"]);
        assert_eq!(complete("SELECT * FROM u"), ["users"]);
        assert_eq!(complete("SELECT * FROM orders, u"), ["users"]);
        assert_eq!(complete("SELECT * FROM orders o JOIN us"), ["users"]);
        assert_eq!(complete("INSERT INTO o"), ["orders"]);
        assert_eq!(complete("UPDATE "), ["orders", "users"]);
        assert_eq!(complete("CREATE INDEX i ON u"), ["users"]);
        assert_eq!(complete("\\d u"), ["users"]);
    }

    #[test]
    fn nothing_for_new_names_or_inside_strings() {
        assert!(complete("CREATE TABLE u").is_empty());
        assert!(complete("SELECT 'u").is_empty());
        assert!(complete("SELECT 'u|'").is_empty());
        assert!(complete("SELECT 1 -- u").is_empty());
    }

    #[test]
    fn columns_of_the_tables_in_the_statement() {
        assert_eq!(complete("SELECT na| FROM users"), ["name"]);
        // `user_id` is in orders, which this statement doesn't mention.
        assert_eq!(
            complete("SELECT * FROM users WHERE u"),
            ["upper(", "users", "update"]
        );
        // With no table mentioned, every table's columns.
        assert_eq!(
            complete("SELECT u"),
            ["user_id", "upper(", "users", "update"]
        );
        // Only the statement at the cursor counts.
        assert_eq!(
            complete("SELECT * FROM orders; SELECT u| FROM users"),
            ["upper(", "users", "update"]
        );
    }

    #[test]
    fn qualified_columns() {
        assert_eq!(complete("SELECT users."), ["id", "name"]);
        assert_eq!(complete("SELECT o.u| FROM orders o"), ["user_id"]);
        assert_eq!(complete("SELECT x.| FROM users AS x"), ["id", "name"]);
        assert!(complete("SELECT nope.").is_empty());
    }

    #[test]
    fn backslash_commands() {
        assert_eq!(
            complete("\\"),
            ["\\?", "\\clear", "\\d", "\\q", "\\session", "\\timing"]
        );
        assert_eq!(complete("\\t"), ["\\timing"]);
    }
}
