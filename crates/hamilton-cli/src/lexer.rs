//! Splits SQL into tokens for the editor: highlighting, completion, and
//! deciding when the input holds whole statements.
//!
//! The lexer never fails. Input that is still being typed (an open string, a
//! half-written comment) lexes too, with the unfinished token marked as such.
//! The parser does the real work once a statement is complete.

/// Keywords the parser understands, uppercase and sorted.
pub const KEYWORDS: &[&str] = &[
    "AND",
    "ARRAY",
    "AS",
    "ASC",
    "BIGINT",
    "BOOL",
    "BOOLEAN",
    "BY",
    "CREATE",
    "CROSS",
    "DELETE",
    "DESC",
    "EXISTS",
    "EXPLAIN",
    "FALSE",
    "FROM",
    "GROUP",
    "HAVING",
    "IN",
    "INDEX",
    "INNER",
    "INSERT",
    "INT",
    "INTEGER",
    "INTO",
    "IS",
    "JOIN",
    "KEY",
    "LEFT",
    "LIMIT",
    "NOT",
    "NULL",
    "ON",
    "OR",
    "ORDER",
    "OUTER",
    "OVER",
    "PARTITION",
    "PRIMARY",
    "SELECT",
    "SET",
    "TABLE",
    "TRUE",
    "UPDATE",
    "VALUES",
    "VARCHAR",
    "WHERE",
];

/// Keywords that stand for a value rather than shape the statement.
pub const CONSTANTS: &[&str] = &["FALSE", "NULL", "TRUE"];

/// Functions the binder knows, with what kind each is.
pub const FUNCTIONS: &[(&str, &str)] = &[
    ("count", "aggregate"),
    ("lower", "function"),
    ("max", "aggregate"),
    ("min", "aggregate"),
    ("rank", "window function"),
    ("row_number", "window function"),
    ("sum", "aggregate"),
    ("upper", "function"),
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenKind {
    Whitespace,
    /// `-- ...` up to the end of the line, or `/* ... */`.
    Comment,
    /// One of [`KEYWORDS`], in any case.
    Keyword,
    Identifier,
    /// `"name"`
    QuotedIdentifier,
    Number,
    /// `'...'`, with `''` standing for a quote.
    String,
    Operator,
    /// `( ) [ ] , . ;`
    Punctuation,
    /// A character SQL has no use for.
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Token<'a> {
    pub kind: TokenKind,
    pub text: &'a str,
    /// Byte offset of the token in the input.
    pub start: usize,
    /// False for a string, quoted identifier or block comment that the input
    /// ends inside of.
    pub terminated: bool,
}

impl Token<'_> {
    pub fn end(&self) -> usize {
        self.start + self.text.len()
    }

    /// Whitespace and finished comments, which mean nothing to the parser.
    pub fn is_trivia(&self) -> bool {
        match self.kind {
            TokenKind::Whitespace => true,
            TokenKind::Comment => self.terminated,
            _ => false,
        }
    }

    pub fn is_keyword(&self, keyword: &str) -> bool {
        self.kind == TokenKind::Keyword && self.text.eq_ignore_ascii_case(keyword)
    }

    pub fn is_punct(&self, punct: &str) -> bool {
        self.kind == TokenKind::Punctuation && self.text == punct
    }
}

pub fn tokenize(sql: &str) -> Vec<Token<'_>> {
    let mut tokens = Vec::new();
    let mut start = 0;
    while let Some(c) = sql[start..].chars().next() {
        let rest = &sql[start..];
        let (kind, len, terminated) = match c {
            c if c.is_whitespace() => (
                TokenKind::Whitespace,
                prefix_len(rest, char::is_whitespace),
                true,
            ),
            '-' if rest.starts_with("--") => (
                TokenKind::Comment,
                rest.find('\n').unwrap_or(rest.len()),
                true,
            ),
            '/' if rest.starts_with("/*") => match rest[2..].find("*/") {
                Some(end) => (TokenKind::Comment, end + 4, true),
                None => (TokenKind::Comment, rest.len(), false),
            },
            '\'' => quoted(rest, TokenKind::String),
            '"' => quoted(rest, TokenKind::QuotedIdentifier),
            '0'..='9' => (
                TokenKind::Number,
                prefix_len(rest, |c| c.is_ascii_alphanumeric() || c == '.' || c == '_'),
                true,
            ),
            c if is_word_start(c) => {
                let len = prefix_len(rest, is_word_char);
                let kind = if is_keyword(&rest[..len]) {
                    TokenKind::Keyword
                } else {
                    TokenKind::Identifier
                };
                (kind, len, true)
            }
            '(' | ')' | '[' | ']' | ',' | '.' | ';' => (TokenKind::Punctuation, 1, true),
            c if is_operator_char(c) => (
                TokenKind::Operator,
                prefix_len(rest, is_operator_char),
                true,
            ),
            c => (TokenKind::Unknown, c.len_utf8(), true),
        };
        tokens.push(Token {
            kind,
            text: &rest[..len],
            start,
            terminated,
        });
        start += len;
    }
    tokens
}

/// Whether `sql` ends with a `;` outside any string or comment, so it can be
/// run. Comments after the `;` are fine.
pub fn ends_statement(sql: &str) -> bool {
    tokenize(sql)
        .iter()
        .rev()
        .find(|token| !token.is_trivia())
        .is_some_and(|token| token.is_punct(";"))
}

/// The statements in `sql`, split at semicolons and trimmed. Empty
/// statements are dropped; the last statement needs no `;`.
pub fn split_statements(sql: &str) -> Vec<&str> {
    let mut statements = Vec::new();
    let (mut start, mut has_code) = (0, false);
    for token in tokenize(sql) {
        if token.is_punct(";") {
            if has_code {
                statements.push(sql[start..token.start].trim());
            }
            (start, has_code) = (token.end(), false);
        } else if !token.is_trivia() {
            has_code = true;
        }
    }
    if has_code {
        statements.push(sql[start..].trim());
    }
    statements
}

pub fn is_keyword(word: &str) -> bool {
    KEYWORDS
        .binary_search_by(|keyword| cmp_ignore_case(keyword, word))
        .is_ok()
}

pub fn is_word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn is_word_start(c: char) -> bool {
    c.is_alphabetic() || c == '_'
}

fn is_operator_char(c: char) -> bool {
    matches!(
        c,
        '+' | '-' | '*' | '/' | '%' | '<' | '>' | '=' | '!' | '|' | '&' | '^' | '~' | ':'
    )
}

fn prefix_len(s: &str, pred: impl Fn(char) -> bool) -> usize {
    s.find(|c| !pred(c)).unwrap_or(s.len())
}

/// Lexes a token that runs to the next lone `quote`, the first character of
/// `s`. A doubled quote stands for one and doesn't end the token.
fn quoted(s: &str, kind: TokenKind) -> (TokenKind, usize, bool) {
    let quote = s.as_bytes()[0];
    let bytes = s.as_bytes();
    let mut i = 1;
    while i < bytes.len() {
        if bytes[i] == quote {
            if bytes.get(i + 1) == Some(&quote) {
                i += 2;
                continue;
            }
            return (kind, i + 1, true);
        }
        i += 1;
    }
    (kind, s.len(), false)
}

fn cmp_ignore_case(a: &str, b: &str) -> std::cmp::Ordering {
    a.bytes()
        .map(|b| b.to_ascii_uppercase())
        .cmp(b.bytes().map(|b| b.to_ascii_uppercase()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(sql: &str) -> Vec<(TokenKind, &str)> {
        tokenize(sql)
            .into_iter()
            .filter(|t| t.kind != TokenKind::Whitespace)
            .map(|t| (t.kind, t.text))
            .collect()
    }

    #[test]
    fn keywords_are_sorted() {
        assert!(KEYWORDS.is_sorted());
        assert!(FUNCTIONS.is_sorted());
    }

    #[test]
    fn tokens_cover_the_input() {
        let sql = "SELECT a, 'it''s' -- c\n FROM t WHERE x >= 1.5;";
        let text: String = tokenize(sql).iter().map(|t| t.text).collect();
        assert_eq!(text, sql);
        use TokenKind::{Comment, Identifier, Keyword, Number, Operator, Punctuation};
        assert_eq!(
            kinds(sql),
            [
                (Keyword, "SELECT"),
                (Identifier, "a"),
                (Punctuation, ","),
                (TokenKind::String, "'it''s'"),
                (Comment, "-- c"),
                (Keyword, "FROM"),
                (Identifier, "t"),
                (Keyword, "WHERE"),
                (Identifier, "x"),
                (Operator, ">="),
                (Number, "1.5"),
                (Punctuation, ";"),
            ]
        );
    }

    #[test]
    fn keywords_in_any_case() {
        assert_eq!(kinds("select Users")[0], (TokenKind::Keyword, "select"));
        assert_eq!(kinds("select Users")[1], (TokenKind::Identifier, "Users"));
    }

    #[test]
    fn unfinished_tokens() {
        let tokens = tokenize("SELECT 'abc");
        assert_eq!(tokens[2].kind, TokenKind::String);
        assert!(!tokens[2].terminated);
        let tokens = tokenize("/* note");
        assert_eq!(tokens[0].kind, TokenKind::Comment);
        assert!(!tokens[0].terminated && !tokens[0].is_trivia());
    }

    #[test]
    fn ends_statement_ignores_quoted_semicolons() {
        assert!(ends_statement("SELECT 1;"));
        assert!(ends_statement("SELECT 1;  -- done\n"));
        assert!(!ends_statement("SELECT 1"));
        assert!(!ends_statement("SELECT ';"));
        assert!(!ends_statement("SELECT 1; /* open"));
        assert!(!ends_statement("SELECT 1; SELECT"));
        assert!(!ends_statement(""));
    }

    #[test]
    fn single_statement() {
        assert_eq!(split_statements("SELECT * FROM t;\n"), ["SELECT * FROM t"]);
    }

    #[test]
    fn statement_spanning_lines() {
        assert_eq!(
            split_statements("CREATE TABLE t (\n  id INT);\n"),
            ["CREATE TABLE t (\n  id INT)"]
        );
    }

    #[test]
    fn multiple_statements_on_one_line() {
        assert_eq!(
            split_statements("INSERT INTO t VALUES (1); SELECT * FROM t;\n"),
            ["INSERT INTO t VALUES (1)", "SELECT * FROM t"]
        );
    }

    #[test]
    fn semicolons_inside_strings_and_comments() {
        assert_eq!(
            split_statements("INSERT INTO t VALUES (1, 'a;b''c'); -- x; y\n"),
            ["INSERT INTO t VALUES (1, 'a;b''c')"]
        );
    }

    #[test]
    fn empty_statements_are_skipped() {
        assert!(split_statements(";;\n").is_empty());
        assert!(split_statements("-- only a comment;\n").is_empty());
    }

    #[test]
    fn last_statement_needs_no_semicolon() {
        assert_eq!(
            split_statements("SELECT 1; SELECT 2"),
            ["SELECT 1", "SELECT 2"]
        );
    }
}
