//! Colors the SQL being typed, and the bracket matching the one at the
//! cursor.

use std::sync::Arc;

use catalog::Catalog;
use nu_ansi_term::{Color, Style};
use reedline::{Highlighter, StyledText};

use crate::lexer::{CONSTANTS, Token, TokenKind, tokenize};
use crate::meta;

pub struct SqlHighlighter {
    catalog: Arc<Catalog>,
}

impl SqlHighlighter {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self { catalog }
    }

    fn style(&self, tokens: &[Token], i: usize) -> Style {
        let token = &tokens[i];
        match token.kind {
            TokenKind::Keyword if CONSTANTS.iter().any(|c| token.is_keyword(c)) => {
                Color::Purple.bold()
            }
            TokenKind::Keyword => Color::Blue.bold(),
            TokenKind::Identifier if is_call(tokens, i) => Color::Cyan.normal(),
            TokenKind::Identifier if self.catalog.get_table(token.text).is_some() => {
                Color::Yellow.normal()
            }
            TokenKind::Number => Color::Purple.normal(),
            TokenKind::String => Color::Green.normal(),
            TokenKind::Comment => Color::DarkGray.italic(),
            TokenKind::Unknown => Color::Red.normal(),
            TokenKind::Identifier
            | TokenKind::QuotedIdentifier
            | TokenKind::Operator
            | TokenKind::Punctuation
            | TokenKind::Whitespace => Style::new(),
        }
    }
}

impl Highlighter for SqlHighlighter {
    fn highlight(&self, line: &str, cursor: usize) -> StyledText {
        let mut styled = StyledText::new();
        if let Some((command, args)) = meta::split(line) {
            styled.push((Color::Cyan.bold(), command.to_owned()));
            styled.push((Style::new(), args.to_owned()));
            return styled;
        }
        let tokens = tokenize(line);
        let brackets = Brackets::find(&tokens);
        let active = brackets.at_cursor(&tokens, cursor);
        for (i, token) in tokens.iter().enumerate() {
            let style = if active.contains(&i) {
                Color::Yellow.bold().underline()
            } else if brackets.unmatched.contains(&i) {
                Color::Red.bold()
            } else {
                self.style(&tokens, i)
            };
            styled.push((style, token.text.to_owned()));
        }
        styled
    }
}

/// Whether the identifier at `i` names a function: a `(` follows it.
fn is_call(tokens: &[Token], i: usize) -> bool {
    tokens[i + 1..]
        .iter()
        .find(|t| t.kind != TokenKind::Whitespace)
        .is_some_and(|t| t.is_punct("("))
}

/// The parentheses and square brackets of a line, by token index.
struct Brackets {
    pairs: Vec<(usize, usize)>,
    unmatched: Vec<usize>,
}

impl Brackets {
    fn find(tokens: &[Token]) -> Self {
        let mut open: Vec<usize> = Vec::new();
        let mut brackets = Brackets {
            pairs: Vec::new(),
            unmatched: Vec::new(),
        };
        for (i, token) in tokens.iter().enumerate() {
            if token.kind != TokenKind::Punctuation {
                continue;
            }
            match token.text {
                "(" | "[" => open.push(i),
                ")" | "]" => match open.pop() {
                    Some(o) if closes(tokens[o].text, token.text) => brackets.pairs.push((o, i)),
                    Some(o) => brackets.unmatched.extend([o, i]),
                    None => brackets.unmatched.push(i),
                },
                _ => {}
            }
        }
        // Brackets still open are not unmatched yet: the rest of the
        // statement may be on its way.
        brackets
    }

    /// The pair with a bracket just before or under the cursor, the one
    /// before taking precedence as the one just typed.
    fn at_cursor(&self, tokens: &[Token], cursor: usize) -> Vec<usize> {
        let bracket_at = |pos: usize| {
            tokens
                .iter()
                .position(|t| t.start == pos && t.kind == TokenKind::Punctuation)
        };
        let candidates = [
            cursor.checked_sub(1).and_then(bracket_at),
            bracket_at(cursor),
        ];
        candidates
            .into_iter()
            .flatten()
            .find_map(|i| self.pairs.iter().find(|&&(o, c)| o == i || c == i))
            .map(|&(o, c)| vec![o, c])
            .unwrap_or_default()
    }
}

fn closes(open: &str, close: &str) -> bool {
    matches!((open, close), ("(", ")") | ("[", "]"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_brackets() {
        let tokens = tokenize("f((a), b])");
        let brackets = Brackets::find(&tokens);
        let text = |i: usize| (tokens[i].start, tokens[i].text);
        let pairs: Vec<_> = brackets
            .pairs
            .iter()
            .map(|&(o, c)| (text(o), text(c)))
            .collect();
        assert_eq!(pairs, [((2, "("), (4, ")"))]);
        let unmatched: Vec<_> = brackets.unmatched.iter().map(|&i| text(i)).collect();
        assert_eq!(unmatched, [(1, "("), (8, "]"), (9, ")")]);
    }

    #[test]
    fn bracket_at_cursor() {
        let tokens = tokenize("(a)");
        let brackets = Brackets::find(&tokens);
        // Just after the closing bracket, as when it was just typed.
        assert_eq!(brackets.at_cursor(&tokens, 3), [0, 2]);
        // On the opening bracket.
        assert_eq!(brackets.at_cursor(&tokens, 0), [0, 2]);
        assert_eq!(brackets.at_cursor(&tokens, 2), [0, 2]);
        let tokens = tokenize("a b");
        assert!(Brackets::find(&tokens).at_cursor(&tokens, 1).is_empty());
    }
}
