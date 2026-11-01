//! Backslash commands, as in psql, and the bare words `exit`, `quit` and
//! `help`.

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Quit,
    Help,
    /// `\d`
    ListTables,
    /// `\d NAME`
    Describe(String),
    /// `\timing`: show how long each statement took, or stop showing it.
    Timing,
    Clear,
    Unknown(String),
}

/// The backslash commands and what they do, for help and completion.
pub const COMMANDS: &[(&str, &str)] = &[
    ("\\?", "show help"),
    ("\\clear", "clear the screen"),
    ("\\d", "list tables, or describe one"),
    ("\\q", "quit"),
    ("\\timing", "toggle statement timing"),
];

/// Reads `line` as a command, or returns `None` if it is SQL.
pub fn parse(line: &str) -> Option<Command> {
    let line = line.trim();
    match line {
        "exit" | "quit" => return Some(Command::Quit),
        "help" => return Some(Command::Help),
        _ => {}
    }
    let (command, args) = split(line)?;
    // A `;` out of SQL habit is harmless.
    let arg = args.trim().trim_end_matches(';').trim();
    Some(match (command.trim_start(), arg) {
        ("\\q", _) => Command::Quit,
        ("\\?" | "\\h", _) => Command::Help,
        ("\\d" | "\\dt", "") => Command::ListTables,
        ("\\d" | "\\dt", name) => Command::Describe(name.to_owned()),
        ("\\timing", _) => Command::Timing,
        ("\\clear", _) => Command::Clear,
        (command, _) => Command::Unknown(command.to_owned()),
    })
}

/// Splits a backslash command line into the command, with any whitespace
/// before it, and the rest of the line, so that the two make up `line`.
/// `None` if `line` is not a command.
pub fn split(line: &str) -> Option<(&str, &str)> {
    let body = line.trim_start();
    if !body.starts_with('\\') {
        return None;
    }
    let indent = line.len() - body.len();
    Some(line.split_at(indent + body.find(char::is_whitespace).unwrap_or(body.len())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_commands() {
        assert_eq!(parse("\\q"), Some(Command::Quit));
        assert_eq!(parse("  quit  "), Some(Command::Quit));
        assert_eq!(parse("\\?"), Some(Command::Help));
        assert_eq!(parse("\\d"), Some(Command::ListTables));
        assert_eq!(parse("\\d users;"), Some(Command::Describe("users".into())));
        assert_eq!(parse("\\timing"), Some(Command::Timing));
        assert_eq!(parse("\\x"), Some(Command::Unknown("\\x".into())));
        assert_eq!(parse("SELECT 1;"), None);
        assert_eq!(parse("exit now"), None);
    }

    #[test]
    fn splits_command_from_arguments() {
        assert_eq!(split("\\d users"), Some(("\\d", " users")));
        assert_eq!(split("  \\d"), Some(("  \\d", "")));
        assert_eq!(split("SELECT 1"), None);
    }
}
