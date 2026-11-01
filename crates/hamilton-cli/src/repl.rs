//! The interactive editor: history, completion, highlighting and multi-line
//! statements, on top of reedline.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use nu_ansi_term::{Color, Style};
use reedline::{
    DefaultHinter, DescriptionMode, EditCommand, Emacs, FileBackedHistory, IdeMenu, KeyCode,
    KeyModifiers, Keybindings, MenuBuilder, Reedline, ReedlineEvent, ReedlineMenu, Signal,
    ValidationResult, Validator, default_emacs_keybindings,
};

use crate::complete::SqlCompleter;
use crate::database::Database;
use crate::highlight::SqlHighlighter;
use crate::lexer::ends_statement;
use crate::meta;
use crate::output::Printer;
use crate::prompt::SqlPrompt;
use crate::{Outcome, Session};

const HISTORY_FILE: &str = ".hamilton_history";
const HISTORY_SIZE: usize = 1000;
const COMPLETION_MENU: &str = "completion_menu";

pub fn run(db: &Database, path: &str) -> anyhow::Result<()> {
    let mut editor = editor(db)?;
    let name = Path::new(path)
        .file_name()
        .map_or(path.into(), |n| n.to_string_lossy());
    let mut prompt = SqlPrompt::new(name);
    let mut session = Session::new(
        db,
        Printer {
            color: true,
            timing: true,
        },
    );
    let mut stdout = io::stdout();
    banner(path);

    loop {
        match editor.read_line(&prompt)? {
            Signal::Success(input) => {
                // Saved as it is entered, like the database, so a session
                // that is killed loses none of it.
                editor.sync_history()?;
                let outcome = session.run(&input, &mut stdout)?;
                prompt.session = db.session();
                match outcome {
                    Outcome::Done { failed } => prompt.failed = failed,
                    Outcome::Clear => editor.clear_screen()?,
                    Outcome::Quit => break,
                }
            }
            // Ctrl-C drops what was typed, as in psql.
            Signal::CtrlC => prompt.failed = false,
            Signal::CtrlD => break,
            _ => {}
        }
    }
    Ok(())
}

fn editor(db: &Database) -> anyhow::Result<Reedline> {
    let catalog = Arc::clone(db.catalog());
    let menu = IdeMenu::default()
        .with_name(COMPLETION_MENU)
        // Shown in place of the prompt's arrow while the menu is open.
        .with_marker(" ❯ ")
        .with_default_border()
        .with_max_completion_height(12)
        .with_description_mode(DescriptionMode::PreferRight)
        .with_max_description_width(40)
        .with_text_style(Color::Default.normal())
        .with_selected_text_style(Color::Black.on(Color::Cyan))
        .with_description_text_style(Color::DarkGray.italic())
        .with_match_text_style(Color::Cyan.bold())
        .with_selected_match_text_style(Color::Black.bold().on(Color::Cyan));
    let mut editor = Reedline::create()
        .with_highlighter(Box::new(SqlHighlighter::new(Arc::clone(&catalog))))
        .with_completer(Box::new(SqlCompleter::new(catalog)))
        .with_menu(ReedlineMenu::EngineCompleter(Box::new(menu)))
        .with_quick_completions(true)
        .with_partial_completions(true)
        .with_hinter(Box::new(
            DefaultHinter::default().with_style(Color::DarkGray.italic()),
        ))
        .with_validator(Box::new(StatementValidator))
        .with_transient_prompt(Box::new(SqlPrompt::transient()))
        .with_edit_mode(Box::new(Emacs::new(keybindings())))
        .use_bracketed_paste(true);
    if let Some(history) = history_file() {
        editor = editor.with_history(Box::new(FileBackedHistory::with_file(
            HISTORY_SIZE,
            history,
        )?));
    }
    Ok(editor)
}

/// Emacs keys, which already map ↑/↓ to history (or to the line above and
/// below, in a statement spanning lines) and Ctrl-R to history search.
fn keybindings() -> Keybindings {
    let mut keys = default_emacs_keybindings();
    keys.add_binding(
        KeyModifiers::NONE,
        KeyCode::Tab,
        ReedlineEvent::UntilFound(vec![
            ReedlineEvent::Menu(COMPLETION_MENU.to_owned()),
            ReedlineEvent::MenuNext,
        ]),
    );
    keys.add_binding(
        KeyModifiers::SHIFT,
        KeyCode::BackTab,
        ReedlineEvent::MenuPrevious,
    );
    keys.add_binding(
        KeyModifiers::CONTROL,
        KeyCode::Char('l'),
        ReedlineEvent::ClearScreen,
    );
    // A new line without running anything, even after a `;`.
    keys.add_binding(
        KeyModifiers::ALT,
        KeyCode::Enter,
        ReedlineEvent::Edit(vec![EditCommand::InsertNewline]),
    );
    keys
}

/// Enter runs the input once it ends a statement, and starts a new line
/// until then.
struct StatementValidator;

impl Validator for StatementValidator {
    fn validate(&self, input: &str) -> ValidationResult {
        if input.trim().is_empty() || meta::parse(input).is_some() || ends_statement(input) {
            ValidationResult::Complete
        } else {
            ValidationResult::Incomplete
        }
    }
}

/// `~/.hamilton_history`, shared by every database, like psql's.
fn history_file() -> Option<PathBuf> {
    std::env::home_dir().map(|home| home.join(HISTORY_FILE))
}

fn banner(path: &str) {
    let dim = Color::DarkGray.normal();
    let key = Style::new().bold();
    println!(
        "{} {} {} {}",
        Color::Cyan.bold().paint("hamilton_db"),
        dim.paint(concat!("v", env!("CARGO_PKG_VERSION"))),
        dim.paint("·"),
        Color::Yellow.paint(path),
    );
    println!(
        "{}{}{}",
        dim.paint("Type "),
        key.paint("\\?"),
        dim.paint(" for help.\n"),
    );
}
