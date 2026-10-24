//! The prompt: `hamilton ❯`, its arrow red after a statement fails, with the
//! database's name on the right. Lines already entered keep the prompt but
//! lose the name, so it shows only on the line being edited.

use std::borrow::Cow;

use nu_ansi_term::Color;
use reedline::{Prompt, PromptEditMode, PromptHistorySearch, PromptHistorySearchStatus};

pub struct SqlPrompt {
    /// Shown on the right; empty for lines already entered.
    database: String,
    /// Whether the last input failed.
    pub failed: bool,
}

impl SqlPrompt {
    pub fn new(database: impl Into<String>) -> Self {
        Self {
            database: database.into(),
            failed: false,
        }
    }

    /// The prompt that replaces this one once its line is entered.
    pub fn transient() -> Self {
        Self::new("")
    }
}

impl Prompt for SqlPrompt {
    fn render_prompt_left(&self) -> Cow<'_, str> {
        Cow::Borrowed("hamilton")
    }

    fn render_prompt_right(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.database)
    }

    fn render_prompt_indicator(&self, _mode: PromptEditMode) -> Cow<'_, str> {
        Cow::Borrowed(" ❯ ")
    }

    /// As wide as `hamilton ❯ `, so continued lines line up.
    fn render_prompt_multiline_indicator(&self) -> Cow<'_, str> {
        Cow::Borrowed("         · ")
    }

    fn render_prompt_history_search_indicator(&self, search: PromptHistorySearch) -> Cow<'_, str> {
        let status = match search.status {
            PromptHistorySearchStatus::Passing => "",
            PromptHistorySearchStatus::Failing => "failing ",
        };
        Cow::Owned(format!("({status}search: {}) ", search.term))
    }

    fn get_prompt_color(&self) -> Color {
        Color::Cyan
    }

    fn get_prompt_multiline_color(&self) -> Color {
        Color::DarkGray
    }

    fn get_indicator_color(&self) -> Color {
        if self.failed {
            Color::Red
        } else {
            Color::Green
        }
    }

    fn get_prompt_right_color(&self) -> Color {
        Color::DarkGray
    }
}
