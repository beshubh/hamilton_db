use thiserror::Error;

pub type Result<T, E = ParseError> = std::result::Result<T, E>;

/// Everything that can go wrong while turning SQL text into a [`crate::Statement`].
#[derive(Debug, Error)]
pub enum ParseError {
    /// The text is not valid SQL.
    #[error(transparent)]
    Syntax(#[from] sqlparser::parser::ParserError),

    /// Valid SQL that hamilton_db does not handle yet.
    #[error("not supported yet: {0}")]
    Unsupported(String),

    #[error("integer {0} is out of range")]
    IntegerOutOfRange(String),

    #[error("multiple primary keys for table {0} are not allowed")]
    MultiplePrimaryKeys(String),

    #[error("VARCHAR length {0} must be between 1 and {max}", max = u32::MAX)]
    InvalidVarcharLength(u64),
}
