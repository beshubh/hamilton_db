/// Which rows of a table may share a key in one of its indexes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexKind {
    /// Any number of rows may share a key.
    Plain,
    /// No two rows share a key. With `nulls_distinct`, as by default in SQL,
    /// a key with a NULL column equals no other key, so it never conflicts.
    Unique { nulls_distinct: bool },
    /// The table's primary key: unique, on NOT NULL columns.
    PrimaryKey,
}

impl IndexKind {
    pub fn is_primary_key(self) -> bool {
        self == IndexKind::PrimaryKey
    }

    pub fn is_unique(self) -> bool {
        self != IndexKind::Plain
    }
}

/// A UNIQUE constraint: the columns of its key, in key order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UniqueKey {
    /// From `CONSTRAINT name`. Its index takes the name.
    pub name: Option<String>,
    pub columns: Vec<String>,
    /// False after `NULLS NOT DISTINCT`: then NULL keys conflict too.
    pub nulls_distinct: bool,
}

impl UniqueKey {
    pub fn kind(&self) -> IndexKind {
        IndexKind::Unique {
            nulls_distinct: self.nulls_distinct,
        }
    }
}
