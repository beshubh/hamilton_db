use crate::tuple;
use crate::value::DataType;

/// A named, typed column of a table or of a query's output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }
}

/// The ordered columns of a tuple.
///
/// Names need not be unique: a query's output can repeat a column
/// (`SELECT a, a`). Table schemas are kept unique by the catalog.
///
/// The schema also holds where each column's fixed-width slot sits in an
/// encoded tuple (see [`crate::tuple`]), so one column can be read without
/// walking the ones before it.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Schema {
    columns: Vec<Column>,
    fixed_offsets: Vec<usize>,
    /// Length of the null bitmap and fixed-width slots together; the
    /// variable-length data of a tuple starts here.
    fixed_len: usize,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        let mut fixed_offsets = Vec::with_capacity(columns.len());
        let mut fixed_len = tuple::bitmap_len(columns.len());
        for column in &columns {
            fixed_offsets.push(fixed_len);
            fixed_len += tuple::fixed_width(column.data_type);
        }
        Self {
            columns,
            fixed_offsets,
            fixed_len,
        }
    }

    /// Offset of column `idx`'s fixed-width slot in an encoded tuple.
    pub(crate) fn fixed_offset(&self, idx: usize) -> usize {
        self.fixed_offsets[idx]
    }

    pub(crate) fn fixed_len(&self) -> usize {
        self.fixed_len
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn column(&self, idx: usize) -> Option<&Column> {
        self.columns.get(idx)
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Position of the first column with this name.
    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn users() -> Schema {
        Schema::new(vec![
            Column::new("id", DataType::Int, false),
            Column::new("name", DataType::Varchar(64), true),
            Column::new("active", DataType::Bool, false),
        ])
    }

    #[test]
    fn looks_up_columns_by_position_and_name() {
        let schema = users();
        assert_eq!(schema.len(), 3);
        assert_eq!(schema.index_of("name"), Some(1));
        assert_eq!(schema.index_of("missing"), None);
        let name = schema.column(1).unwrap();
        assert_eq!(name.data_type, DataType::Varchar(64));
        assert!(name.nullable);
        assert!(schema.column(3).is_none());
    }

    #[test]
    fn index_of_finds_first_duplicate() {
        let schema = Schema::new(vec![
            Column::new("a", DataType::Int, false),
            Column::new("a", DataType::BigInt, false),
        ]);
        assert_eq!(schema.index_of("a"), Some(0));
    }

    #[test]
    fn empty() {
        assert!(Schema::default().is_empty());
    }
}
