use std::sync::Arc;

use common::{Column, DataType, Schema};
use storage::index::IndexMetadata;

fn metadata(is_primary_key: bool) -> IndexMetadata {
    let key_schema = Schema::new(vec![
        Column::new("id", DataType::Int, false),
        Column::new("name", DataType::Varchar(16), true),
    ]);
    IndexMetadata::new(
        "users_id_name",
        "users",
        vec![0, 2],
        Arc::new(key_schema),
        is_primary_key,
    )
}

#[test]
fn accessors() {
    let meta = metadata(true);
    assert_eq!(meta.name(), "users_id_name");
    assert_eq!(meta.table_name(), "users");
    assert_eq!(meta.key_attrs(), [0, 2]);
    assert_eq!(meta.key_schema().len(), 2);
    assert!(meta.is_primary_key());
}

#[test]
fn display() {
    assert_eq!(
        metadata(false).to_string(),
        format!(
            "users_id_name ON users (id INT, name {})",
            DataType::Varchar(16)
        )
    );
    assert!(metadata(true).to_string().ends_with(") PRIMARY KEY"));
}
