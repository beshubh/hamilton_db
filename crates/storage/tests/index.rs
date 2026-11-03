use std::sync::Arc;

use common::{Column, DataType, IndexKind, Schema};
use storage::index::{IndexMetadata, LocatorKind};

fn metadata(kind: IndexKind) -> IndexMetadata {
    let key_schema = Schema::new(vec![
        Column::new("id", DataType::Int, false),
        Column::new("name", DataType::Varchar(16), true),
    ]);
    IndexMetadata::new(
        "users_id_name",
        "users",
        vec![0, 2],
        Arc::new(key_schema),
        kind,
        LocatorKind::Rid,
    )
}

#[test]
fn accessors() {
    let meta = metadata(IndexKind::PrimaryKey);
    assert_eq!(meta.name(), "users_id_name");
    assert_eq!(meta.table_name(), "users");
    assert_eq!(meta.key_attrs(), [0, 2]);
    assert_eq!(meta.key_schema().len(), 2);
    assert!(meta.is_primary_key());
    assert_eq!(meta.locator(), LocatorKind::Rid);
}

#[test]
fn display() {
    assert_eq!(
        metadata(IndexKind::Plain).to_string(),
        format!(
            "users_id_name ON users (id INT, name {})",
            DataType::Varchar(16)
        )
    );
    assert!(
        metadata(IndexKind::PrimaryKey)
            .to_string()
            .ends_with(") PRIMARY KEY")
    );
    let unique = |nulls_distinct| metadata(IndexKind::Unique { nulls_distinct }).to_string();
    assert!(unique(true).ends_with(") UNIQUE"));
    assert!(unique(false).ends_with(") UNIQUE NULLS NOT DISTINCT"));
}
