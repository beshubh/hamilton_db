use std::cmp::Ordering;
use std::sync::Arc;

use common::{Column, DataType, Schema, Tuple, Value};
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::disk::disk_manager::DiskManager;
use storage::index::IndexMetadata;
use storage::index::btree_index::{BtreeIndex, KeySchemaComparator};
use storage::page::btree_page::KeyComparator;
use tempfile::TempDir;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Column::new("id", DataType::Int, true),
        Column::new("name", DataType::Varchar(16), true),
    ]))
}

fn key(schema: &Schema, id: Option<i32>, name: &str) -> Vec<u8> {
    let id = id.map_or(Value::Null, Value::Int);
    Tuple::encode(schema, &[id, Value::from(name)])
        .unwrap()
        .into_data()
}

#[test]
fn compares_column_by_column() {
    let schema = schema();
    let cmp = KeySchemaComparator::new(Arc::clone(&schema));
    let ordered = [
        key(&schema, None, "a"),
        key(&schema, Some(-1), "z"),
        key(&schema, Some(1), "ab"),
        key(&schema, Some(1), "b"),
        key(&schema, Some(256), ""),
    ];
    for (i, a) in ordered.iter().enumerate() {
        for (j, b) in ordered.iter().enumerate() {
            assert_eq!(cmp.compare(a, b), i.cmp(&j), "key {i} against key {j}");
        }
    }
}

#[test]
fn nulls_are_equal() {
    let schema = schema();
    let cmp = KeySchemaComparator::new(Arc::clone(&schema));
    assert_eq!(
        cmp.compare(&key(&schema, None, "a"), &key(&schema, None, "a")),
        Ordering::Equal
    );
}

#[test]
fn long_strings_compare_by_value() {
    let schema = Arc::new(Schema::new(vec![Column::new(
        "name",
        DataType::Varchar(900),
        false,
    )]));
    let cmp = KeySchemaComparator::new(Arc::clone(&schema));
    let encode = |s: &str| {
        Tuple::encode(&schema, &[Value::from(s)])
            .unwrap()
            .into_data()
    };
    let (a, b) = (encode(&"a".repeat(800)), encode("b"));
    assert!(a.len() > 800 && a.len() <= BtreeIndex::MAX_KEY_SIZE);
    assert_eq!(cmp.compare(&a, &b), Ordering::Less);
    assert_eq!(cmp.compare(&a, &a.clone()), Ordering::Equal);
}

#[test]
fn new_index_is_empty() {
    let dir = TempDir::new().unwrap();
    let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(4, dm));
    let header_page_id = bpm.new_page();
    let metadata = IndexMetadata::new("users_id_name", "users", vec![0, 1], schema(), false);
    let index = BtreeIndex::new(metadata, header_page_id, bpm).unwrap();
    assert!(index.tree().is_empty().unwrap());
    assert_eq!(index.tree().header_page_id(), header_page_id);
}
