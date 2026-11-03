pub mod catalog;
pub mod error;
pub mod header_page;
mod locks;
mod sequence;
mod system_tables;

pub use catalog::{
    Catalog, IndexInfo, IndexOid, MAX_NAME_LEN, RelationKind, TableInfo, TableOid, TableWriteGuard,
};
pub use error::{CatalogError, Result};
pub use locks::LockMode;
pub use sequence::Sequence;
