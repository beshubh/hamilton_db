pub mod catalog;
pub mod error;
pub mod header_page;
mod system_tables;

pub use catalog::{
    Catalog, IndexInfo, IndexOid, MAX_NAME_LEN, TableInfo, TableOid, TableWriteGuard,
};
pub use error::{CatalogError, Result};
