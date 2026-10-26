pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

pub mod config;
pub mod error;
pub mod hash;
pub mod ids;
pub mod rid;
pub mod schema;
pub mod tuple;
pub mod value;

pub use error::{Result, ValueError};
pub use ids::{FrameId, INVALID_LSN, INVALID_PAGE_ID, INVALID_TXN_ID, Lsn, PageId, TxnId};
pub use rid::{Rid, SlotId};
pub use schema::{Column, Schema};
pub use tuple::Tuple;
pub use value::{DataType, Value, ValueRef};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
