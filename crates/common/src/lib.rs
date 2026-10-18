pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

pub mod config;
pub mod ids;
pub mod rid;

pub use ids::{FrameId, INVALID_LSN, INVALID_PAGE_ID, INVALID_TXN_ID, Lsn, PageId, TxnId};
pub use rid::{Rid, SlotId};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
