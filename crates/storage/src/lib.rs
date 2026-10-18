pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

pub mod buffer;
pub mod concurrency;
pub mod disk;
pub mod error;
pub mod index;
pub mod page;
pub mod table;

pub use error::{Result, StorageError};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
