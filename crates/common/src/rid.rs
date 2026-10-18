use std::fmt;

use crate::ids::PageId;

/// Index of a row's slot within its page.
pub type SlotId = u16;

/// Record id: where a row lives, as the page it is on and its slot in that
/// page.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Rid {
    pub page_id: PageId,
    pub slot: SlotId,
}

impl Rid {
    pub fn new(page_id: PageId, slot: SlotId) -> Self {
        Self { page_id, slot }
    }
}

impl fmt::Display for Rid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {})", self.page_id, self.slot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orders_by_page_then_slot() {
        assert!(Rid::new(1, 9) < Rid::new(2, 0));
        assert!(Rid::new(2, 0) < Rid::new(2, 1));
    }

    #[test]
    fn display() {
        assert_eq!(Rid::new(3, 7).to_string(), "(3, 7)");
    }
}
