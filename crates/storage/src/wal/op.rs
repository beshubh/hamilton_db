//! Row-level page changes, logged as themselves instead of as page images.
//!
//! An op is encoded as a tag byte and its fields, all little-endian:
//!
//! ```text
//! HeapInit      1 |
//! HeapSetNext   2 | next i32
//! HeapInsert    3 | slot u16 | meta | len u16 | payload
//! HeapSetTuple  4 | slot u16 | meta | len u16 | payload
//! HeapSetMeta   5 | slot u16 | meta
//! HeapStamp     6 | slot u16 | keep_len u16
//! HeapMarkDead  7 | slot u16
//! LeafInsert    8 | num_tombs u16 | index u16 | tomb u16 | page_id i32 | slot u16 | len u16 | key
//! LeafRemove    9 | num_tombs u16 | index u16
//!
//! meta: ts u64 | undo_ptr u64 | flags u8 (1: deleted, 2: has nulls)
//! ```
//!
//! `tomb` is `u16::MAX` for an insert that revives no tombstone.

use common::{PageId, Rid, SlotId};

use crate::error::{Result, StorageError};
use crate::page::PAGE_SIZE;
use crate::page::btree_leaf_page::{self, InsertPosition};
use crate::table::table_page::{TablePage, TupleMeta};

/// A change to one page that depends on nothing but the page's bytes and
/// the op itself. It needs no schema and no comparator, so redo can make it
/// before the catalog can be read. `MtrPage::apply` makes and logs it, and
/// redo replays it with the same `apply`. A change that moves many entries,
/// such as a B+tree split, goes through `MtrPage::raw_mut` and is logged as
/// an image instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageOp<'a> {
    /// Formats the page as an empty table page, whatever it held, so redo
    /// needs nothing of the page from before it.
    HeapInit,
    HeapSetNext {
        next: PageId,
    },
    /// Stores a tuple in `slot`, as `TablePage::insert_at` does.
    HeapInsert {
        slot: SlotId,
        meta: TupleMeta,
        payload: &'a [u8],
    },
    /// Replaces the tuple in `slot`, as `TablePage::update` does.
    HeapSetTuple {
        slot: SlotId,
        meta: TupleMeta,
        payload: &'a [u8],
    },
    HeapSetMeta {
        slot: SlotId,
        meta: TupleMeta,
    },
    /// Commits the tuple in `slot`, as `TablePage::stamp` does.
    HeapStamp {
        slot: SlotId,
        keep_len: usize,
    },
    HeapMarkDead {
        slot: SlotId,
    },
    /// Inserts into a leaf as `BtreeLeafPage::insert_at` does. The leaf's
    /// room for tombstones decides where its slots are, and the page does
    /// not record it.
    LeafInsert {
        num_tombs: usize,
        position: InsertPosition,
        key: &'a [u8],
        rid: Rid,
    },
    /// Removes the live key at `index` from a leaf, as
    /// `BtreeLeafPage::remove` does.
    LeafRemove {
        num_tombs: usize,
        index: usize,
    },
}

const HEAP_INIT: u8 = 1;
const HEAP_SET_NEXT: u8 = 2;
const HEAP_INSERT: u8 = 3;
const HEAP_SET_TUPLE: u8 = 4;
const HEAP_SET_META: u8 = 5;
const HEAP_STAMP: u8 = 6;
const HEAP_MARK_DEAD: u8 = 7;
const LEAF_INSERT: u8 = 8;
const LEAF_REMOVE: u8 = 9;

const NO_TOMB: u16 = u16::MAX;
const DELETED: u8 = 1;
const HAS_NULLS: u8 = 2;

impl<'a> PageOp<'a> {
    /// Makes the change to `data`. Checks first, so a change that fails
    /// leaves the page as it was.
    pub fn apply(&self, data: &mut [u8; PAGE_SIZE]) -> Result<()> {
        match *self {
            Self::HeapInit => {
                TablePage::init(data);
                Ok(())
            }
            Self::HeapSetNext { next } => {
                TablePage::new(data)?.set_next_page_id(next);
                Ok(())
            }
            Self::HeapInsert {
                slot,
                meta,
                payload,
            } => TablePage::new(data)?.insert_at(slot, &meta, payload),
            Self::HeapSetTuple {
                slot,
                meta,
                payload,
            } => TablePage::new(data)?.update(slot, &meta, payload),
            Self::HeapSetMeta { slot, meta } => TablePage::new(data)?.update_meta(slot, &meta),
            Self::HeapStamp { slot, keep_len } => TablePage::new(data)?.stamp(slot, keep_len),
            Self::HeapMarkDead { slot } => TablePage::new(data)?.mark_dead(slot),
            Self::LeafInsert {
                num_tombs,
                position,
                key,
                rid,
            } => btree_leaf_page::apply_insert(data, num_tombs, position, key, rid),
            Self::LeafRemove { num_tombs, index } => {
                btree_leaf_page::apply_remove(data, num_tombs, index)
            }
        }
    }

    /// Appends the op's encoding to `buf`.
    ///
    /// # Panics
    /// If a length or index does not fit in a `u16`, which nothing on a
    /// page can exceed.
    pub(crate) fn encode(&self, buf: &mut Vec<u8>) {
        let u16_of = |n: usize| u16::try_from(n).expect("larger than a page");
        match *self {
            Self::HeapInit => buf.push(HEAP_INIT),
            Self::HeapSetNext { next } => {
                buf.push(HEAP_SET_NEXT);
                buf.extend_from_slice(&next.to_le_bytes());
            }
            Self::HeapInsert {
                slot,
                meta,
                payload,
            }
            | Self::HeapSetTuple {
                slot,
                meta,
                payload,
            } => {
                let tag = if matches!(self, Self::HeapInsert { .. }) {
                    HEAP_INSERT
                } else {
                    HEAP_SET_TUPLE
                };
                buf.push(tag);
                buf.extend_from_slice(&slot.to_le_bytes());
                encode_meta(buf, &meta);
                buf.extend_from_slice(&u16_of(payload.len()).to_le_bytes());
                buf.extend_from_slice(payload);
            }
            Self::HeapSetMeta { slot, meta } => {
                buf.push(HEAP_SET_META);
                buf.extend_from_slice(&slot.to_le_bytes());
                encode_meta(buf, &meta);
            }
            Self::HeapStamp { slot, keep_len } => {
                buf.push(HEAP_STAMP);
                buf.extend_from_slice(&slot.to_le_bytes());
                buf.extend_from_slice(&u16_of(keep_len).to_le_bytes());
            }
            Self::HeapMarkDead { slot } => {
                buf.push(HEAP_MARK_DEAD);
                buf.extend_from_slice(&slot.to_le_bytes());
            }
            Self::LeafInsert {
                num_tombs,
                position,
                key,
                rid,
            } => {
                buf.push(LEAF_INSERT);
                buf.extend_from_slice(&u16_of(num_tombs).to_le_bytes());
                buf.extend_from_slice(&u16_of(position.index).to_le_bytes());
                let tomb = position.tomb.map_or(NO_TOMB, u16_of);
                buf.extend_from_slice(&tomb.to_le_bytes());
                buf.extend_from_slice(&rid.page_id.to_le_bytes());
                buf.extend_from_slice(&rid.slot.to_le_bytes());
                buf.extend_from_slice(&u16_of(key.len()).to_le_bytes());
                buf.extend_from_slice(key);
            }
            Self::LeafRemove { num_tombs, index } => {
                buf.push(LEAF_REMOVE);
                buf.extend_from_slice(&u16_of(num_tombs).to_le_bytes());
                buf.extend_from_slice(&u16_of(index).to_le_bytes());
            }
        }
    }

    /// The op at the start of `bytes`, and the bytes after it.
    fn decode(bytes: &'a [u8]) -> Result<(Self, &'a [u8])> {
        let mut r = Reader(bytes);
        let op = match r.u8()? {
            HEAP_INIT => Self::HeapInit,
            HEAP_SET_NEXT => Self::HeapSetNext { next: r.i32()? },
            tag @ (HEAP_INSERT | HEAP_SET_TUPLE) => {
                let (slot, meta) = (r.u16()?, r.meta()?);
                let len = r.u16()?.into();
                let payload = r.bytes(len)?;
                if tag == HEAP_INSERT {
                    Self::HeapInsert {
                        slot,
                        meta,
                        payload,
                    }
                } else {
                    Self::HeapSetTuple {
                        slot,
                        meta,
                        payload,
                    }
                }
            }
            HEAP_SET_META => Self::HeapSetMeta {
                slot: r.u16()?,
                meta: r.meta()?,
            },
            HEAP_STAMP => Self::HeapStamp {
                slot: r.u16()?,
                keep_len: r.u16()?.into(),
            },
            HEAP_MARK_DEAD => Self::HeapMarkDead { slot: r.u16()? },
            LEAF_INSERT => {
                let (num_tombs, index, tomb) = (r.u16()?, r.u16()?, r.u16()?);
                let rid = Rid::new(r.i32()?, r.u16()?);
                let len = r.u16()?.into();
                Self::LeafInsert {
                    num_tombs: num_tombs.into(),
                    position: InsertPosition {
                        index: index.into(),
                        tomb: (tomb != NO_TOMB).then_some(tomb.into()),
                    },
                    key: r.bytes(len)?,
                    rid,
                }
            }
            LEAF_REMOVE => Self::LeafRemove {
                num_tombs: r.u16()?.into(),
                index: r.u16()?.into(),
            },
            tag => return Err(corrupt(format!("unknown op {tag}"))),
        };
        Ok((op, r.0))
    }
}

/// The ops a record logs for one page, in the order they were made.
#[derive(Debug, Clone, Copy)]
pub struct Ops<'a>(&'a [u8]);

impl<'a> Ops<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }

    /// Whether the first op formats the page, so the ops need nothing of the
    /// page from before them.
    pub fn replace_page(&self) -> bool {
        self.0.first() == Some(&HEAP_INIT)
    }

    pub fn iter(&self) -> impl Iterator<Item = Result<PageOp<'a>>> + 'a {
        let mut rest = self.0;
        std::iter::from_fn(move || {
            if rest.is_empty() {
                return None;
            }
            Some(match PageOp::decode(rest) {
                Ok((op, after)) => {
                    rest = after;
                    Ok(op)
                }
                Err(e) => {
                    rest = &[];
                    Err(e)
                }
            })
        })
    }

    /// Makes every op, in order, on `data`.
    pub fn apply(&self, data: &mut [u8; PAGE_SIZE]) -> Result<()> {
        self.iter().try_for_each(|op| op?.apply(data))
    }
}

fn encode_meta(buf: &mut Vec<u8>, meta: &TupleMeta) {
    buf.extend_from_slice(&meta.ts.to_le_bytes());
    buf.extend_from_slice(&meta.undo_ptr.to_le_bytes());
    let mut flags = 0;
    if meta.is_deleted {
        flags |= DELETED;
    }
    if meta.has_nulls {
        flags |= HAS_NULLS;
    }
    buf.push(flags);
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        let (head, rest) = self
            .0
            .split_at_checked(n)
            .ok_or_else(|| corrupt("truncated op".into()))?;
        self.0 = rest;
        Ok(head)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.bytes(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.bytes(2)?.try_into().unwrap()))
    }

    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_le_bytes(self.bytes(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.bytes(8)?.try_into().unwrap()))
    }

    fn meta(&mut self) -> Result<TupleMeta> {
        let (ts, undo_ptr, flags) = (self.u64()?, self.u64()?, self.u8()?);
        Ok(TupleMeta {
            ts,
            undo_ptr,
            is_deleted: flags & DELETED != 0,
            has_nulls: flags & HAS_NULLS != 0,
        })
    }
}

fn corrupt(msg: String) -> StorageError {
    StorageError::CorruptLog(msg)
}
