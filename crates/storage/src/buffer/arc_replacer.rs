use std::collections::HashMap;

use common::{FrameId, PageId};
use hashlink::LinkedHashSet;
use parking_lot::Mutex;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AccessType {
    #[default]
    Unknown,
    Lookup,
    Scan,
    Index,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArcStatus {
    Mru,
    Mfu,
    MruGhost,
    MfuGhost,
}

#[derive(Debug)]
pub struct FrameStatus {
    page_id: PageId,
    frame_id: FrameId,
    evictable: bool,
    status: ArcStatus,
}

/// All lists are ordered oldest-first: `insert` adds the most recent entry at
/// the back and the eviction candidate is at the front.
struct ArcState {
    mru: LinkedHashSet<FrameId>,
    mfu: LinkedHashSet<FrameId>,
    mru_ghost: LinkedHashSet<PageId>,
    mfu_ghost: LinkedHashSet<PageId>,
    /// Alive entries keyed by frame id (in `mru` or `mfu`).
    alive_map: HashMap<FrameId, FrameStatus>,
    /// Dead entries keyed by page id (in `mru_ghost` or `mfu_ghost`).
    ghost_map: HashMap<PageId, FrameStatus>,
    /// Number of evictable frames.
    curr_size: usize,
    mru_target_size: usize,
    replacer_size: usize,
}

pub struct ArcReplacer {
    state: Mutex<ArcState>,
}

impl ArcState {
    /// Evicts the oldest evictable frame from `mru` (or `mfu` when `from_mfu`)
    /// into the matching ghost list.
    fn try_evict(&mut self, from_mfu: bool) -> Option<FrameId> {
        let alive = if from_mfu { &self.mfu } else { &self.mru };
        let frame_id = *alive.iter().find(|f| self.alive_map[f].evictable)?;

        let mut frame = self.alive_map.remove(&frame_id).unwrap();
        if from_mfu {
            self.mfu.remove(&frame_id);
            self.mfu_ghost.insert(frame.page_id);
            frame.status = ArcStatus::MfuGhost;
        } else {
            self.mru.remove(&frame_id);
            self.mru_ghost.insert(frame.page_id);
            frame.status = ArcStatus::MruGhost;
        }
        self.ghost_map.insert(frame.page_id, frame);
        self.curr_size -= 1;
        Some(frame_id)
    }
}

impl ArcReplacer {
    /// Creates a replacer that caches at most `num_frames` frames, with empty
    /// lists and a target size of 0.
    pub fn new(num_frames: usize) -> Self {
        Self {
            state: Mutex::new(ArcState {
                mru: LinkedHashSet::new(),
                mfu: LinkedHashSet::new(),
                mru_ghost: LinkedHashSet::new(),
                mfu_ghost: LinkedHashSet::new(),
                alive_map: HashMap::new(),
                ghost_map: HashMap::new(),
                curr_size: 0,
                mru_target_size: 0,
                replacer_size: num_frames,
            }),
        }
    }

    /// Evicts from `mru` or `mfu` into the corresponding ghost list according
    /// to the balancing policy. Pinned entries are skipped; if every entry on
    /// the preferred side is pinned, the other side is tried instead.
    ///
    /// Returns the evicted frame id, or `None` if nothing can be evicted.
    pub fn evict(&self) -> Option<FrameId> {
        let mut state = self.state.lock();
        let prefer_mfu = state.mru.len() < state.mru_target_size;
        state
            .try_evict(prefer_mfu)
            .or_else(|| state.try_evict(!prefer_mfu))
    }

    /// Records an access to `frame_id` holding `page_id`. A hit in any list
    /// moves the entry to the front of `mfu`; a miss adds it to the front of
    /// `mru`. Performs everything from the ARC paper except REPLACE, which is
    /// left to `evict`.
    pub fn record_access(&self, frame_id: FrameId, page_id: PageId, _access_type: AccessType) {
        let mut state = self.state.lock();
        let state = &mut *state;

        // Case 1: hit in mru or mfu.
        if let Some(frame) = state.alive_map.get_mut(&frame_id) {
            match frame.status {
                ArcStatus::Mru => state.mru.remove(&frame_id),
                _ => state.mfu.remove(&frame_id),
            };
            state.mfu.insert(frame_id);
            frame.status = ArcStatus::Mfu;
            return;
        }

        // Cases 2/3: hit in mru_ghost or mfu_ghost.
        if let Some(mut frame) = state.ghost_map.remove(&page_id) {
            let target = state.mru_target_size;
            if frame.status == ArcStatus::MruGhost {
                let delta = (state.mfu_ghost.len() / state.mru_ghost.len()).max(1);
                state.mru_target_size = (target + delta).min(state.replacer_size);
                state.mru_ghost.remove(&page_id);
            } else {
                let delta = (state.mru_ghost.len() / state.mfu_ghost.len()).max(1);
                state.mru_target_size = target.saturating_sub(delta);
                state.mfu_ghost.remove(&page_id);
            }

            state.mfu.insert(frame_id);
            frame.frame_id = frame_id;
            frame.evictable = false;
            frame.status = ArcStatus::Mfu;
            state.alive_map.insert(frame_id, frame);
            return;
        }

        // Case 4: miss in all lists. Trim the ghost lists to keep the
        // directory within bounds before adding the new entry.
        let recency_size = state.mru.len() + state.mru_ghost.len();
        let total_size = recency_size + state.mfu.len() + state.mfu_ghost.len();
        if recency_size == state.replacer_size {
            if let Some(pid) = state.mru_ghost.pop_front() {
                state.ghost_map.remove(&pid);
            }
        } else if recency_size < state.replacer_size && total_size == 2 * state.replacer_size {
            if let Some(pid) = state.mfu_ghost.pop_front() {
                state.ghost_map.remove(&pid);
            }
        }

        state.mru.insert(frame_id);
        state.alive_map.insert(
            frame_id,
            FrameStatus {
                page_id,
                frame_id,
                evictable: false,
                status: ArcStatus::Mru,
            },
        );
    }

    /// Marks a frame evictable or not, adjusting the replacer size. Unknown
    /// frames are ignored.
    pub fn set_evictable(&self, frame_id: FrameId, set_evictable: bool) {
        let mut state = self.state.lock();
        let Some(frame) = state.alive_map.get_mut(&frame_id) else {
            return;
        };
        if frame.evictable == set_evictable {
            return;
        }
        frame.evictable = set_evictable;
        if set_evictable {
            state.curr_size += 1;
        } else {
            state.curr_size -= 1;
        }
    }

    /// Removes an evictable frame from the replacer without moving it to a
    /// ghost list. Unknown frames are ignored.
    ///
    /// # Panics
    /// If the frame is not evictable.
    pub fn remove(&self, frame_id: FrameId) {
        let mut state = self.state.lock();
        let Some(frame) = state.alive_map.get(&frame_id) else {
            return;
        };
        assert!(
            frame.evictable,
            "calling remove on non-evictable frame {frame_id} is not allowed"
        );
        match frame.status {
            ArcStatus::Mru => state.mru.remove(&frame_id),
            _ => state.mfu.remove(&frame_id),
        };
        state.alive_map.remove(&frame_id);
        state.curr_size -= 1;
    }

    /// Number of evictable frames.
    pub fn size(&self) -> usize {
        self.state.lock().curr_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn access(r: &ArcReplacer, frame_id: FrameId, page_id: PageId) {
        r.record_access(frame_id, page_id, AccessType::Unknown);
    }

    #[test]
    fn new_frames_are_not_evictable() {
        let r = ArcReplacer::new(3);
        access(&r, 0, 100);
        assert_eq!(r.size(), 0);
        assert_eq!(r.evict(), None);
    }

    #[test]
    fn set_evictable_tracks_size() {
        let r = ArcReplacer::new(3);
        access(&r, 0, 100);
        access(&r, 1, 101);
        r.set_evictable(0, true);
        r.set_evictable(0, true);
        r.set_evictable(1, true);
        assert_eq!(r.size(), 2);
        r.set_evictable(1, false);
        assert_eq!(r.size(), 1);
        r.set_evictable(42, true); // unknown frame
        assert_eq!(r.size(), 1);
    }

    #[test]
    fn evicts_oldest_mru_first() {
        let r = ArcReplacer::new(3);
        for f in 0..3 {
            access(&r, f, 100 + f);
            r.set_evictable(f, true);
        }
        assert_eq!(r.evict(), Some(0));
        assert_eq!(r.evict(), Some(1));
        assert_eq!(r.evict(), Some(2));
        assert_eq!(r.evict(), None);
        assert_eq!(r.size(), 0);
    }

    #[test]
    fn skips_pinned_frames() {
        let r = ArcReplacer::new(3);
        for f in 0..3 {
            access(&r, f, 100 + f);
            r.set_evictable(f, true);
        }
        r.set_evictable(0, false);
        assert_eq!(r.evict(), Some(1));
    }

    #[test]
    fn falls_back_to_mfu_when_mru_pinned() {
        let r = ArcReplacer::new(3);
        access(&r, 0, 100);
        access(&r, 1, 101);
        access(&r, 1, 101); // frame 1 moves to mfu
        r.set_evictable(1, true);
        assert_eq!(r.evict(), Some(1));
    }

    #[test]
    fn mru_ghost_hit_grows_target_and_moves_to_mfu() {
        let r = ArcReplacer::new(2);
        access(&r, 0, 100);
        access(&r, 1, 101);
        r.set_evictable(0, true);
        r.set_evictable(1, true);
        assert_eq!(r.evict(), Some(0)); // page 100 -> mru_ghost

        // Page 100 comes back in frame 0: ghost hit, now in mfu and pinned.
        access(&r, 0, 100);
        {
            let state = r.state.lock();
            assert_eq!(state.mru_target_size, 1);
            assert_eq!(state.alive_map[&0].status, ArcStatus::Mfu);
            assert!(state.mru_ghost.is_empty());
        }
        assert_eq!(r.size(), 1);

        // mru (len 1) >= target (1), so evict from mru first.
        r.set_evictable(0, true);
        assert_eq!(r.evict(), Some(1));
    }

    #[test]
    fn mfu_ghost_hit_shrinks_target() {
        let r = ArcReplacer::new(2);
        access(&r, 0, 100);
        access(&r, 0, 100); // mfu
        r.set_evictable(0, true);
        assert_eq!(r.evict(), Some(0)); // page 100 -> mfu_ghost
        r.state.lock().mru_target_size = 2;

        access(&r, 1, 100); // mfu_ghost hit
        let state = r.state.lock();
        assert_eq!(state.mru_target_size, 1);
        assert_eq!(state.alive_map[&1].page_id, 100);
        assert!(state.mfu_ghost.is_empty());
    }

    #[test]
    fn remove_evictable_frame() {
        let r = ArcReplacer::new(2);
        access(&r, 0, 100);
        r.set_evictable(0, true);
        r.remove(0);
        assert_eq!(r.size(), 0);
        assert_eq!(r.evict(), None);
        r.remove(0); // unknown frame is a no-op
    }

    #[test]
    #[should_panic(expected = "non-evictable")]
    fn remove_pinned_frame_panics() {
        let r = ArcReplacer::new(2);
        access(&r, 0, 100);
        r.remove(0);
    }

    #[test]
    fn miss_trims_mru_ghost_when_full() {
        let r = ArcReplacer::new(2);
        access(&r, 0, 100);
        access(&r, 1, 101);
        r.set_evictable(0, true);
        assert_eq!(r.evict(), Some(0)); // mru = [1], mru_ghost = [100]

        access(&r, 0, 102); // recency size == 2, drops page 100 from mru_ghost
        let state = r.state.lock();
        assert!(state.mru_ghost.is_empty());
        assert!(!state.ghost_map.contains_key(&100));
    }
}
