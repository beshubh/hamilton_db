pub type FrameId = i32;

/// Replacer is a trait that tracks the page usage
pub trait Replacer {
    // Remove the victim frame as defined by the replacement policy
    fn victim() -> (Option<FrameId>, bool);

    // Pins a frame, indicating that it should not be victimized until unpinned
    fn pin(frame_id: FrameId);

    // Unpins a frame, indicating that this frame can be victimized
    fn unpin(frame_id: FrameId);

    // Number of elements in the replacer that can be victimized
    fn size() -> usize;
}
