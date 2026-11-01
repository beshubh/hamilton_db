# Version storage

Versions are ordered newest to oldest, and the newest is stored in place. The heap holds each tuple's newest version at its rid. Older versions are undo records in memory, each holding the old values of the columns a write changed. The visibility walk and the record format are in [`undo_log.rs`](../../crates/storage/src/table/undo_log.rs), and the versioned writes are in [`table_heap.rs`](../../crates/storage/src/table/table_heap.rs).

```text
heap (rid 3:1): ts 30 -> record: ts 20, name = 'b' -> record: ts 10, v = 1 -> end
```

## Decisions

- **Newest version in place.** A row keeps its rid across updates, so an index that stores the rid changes only when a key column changes. A reader that sees the head, which is the usual case, copies the page bytes without decoding them or touching the log.
- **Chains never cross rids.** An update that doesn't fit on its page deletes the tuple and inserts the new version at a new rid: the row moves. The old rid's chain still serves older readers, so its index entries must stay (see [index-management.md](index-management.md)).
- **Deltas are `(column, old value)` pairs, not bytes.** Records never reach disk. Rebuilding a version decodes the head once, applies the deltas, and encodes once.
- **Records are immutable and keyed by their writer's plain Tid.** Commit only clears the uncommitted bit in the heap header. Garbage collection frees every record whose writer is below the watermark.
- **A locked update that shrinks a tuple keeps its old length**, padded with zeros, so a rollback always fits back in place and never has to move the row. Commit releases the padding.
- **A delete sets a flag** and pushes a record with an empty delta; the bytes stay on the page. Once no reader can see the tuple, `reclaim` frees the bytes and marks the slot dead.
- **Dead slots are never reused.** An index entry may still name the rid, and must not come to lead to an unrelated tuple. Known gap: dead slots pile up until a GC also removes the index entries that name them.
- **Not durable.** Undo records live only in memory. [mvto.md](mvto.md#restart-and-crashes) explains why a clean restart doesn't need them.
