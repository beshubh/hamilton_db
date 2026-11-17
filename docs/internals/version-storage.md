# Version storage

A tuple's versions are ordered newest to oldest, and the newest is stored in place. The heap holds the newest version, called the head, at the tuple's rid. Older versions are undo records in memory. Each record holds the old values of the columns that one write changed. The visibility walk and the record format are in [`undo_log.rs`](../../crates/storage/src/table/undo_log.rs), and the versioned writes are in [`table_heap.rs`](../../crates/storage/src/table/table_heap.rs).

```text
heap (rid 3:1): ts 30 -> record: ts 20, name = 'b' -> record: ts 10, v = 1 -> end
```

## Design decisions

- **Newest version in place.** A row keeps its rid across updates, so an index that stores the rid changes only when a key column changes. A reader that sees the head, which is the usual case, copies the page bytes. It does not decode them or read the undo log.
- **Chains never cross rids.** An update that does not fit on its page deletes the tuple and inserts the new version at a new rid, so the row moves. The old rid's chain still serves older readers, so the old rid's index entries must stay. See [Index management](index-management.md).
- **Deltas are `(column, old value)` pairs, not bytes.** Records never reach disk. To rebuild a version, the heap decodes the head once, applies the deltas, and encodes once.
- **Records are immutable and keyed by their writer's plain Tid.** Commit only clears the uncommitted bit in the heap header. Garbage collection frees every record whose writer is below the watermark.
- **A locked update that shrinks a tuple keeps the tuple's old length**, padded with zeros. Undoing a write therefore always fits back in place and never has to move the row, which recovery relies on. Commit releases the padding.
- **A delete sets a flag** and pushes a record with an empty delta. The bytes stay on the page. After no reader can see the tuple, `reclaim` frees the bytes and marks the slot dead.
- **Dead slots are never reused.** An index entry may still name the rid, and must not lead to an unrelated tuple. Known gap: nothing frees dead slots yet, so each one keeps its entry in the slot array. Freeing them needs a garbage collection that also removes the index entries that name the rid.
- **Undo records are the version store only.** They are in memory, and rollback does not read them: an abort takes a transaction's writes back from the before-images in the log, as recovery does ([Recovery](recovery.md#undo-is-in-each-changes-record)). [Restart and crashes in MVTO](mvto.md#restart-and-crashes) explains why a restart does not need them.
