# CLAUDE.md

Use this file to write down any surprises we face in development or testing. Any time you are corrected you should note that down here for future purposes. Don't record what the code already says: the code and its module docs are the source of truth for design.

- Don't run `cargo fmt --all`: it reformats unrelated files. Format only the files you changed (`rustfmt --edition 2024 <file>`).
  - `rustfmt` follows `mod` declarations, so formatting `lib.rs` (or any file declaring modules) also rewrites those submodules. Check `git diff --stat` afterwards and revert files you didn't mean to touch.
- Test-only files live in each crate's `tests/` directory, not in `src/`. Helpers for in-file `#[cfg(test)]` blocks go under `tests/util/` and are pulled in with `#[path]`; Cargo doesn't build `tests/` subdirectories as test targets.
- Commits and PRs carry no Claude attribution: no `Co-Authored-By` trailer, no "Generated with Claude Code" line.
- A database is three files: `<name>.db`, `<name>.meta` and `<name>.log`. To start the CLI fresh, delete all three. Deleting only the `.db` leaves a stale `.meta` that claims pages exist, and opening fails with "page 0 is not a catalog header page".
- Before designing a storage API, read the plan nodes and executors that will call it (`crates/execution/src/plans/`). The index scan API was first built for whole keys, but `IndexScanPlan`'s `KeyBound` is a key prefix, so it had to be redone.
