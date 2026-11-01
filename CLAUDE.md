# Hamilton DB

# Formatting
  - Don't run `cargo fmt --all`: it reformats unrelated files. Format only the files you changed (`rustfmt --edition 2024 <file>`).
    - `rustfmt` follows `mod` declarations, so formatting `lib.rs` (or any file declaring modules) also rewrites those submodules. Check `git diff --stat` afterwards and revert files you didn't mean to touch.

# Rules
- Test-only files live in each crate's `tests/` directory, not in `src/`. Helpers for in-file `#[cfg(test)]` blocks go under `tests/util/` and are pulled in with `#[path]`; Cargo doesn't build `tests/` subdirectories as test targets.

# How does it works  
- To start the CLI fresh (or after bumping the catalog `VERSION`), delete all three database files: `<name>.db`, `<name>.meta` and `<name>.log`. Deleting only the `.db` leaves a stale `.meta` that claims pages exist, and opening fails with "page 0 is not a catalog header page".

## A small glossary
 - **you** means the agent reading this file and chaning HamiltonDB
 - **we** means Shubham and people building HamiltonDB

# Documentation
---
Most code changes do not need an internal documentation change. Agents can read the code.

- `docs/internals/` is for architectural decisions and their reasons, constraints that span components, and implementation traps that are hard to discover from the source. Before adding a paragraph, ask what a maintainer would get wrong without it. If reading the relevant code answers the question, leave it out.
- Do not document every feature, enumerate fields or methods, narrate control flow, maintain file catalogs, or append PR summaries. Types, tests, and code already record the implementation. The glossary defines shared vocabulary; it is not a feature index.
- A local reason the code can't show goes in a short nearby code comment. Use an internal doc when the reasoning crosses boundaries or needs context the code cannot carry well. Link to the relevant source instead of copying it.
- When a documented decision or constraint changes, rewrite or remove the affected text. Do not append another account of the new behavior. A new internal page needs a distinct, durable reason to exist.****

# Plans and work artifacts
---
- Do not commit implementation plans, research notes, or agent scratch files. Keep temporary working material outside the worktree. .plans/ is gitignored only as a safety net for legacy tooling.
- Track active maintainer work in the GitHub issue or project item that owns it. External proposals follow CONTRIBUTING.md and belong in Ideas discussions.
- A merged PR is the implementation record. Close or update its tracking item when the work lands; do not preserve a second checklist in the repository.

# Taste
---
- Default to no comment. Write one only for a constraint, an external quirk, or an invariant the code can't show, in one line where you can and four at most. Don't restate names or signatures, narrate steps, record history, or explain the fix you just made; the PR carries that.
- Do not add complex state machinery for simple nitpick features, and prefer state machine where we would want to concretely remove the wrong state transformations.

# Performance
---
## Hot paths (scans, filters, comparisons, page/tuple decoding, executor loops)
- No per-row heap allocations: borrow from page buffers (&[u8], &str), reuse
  buffers (clear() not new), no clone()/to_vec()/format!() in loops.
- Prefer predictable or branchless code over data-dependent branches.
- Outside hot paths, write plain readable code; don't optimize setup/DDL/error paths.
- Verify before and after: `samply record` for CPU time, `dhat-rs` for allocations.
- Don't claim a perf win without numbers. 
