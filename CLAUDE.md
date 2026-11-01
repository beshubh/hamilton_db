# Hamilton DB
Hamilton DB is a relational OLTP DBMS written in rust. It supports postgres's SQL dialect and is written as a side project for learning and building database systems.

Use this file to write down any surprises we face in development or testing. Any time you are corrected you should note that down here for future purposes. Don't record what the code already says: the code and its module docs are the source of truth for design.

- Don't run `cargo fmt --all`: it reformats unrelated files. Format only the files you changed (`rustfmt --edition 2024 <file>`).
  - `rustfmt` follows `mod` declarations, so formatting `lib.rs` (or any file declaring modules) also rewrites those submodules. Check `git diff --stat` afterwards and revert files you didn't mean to touch.
- Test-only files live in each crate's `tests/` directory, not in `src/`. Helpers for in-file `#[cfg(test)]` blocks go under `tests/util/` and are pulled in with `#[path]`; Cargo doesn't build `tests/` subdirectories as test targets.
- Commits and PRs carry no Claude attribution: no `Co-Authored-By` trailer, no "Generated with Claude Code" line.
- A database is three files: `<name>.db`, `<name>.meta` and `<name>.log`. To start the CLI fresh, delete all three. Deleting only the `.db` leaves a stale `.meta` that claims pages exist, and opening fails with "page 0 is not a catalog header page".
  - Bumping the catalog `VERSION` (in `header_page.rs`) makes existing database files fail to open with "unsupported catalog version". Delete all three files then too.
- Deleting an index entry and inserting it again leaves the same bytes, so comparing entries can't show that an index was left alone. To test that a statement didn't write an index, trace its pages through the tree's `TracedBufferPoolManager` (`btree.tree().bpm()`).
- Before designing a storage API, read the plan nodes and executors that will call it (`crates/execution/src/plans/`). The index scan API was first built for whole keys, but `IndexScanPlan`'s `KeyBound` is a key prefix, so it had to be redone.
- The CLI only starts the reedline editor when stdin and stdout are terminals; piped input gets plain psql-style output. To drive the editor from a script, run it under a pty that answers the `ESC[6n` cursor-position query, and send Ctrl-C and the next keystrokes as separate writes: input sent in the same burst is lost while reedline restarts.
- When implementing a named algorithm (e.g. block nested loop join), follow its definition, and say up front about anything simplified. The first nested loop join used one child batch as the "block", which ties memory and disk IO to the `batch_size` the parent passes in. That silently undid the point of the algorithm, and it wasn't mentioned until asked. Size operator memory from `ExecutionContext` (e.g. `join_frames`), never from `batch_size`.
- Write executors so the algorithm is visible in the code. The first index nested loop join was a `State` enum stepped through with `mem::replace`, so resuming between `next` calls was correct but nobody could see where outer rows were fetched or the index was queried. Prefer plain helpers (`next_outer_row`, `join_outer_row`) and a small output buffer that `next` drains.
- Write comments so a reader gets the point fast: what the code does, in a few plain sentences, without restating every case the code already shows. Explain a design once, where its type is defined, and link to it from elsewhere. Use one phrase for one idea (index entries "store the rid" or "store the primary key", not a mix of "points by", "locator" and "record id"). A 14-line comment on `UpdateExecutor`'s index handling was unreadable and had to be cut to 4.
- Some tests use a feature that isn't supported yet as their example of an error (a plan with no executor, a literal the parser rejects). They go stale when that feature lands: `ORDER BY` was still the "no executor" example in two tests after the sort executor was added. When adding a feature, grep the tests for it being expected to fail (`NoExecutor`, `assert_unsupported`).
- Run tests with `cargo nextest run --workspace`, not `cargo test`; `.config/nextest.toml` turns off fail-fast so one failure can't hide the rest. Nextest skips doctests, so if you add one, run `cargo test --doc` too.
  - Nextest runs each test in its own process, all at once, so timing races show up more often than under `cargo test`. A test that passes under `cargo test` but fails under nextest is usually racing the GC collector (see the `wait_for_gc` note below), not a nextest problem.
- `planner` depends on `execution`, so nothing in `execution` can bind or plan. The session that runs SQL in transactions (`ExecutionEngine`) lives in `planner::engine`, not in `execution`.
- Under MVTO a reader newer than a lock holder fails with `Conflict`, so a test that has two transactions write the same row must begin the one that takes the lock first. The newer one fails on its scan, before its write.
- The shell is zsh: `for f in $FILES` doesn't split the list, so `rustfmt` gets one long "file" name. Pipe the names into `while read -r f` instead.
- The planner tests (`planner/tests/planner.rs`) check the plan before optimizing (`Planner::plan_unoptimized`); only the EXPLAIN tests use the optimized plan. When the optimizer was added, the tests were half-updated to its output and then broke as more rules landed. A new optimizer rule should only change EXPLAIN expectations there, and the `explain_*` tests in `planner/tests/execute.rs`.
- Show plans in the chat reply in full. If a delegate needs the plan as a file, paste it in chat as well: the index indirection plan was only linked as a scratchpad file, and the user could not see it.
- The transaction manager's collector thread runs after every commit, so a test that counts raw index entries (`index_entries`) right after a statement passes or fails by timing. Call `tm.wait_for_gc()` first, and hold an older transaction open to check that entries are kept for it.
- In a `thread::scope` stress test, stop helper loops from a drop guard in the worker threads, not after joining them: a failed assertion in a worker otherwise leaves the helper looping, and the test hangs instead of failing.
- Keep a plan short and concrete: a code sketch of the core change and a few bullets, not pages of prose. The first index backfill plan was an essay the user would not read.
- Don't offer to leave out what makes a feature correct as an option ("handle concurrent writers, or note it as a known gap?"). An index that can miss rows is broken; just handle it.


## A small glossary
 - **you** means the agent reading this file and chaning HamiltonDB
 - **we** means Shubham and people building HamiltonDB
 - **Hamilton** means this project, the database being built.

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
