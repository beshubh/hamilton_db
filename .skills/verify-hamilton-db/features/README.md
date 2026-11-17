# HamiltonDB verification map

The maintained source for verifying HamiltonDB's user-facing behavior. Read this index, then use the matching feature file as the recipe. `hdb` is `.skills/verify-hamilton-db/hdb`; [../SKILL.md](../SKILL.md) documents it.

## Baseline preconditions

- `export HDB_RUN=<task-name>`, then `hdb up` and `hdb doctor` (must exit 0).
- The database starts empty. Each recipe creates the tables it needs; table names are per-feature so recipes can share a run.
- Never drive port 6942 or the `hamilton*.db` files in the repo root.

## Driving conventions

- Treat every command as literal. SQL goes in a quoted heredoc (`<<'EOF'`) so the shell leaves `\d` and `$` alone.
- One statement per line in `hdb sessions` scripts; `hdb sql` and `hdb psql` allow statements to span lines.
- Use a fresh `LABEL` per step so each evidence file is one action and its result.
- `hdb down` when finished. To rerun a recipe on a clean database: `hdb down && hdb up`.

## Proof and skip reporting

- A proof is the evidence file path plus the lines in it that show the expected state.
- Mutation proof includes a read-back in a later statement; persistence proof includes a read after `hdb restart`.
- Exit codes prove nothing here: failed statements still exit 0. Match on output text.
- Record the feature ID and the entry point (cli, psql, sessions, shell) with every artifact.
- Report an unreachable path with the command tried and the error text. Do not report an entry point as verified through a different one.

## Feature entry contract

Each feature file is an H1 title, one paragraph on the user-visible behavior, then exactly four H2 sections in this order: `Sub-features` (short IDs), `How to get to it (user POV)` (every entry point), `Driving it with hdb` (starts with `Preconditions:`, then labeled bullets pairing an action with an exact command and the observable result), `Gotchas`.

Keep implementation details out. Name only user paths, stable handles, required state, commands and observable proof.

## Features

- [SQL statements](./sql.md): DDL, DML, queries, types, identity columns, `TRUNCATE`, errors.
- [Indexes](./indexes.md): primary keys, secondary and unique indexes, and proof that a query uses one.
- [Transactions](./transactions.md): `BEGIN`/`COMMIT`/`ROLLBACK` and MVTO conflicts across connections.
- [Persistence](./persistence.md): data across restarts, crashes, and the one-process-per-database rule.
- [Shell and clients](./shell.md): `hamilton-cli` commands and interactive shell, and `psql`.
