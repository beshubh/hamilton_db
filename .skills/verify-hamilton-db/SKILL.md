---
name: verify-hamilton-db
description: Launch a disposable hamilton-server and drive HamiltonDB the way a user does (SQL through hamilton-cli, psql, several concurrent connections, or the interactive shell) to prove a change works end to end, with recorded evidence. Use after changing the parser, planner, executors, storage, transactions, the wire protocol, the server or the CLI, or when asked to verify, demo or reproduce behavior in the real database rather than in unit tests.
---

# Verify HamiltonDB

HamiltonDB's user surface is SQL over the Postgres wire protocol. `hamilton-server` owns the database files; users reach it with `hamilton-cli` (piped SQL or an interactive shell) or `psql`. There is no auth and no seed data: an empty data directory is a usable database.

Everything goes through one helper, `.skills/verify-hamilton-db/hdb` (below: `hdb`). Run it from anywhere; it finds the repo root itself.

```sh
alias hdb=.skills/verify-hamilton-db/hdb    # or call it by path
export HDB_RUN=<short-task-name>            # one run per task; default is "default"
```

A run lives in `/tmp/hamilton-verify/$HDB_RUN/`: `data/` (the database, deleted by cleanup) and `evidence/` (kept). Runs are isolated by name: each gets its own data directory and a free port, so several can run side by side.

Never drive the server on port 6942 or the `hamilton*.db` files in the repo root. Those are Shubham's own session, and only one process can hold a database open.

Read [features/README.md](features/README.md) before driving: it maps each user-facing feature to its recipe.

## Launch

```sh
hdb up
```

Builds `hamilton-server` and `hamilton-cli` (debug; `HDB_PROFILE=release hdb up` for anything timing-related), starts the server on a free port against `data/verify.db`, and returns only after `SELECT 1` has come back through the CLI. It prints `up: run=… pid=… port=… db=…`. `HAMILTON_LOG=debug hdb up` makes the server log to `evidence/server.log`.

After changing Rust code, relaunch with `hdb stop && hdb up`: `up` rebuilds, and the data survives. `hdb restart` restarts without rebuilding. The port changes on every start; always ask `hdb port`.

## Doctor

```sh
hdb doctor
```

Read-only. Exits 1 unless: the recorded pid is alive, it is this checkout's server binary serving this run's database, it owns the port, the running process is the binary currently in `target/` (not one from before a rebuild), no source file is newer than that binary, and `SELECT 1` round-trips. Run it first whenever a result looks off; a stale server is the usual cause.

## Drive

Each command records its input, output and exit code to the next numbered file in `evidence/` and prints the file's path. `LABEL` names the file; SQL comes from `FILE` or stdin.

| Path | Command | Use for |
| --- | --- | --- |
| CLI, piped | `hdb sql LABEL [FILE]` | Almost everything. Also runs `\d`, `\d NAME`, `\timing`, `\?`. |
| psql | `hdb psql LABEL [FILE]` | Wire-protocol compatibility with a real Postgres client. |
| Several connections | `hdb sessions LABEL [FILE]` | Interleaved transactions. |
| Interactive shell | `hdb repl start`, `type TEXT`, `keys KEY...`, `shot LABEL [text\|vt\|html]`, `alive`, `stop` | Prompt, completion, highlighting, key bindings. |
| Lifecycle | `hdb restart`, `hdb crash`, `hdb start` | Persistence and recovery. `crash` is SIGKILL. |

```sh
hdb sql create-and-read <<'EOF'
CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(16) NOT NULL);
INSERT INTO t VALUES (1, 'a');
SELECT * FROM t;
\d t
EOF
```

`hdb sessions` takes the statement format of `sqltest/transactions.sql`: one statement per line, `\session NAME` switches connection (opened on first use, `main` until the first), and `-- => EXPECTED` after a statement asserts its result (`100`, `1, a; 2, b` for rows, `UPDATE 1` for a tag, `error: text` for an error containing the text). It exits 1 on any mismatch.

```sh
hdb sessions lost-update <<'EOF'
\session a
BEGIN;
\session b
BEGIN;
UPDATE accounts SET balance = 50 WHERE id = 1;   -- => UPDATE 1
EOF
```

The interactive shell runs in a background Rex tab labeled `hdb-$HDB_RUN` in your own Rex session (`$REX_SESSION`; outside Rex, `hdb repl` refuses to run). Its prompt is `hamilton ❯`. `type` sends literal text, `keys` sends Rex key names (`enter`, `tab`, `up`, `ctrl+c`, `ctrl+d`), and `shot` records the tab's screen and scrollback: plain text by default, `vt` with escape codes or `html` when colors are the thing being proven. `alive` exits 0 while the tab exists. Wait for the prompt between steps rather than sleeping blind.

```sh
hdb repl start
hdb repl type 'SELECT * FROM t;' && hdb repl keys enter
hdb repl shot repl-select
hdb repl stop
```

For regressions rather than a targeted proof, the repo's own suites start their own servers and need no `hdb up`: `cargo nextest run -p <crate>`, `uv run sqltest/run.py transactions`, `uv run sqltest/run.py index_storage`.

## Evidence

Proof lives in `/tmp/hamilton-verify/$HDB_RUN/evidence/`: `NN-LABEL.txt` per drive (header with time, port, git HEAD and `+dirty`, server binary hash; then input, output, exit code) and `server.log`. Cite these paths when reporting.

Standards:

- Drive the user path: SQL through `hdb sql`, `psql`, `sessions` or the shell. A passing `cargo nextest` run is not a proof of user-visible behavior, and neither is calling `Database` from a test.
- Record the action and the resulting state. After a write, read it back in a separate statement, and for anything that must persist, read it again after `hdb restart`.
- Check side effects where they live: `\d NAME` for schema and indexes, `EXPLAIN` for the plan an index claim depends on, a second session for what other transactions see.
- Assert on output text. `hamilton-cli` and `psql` exit 0 even when a statement fails; an error shows as a line starting `error:` (`ERROR:` in psql).
- Nothing is mocked and nothing needs to be: the server, files and sockets are all real and local.
- If a journey fails, report it with the evidence file. Don't reshape the SQL until it passes.

## Cleanup

```sh
hdb down
```

Closes the shell's Rex tab (by the block id recorded at `hdb repl start`) and stops the server this run started (by recorded pid, SIGTERM, waits for exit), then deletes `data/`. It never kills by process name, never closes a Rex tab it didn't open, and never deletes `evidence/`. Run it after failed attempts too. `hdb stop` stops the server but keeps the data.

## Helpers

- `hdb`: the launcher and driver; `hdb help` lists every command and environment variable.
- `sessions.py`: what `hdb sessions` runs. It reuses `sqltest/pgwire.py` and the result format of `sqltest/transactions.py`, so a change to those changes it too.

Needs `cargo`, `python3` (3.10+), `psql` for `hdb psql`, and for the shell `rex` and `jq` inside a Rex terminal.
