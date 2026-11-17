# Shell and clients

`hamilton-cli` connects to a running server. With SQL piped in it prints plain results; at a terminal it is an interactive shell with a `hamilton ❯` prompt, boxed tables, timing, completion and history. `psql` and other Postgres clients can connect too (simple queries, text format, no authentication).

## Sub-features

- `shell-connect` connects with `-h HOST -p PORT` and explains itself when no server is there or a file path is given.
- `shell-meta` runs `\d`, `\d NAME`, `\timing`, `\?` (also `help`), and reports unknown commands.
- `shell-interactive` shows the banner and prompt, runs a typed statement, and shows errors in place.
- `shell-complete` completes keywords and table names with Tab.
- `shell-quit` leaves on `\q`, `exit`, `quit` or Ctrl-D.
- `shell-psql` runs SQL from `psql`.

## How to get to it (user POV)

- `hamilton-cli -p <port>` in a terminal for the interactive shell.
- `hamilton-cli -p <port> < file.sql`, or a pipe, for a script.
- `psql -h 127.0.0.1 -p <port>`.

## Driving it with hdb

Preconditions:

- `hdb doctor` exits 0.
- No table named `sh_pets` exists. Create it with `hdb sql shell-setup`:

  ```sql
  CREATE TABLE sh_pets (id INT PRIMARY KEY, name VARCHAR(16) NOT NULL);
  INSERT INTO sh_pets VALUES (1, 'rex');
  ```

- **Meta commands.** Run `hdb sql shell-meta`. In order: a table listing with a `sh_pets` row showing `2` columns and `sh_pets_pkey`; a `Table "sh_pets"` description; `error: no table named "nope"`; `Timing is on.`; a result followed by a `Time: … ms` line; `error: unknown command \x; try \?`; help text with `SQL`, `Commands` and `Keys` headings.

  ```
  \d
  \d sh_pets
  \d nope
  \timing
  SELECT * FROM sh_pets;
  \x
  \?
  ```

- **Connection errors.** Run `target/debug/hamilton-cli -p 1 </dev/null; echo "exit=$?"`. It prints `could not connect to hamilton-server at localhost:1; is it running?` and `exit=1`. Run `target/debug/hamilton-cli some.db </dev/null; echo "exit=$?"`. It prints `hamilton-cli connects to a server and doesn't open database files` and `exit=1`.

- **Interactive statement.** Run `hdb repl start`, then `hdb repl type 'SELECT * FROM sh_pets;'`, `hdb repl keys enter`, `hdb repl shot shell-select`. The capture shows the banner `hamilton_db v… · 127.0.0.1:<port>`, the prompt `hamilton ❯` with the statement, a boxed table containing `rex`, and `(1 row) · … ms`.

- **Interactive error.** Run `hdb repl type 'SELECT nope;'`, `hdb repl keys enter`, `hdb repl shot shell-error`. The capture shows `error: column nope does not exist` and a fresh prompt.

- **Completion.** Run `hdb repl type 'SEL'`, `hdb repl keys tab`, `hdb repl type ' * FROM sh_p'`, `hdb repl keys tab`, `hdb repl shot shell-complete`. The prompt line reads `SELECT  * FROM sh_pets`. Then `hdb repl keys ctrl+c` to drop it.

- **Quit.** Run `hdb repl alive; echo "exit=$?"` (prints `exit=0`), then `hdb repl keys ctrl+d`, then `hdb repl alive; echo "exit=$?"` again. It now prints a non-zero exit: the shell exited and its Rex tab closed with it. Run `hdb repl stop` to clear the record. `hdb doctor` still exits 0.

- **psql.** Run `hdb psql shell-psql`. Output is a psql-formatted table containing `rex`, then `ERROR:  column nope does not exist`.

  ```sql
  SELECT * FROM sh_pets;
  SELECT nope;
  ```

## Gotchas

- Piped and interactive output differ: piped is plain psql-style with no timing; interactive draws boxes and shows timing by default. A change to one printer path isn't proven by the other.
- `hdb repl shot LABEL` captures text only. The prompt arrow turning red after an error and syntax highlighting are colors: capture them with `hdb repl shot LABEL vt` and look for the escape codes.
- The capture is the tab's whole scrollback, so earlier steps reappear in later shots. Judge a step by the end of the capture.
- The tab opens in the background at whatever size the Rex window has, so the right-aligned server address lands in a different column on different machines. Don't match on line width.
- `hdb repl keys` names are case-insensitive, so `G` types `g`. Send exact text with `hdb repl type`.
- Completion with a single candidate inserts it; with several it opens a menu, and the capture shows the menu rather than a completed word.
- `\timing` toggles. In piped mode it starts off, in the shell it starts on.
- psql's own backslash commands such as `\d` query `pg_catalog`, which HamiltonDB doesn't have. Use `hamilton-cli` for those. `hdb psql` passes `-X` so the user's `.psqlrc` can't interfere.
- The database name and user a client sends are ignored.
