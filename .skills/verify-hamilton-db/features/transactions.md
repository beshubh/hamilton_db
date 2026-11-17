# Transactions

`BEGIN` opens a transaction on a connection; `COMMIT` makes its writes visible and `ROLLBACK` discards them. Concurrent transactions run under MVTO: the outcome must equal running them in `BEGIN` order, so when two clash, one statement fails with a conflict and that transaction rolls back.

## Sub-features

- `txn-commit` makes a transaction's writes visible to other connections only after `COMMIT`.
- `txn-rollback` discards a transaction's writes.
- `txn-snapshot` keeps a transaction's reads stable while a newer one updates and commits.
- `txn-conflict` fails an older transaction's write to a row a newer one has read, and its `COMMIT` reports `ROLLBACK`.
- `txn-anomalies` prevents non-repeatable reads, lost updates and write skew.

## How to get to it (user POV)

- `BEGIN;` … `COMMIT;` or `ROLLBACK;` on any connection: `hamilton-cli`, `psql`, or any Postgres client.
- Concurrency needs two or more connections open at once, such as two shells.

## Driving it with hdb

Preconditions:

- `hdb doctor` exits 0.
- No table named `tx_accounts` exists. Create it once with `hdb sql txn-setup`:

  ```sql
  CREATE TABLE tx_accounts (id INT PRIMARY KEY, balance INT NOT NULL);
  INSERT INTO tx_accounts VALUES (1, 100), (2, 100);
  ```

- **Rollback.** Run `hdb sql txn-rollback`. Inside the transaction the read shows `0`; after `ROLLBACK` it shows `100`.

  ```sql
  BEGIN;
  UPDATE tx_accounts SET balance = 0 WHERE id = 2;
  SELECT balance FROM tx_accounts WHERE id = 2;
  ROLLBACK;
  SELECT balance FROM tx_accounts WHERE id = 2;
  ```

- **Commit visibility and snapshot.** Run `hdb sessions txn-snapshot`. Every line matches its `-- =>`; the transcript ends `0 mismatch(es)`. Session `a` keeps reading `100` until it commits, then reads `50`.

  ```sql
  \session a
  BEGIN;
  SELECT balance FROM tx_accounts WHERE id = 1;            -- => 100
  \session b
  BEGIN;
  UPDATE tx_accounts SET balance = 50 WHERE id = 1;        -- => UPDATE 1
  \session a
  SELECT balance FROM tx_accounts WHERE id = 1;            -- => 100
  \session b
  COMMIT;                                                  -- => COMMIT
  \session a
  SELECT balance FROM tx_accounts WHERE id = 1;            -- => 100
  COMMIT;
  SELECT balance FROM tx_accounts WHERE id = 1;            -- => 50
  ```

- **Conflict.** Run `hdb sessions txn-conflict`. The older transaction's `UPDATE` returns an error containing `conflicts`, its `COMMIT` returns `ROLLBACK`, and the balance is unchanged. Ends `0 mismatch(es)`.

  ```sql
  \session b
  BEGIN;
  \session a
  BEGIN;
  SELECT balance FROM tx_accounts WHERE id = 2;            -- => 100
  \session b
  UPDATE tx_accounts SET balance = 7 WHERE id = 2;         -- => error: conflicts
  \session a
  COMMIT;
  \session b
  COMMIT;                                                  -- => ROLLBACK
  SELECT balance FROM tx_accounts WHERE id = 2;            -- => 100
  ```

- **Anomaly suite.** Run `uv run sqltest/run.py transactions`. It starts a fresh server per case; every case prints `ok`. Use `-k "lost update"` to run the cases whose name contains the text.

## Gotchas

- A transaction's age is fixed when its `BEGIN` runs, not at its first read or write. The order of the `BEGIN` lines in a script decides who loses a conflict.
- After a conflict error the transaction is already dead: its `COMMIT` answers `ROLLBACK`, which is success for the journey, not a failure.
- `hdb sql` is one connection, so it can only prove single-session behavior. Anything about isolation needs `hdb sessions`.
- `hdb sessions` needs one statement per line and waits for each result before sending the next, so it can't show a statement blocking on another.
- Sessions in a script stay open until the script ends. A `BEGIN` left open there is closed with the connection, but don't rely on that for the state the next recipe needs: end every transaction explicitly.
- The conflict message names internal tuple and transaction ids that change run to run. Match on `conflicts`.
