"""The recovery suite: kills hamilton-server with SIGKILL while several
clients write, starts it again on the same files, and checks what survived.

    uv run sqltest/run.py recovery                    # every scenario
    uv run sqltest/run.py recovery --seed 7 -k ddl    # another seed, one scenario

Each scenario runs on a fresh database, served by a debug build so that
every mtr also checks its own redo. Clients send one statement per round
trip and note what the server acknowledged. After each restart:

  - a COMMIT, or a statement outside a block, that came back is there;
  - a block that rolled back, or was open at the kill, is gone;
  - a COMMIT sent with no reply is there whole, or gone;
  - every index returns the rows that a scan of its table does.

The seed fixes each client's choices and the kill delays, not how the
clients interleave. A failure prints the seed and keeps the database files.
"""
import itertools
import os
import random
import re
import shutil
import signal
import subprocess
import tempfile
import threading
import time
import traceback
from collections import Counter
from enum import Enum
from pathlib import Path

import cli
import pgwire

SERVER = cli.ROOT / "target" / "debug" / "hamilton-server"
ENV = {**os.environ, "HAMILTON_LOG": "info"}
# Under these workloads the server evicts pages, and starts a log segment
# and a checkpoint about every second.
SMALL = ["--pool-size", "256", "--log-segment-size", str(256 << 10)]
SWEPT = re.compile(r"swept (\d+) dead tuples and (\d+) index entries")
# Serialization failure and lock not available: the block rolls back, and that's all.
TOLERATED = {"40001", "55P03"}
# Set just before a kill: a client that sees it sends no COMMIT, so a block
# open at the kill counts as uncommitted rather than unknown.
KILLING = threading.Event()


class Outcome(Enum):
    COMMITTED = "committed"
    ABORTED = "rolled back"
    UNCOMMITTED = "open at the kill"
    UNKNOWN = "awaiting COMMIT's reply at the kill"


class Gone(Exception):
    """The server died during a unit of work, which it left `outcome`."""

    def __init__(self, outcome: Outcome):
        self.outcome = outcome


class Failure(Exception):
    pass


def query(conn, sql):
    [result] = conn.query(sql)
    return result


def transaction(conn, statements, commit=True, linger=0.0):
    """Runs `statements` in a block, then COMMIT, or ROLLBACK if not
    `commit`, `linger` seconds after the last. Returns the outcome and the
    statements' results."""
    results = []
    committing = False
    try:
        query(conn, "BEGIN")
        for sql in statements:
            result = query(conn, sql)
            results.append(result)
            if isinstance(result, pgwire.Error):
                query(conn, "ROLLBACK")
                if result.code not in TOLERATED:
                    raise Failure(f"{sql}: {result.message}")
                return Outcome.ABORTED, results
        if KILLING.wait(linger):
            raise Gone(Outcome.UNCOMMITTED)
        if not commit:
            query(conn, "ROLLBACK")
            return Outcome.ABORTED, results
        committing = True
        end = query(conn, "COMMIT")
    except OSError:
        raise Gone(Outcome.UNKNOWN if committing else Outcome.UNCOMMITTED) from None
    return (Outcome.COMMITTED if end == pgwire.Command("COMMIT") else Outcome.ABORTED), results


def statement(conn, sql):
    """Runs `sql` outside a block. Returns its outcome and result."""
    if KILLING.is_set():
        raise Gone(Outcome.UNCOMMITTED)
    try:
        result = query(conn, sql)
    except OSError:
        raise Gone(Outcome.UNKNOWN) from None
    if isinstance(result, pgwire.Error):
        if result.code not in TOLERATED:
            raise Failure(f"{sql}: {result.message}")
        return Outcome.ABORTED, result
    return Outcome.COMMITTED, result


def setup(conn, statements):
    for sql in statements:
        if isinstance(result := query(conn, sql), pgwire.Error):
            raise Failure(f"{sql}: {result.message}")


def read(conn, *queries) -> list[list[tuple]]:
    """The rows of each query, read in one block, so that a row left locked
    fails the read at once instead of after the engine's retries."""
    query(conn, "BEGIN")
    results = []
    for sql in queries:
        result = query(conn, sql)
        if not isinstance(result, pgwire.Rows):
            query(conn, "ROLLBACK")
            raise Failure(f"{sql}: {result}")
        results.append([tuple(row) for row in result.rows])
    query(conn, "COMMIT")
    return results


def value(conn, sql) -> str:
    [[(only,)]] = read(conn, sql)
    return only


def schema(conn) -> tuple[dict, dict]:
    """The tables, each a list of (column, type), and the indexes, each
    (table, key columns)."""
    columns, indexes = conn.query("\\schema")
    tables = {}
    for table, column, type_, _ in columns.rows:
        tables.setdefault(table, []).append((column, type_))
    return tables, {index: (table, key.split(", ")) for index, table, _, key in indexes.rows}


def child(rng: random.Random) -> random.Random:
    return random.Random(rng.getrandbits(64))


def examples(items, n=3) -> str:
    items = list(items)
    more = f" and {len(items) - n} more" if len(items) > n else ""
    return ", ".join(map(str, items[:n])) + more


def expect(failures, work: dict[str, int]):
    """Fails the scenario for each kind of work it exists to test that
    never happened."""
    failures += [f"no {what}" for what, n in work.items() if n == 0]


class Db:
    """One database's files, served by one server at a time."""

    def __init__(self, dir: Path, args=()):
        self.dir = dir
        self.path = dir / "crash.db"
        self.args = list(args)
        self.runs: list[cli.Server] = []

    @property
    def port(self) -> int:
        return self.runs[-1].port

    def spawn(self) -> cli.Server:
        KILLING.clear()
        server = cli.Server(self.path, SERVER, self.args, ENV)
        self.runs.append(server)
        server.spawn()
        return server

    def start(self) -> pgwire.Connection:
        """Starts the server, and returns a connection to it."""
        server = self.spawn()
        server.wait()
        return pgwire.Connection(server.port)

    def crash(self):
        KILLING.set()
        server = self.runs[-1]
        if (code := server.kill()) != -signal.SIGKILL:
            raise Failure(f"the server exited by itself, status {code}:\n{''.join(server.log[-20:])}")

    def stop(self):
        self.runs[-1].stop()

    def close(self):
        for server in self.runs:
            if server.proc.poll() is None:
                server.kill()

    def segments(self) -> list[int]:
        names = (re.fullmatch(r"crash\.log\.(\d+)", path.name) for path in self.dir.iterdir())
        return sorted(int(name[1]) for name in names if name)

    def log_bytes(self) -> int:
        total = 0
        for n in self.segments():
            try:
                total += (self.dir / f"crash.log.{n:08}").stat().st_size
            except FileNotFoundError:
                pass
        return total

    def swept(self) -> tuple[int, int] | None:
        """What the last open's sweep collected: dead tuples, index entries."""
        for line in self.runs[-1].log:
            if match := SWEPT.search(line):
                return int(match[1]), int(match[2])
        return None


class Clients:
    """Workers, each on its own connection, until the server dies. A worker
    is a function of a connection that loops until it raises `Gone`."""

    def __init__(self, port, workers):
        self.anomalies = []
        self.threads = [threading.Thread(target=self.run, args=(port, w)) for w in workers]
        for thread in self.threads:
            thread.start()

    def run(self, port, worker):
        try:
            worker(pgwire.Connection(port))
        except (Gone, OSError):
            pass
        except Exception as e:
            self.anomalies.append(f"a client stopped: {type(e).__name__}: {e}")

    def join(self) -> list[str]:
        for thread in self.threads:
            thread.join(60)
            if thread.is_alive():
                self.anomalies.append("a client was still running 60 s after the kill")
        return self.anomalies


def crash_while(db, workers, seconds) -> list[str]:
    """Runs `workers`, and SIGKILLs the server `seconds` later."""
    clients = Clients(db.port, workers)
    time.sleep(seconds)
    db.crash()
    return clients.join()


def crash_when(db, workers, ready: threading.Event, delay) -> list[str]:
    """Runs `workers`, and SIGKILLs the server `delay` seconds after
    `ready` is set."""
    clients = Clients(db.port, workers)
    readied = ready.wait(120)
    time.sleep(delay)
    db.crash()
    return clients.join() + ([] if readied else ["the workload wasn't ready within 120 s"])


def crash_after_send(db, conn, sql, delay) -> Outcome:
    """Sends `sql` outside a block, SIGKILLs the server `delay` seconds
    later, and returns whether the reply came first."""
    conn.send(sql)
    time.sleep(delay)
    db.crash()
    try:
        [result] = conn.results()
    except OSError:
        return Outcome.UNKNOWN
    if isinstance(result, pgwire.Error):
        raise Failure(f"{sql}: {result.message}")
    return Outcome.COMMITTED


def check_indexes(conn, only=None) -> list[str]:
    """Each index returns the rows that a scan of its table does, read in
    key order and looked up by key."""
    tables, indexes = schema(conn)
    failures = []
    query(conn, "BEGIN")
    for index, (table, key) in indexes.items():
        if only and table not in only:
            continue
        columns = [column for column, _ in tables[table]]
        select = f"SELECT {', '.join(columns)} FROM {table}"
        at = [columns.index(column) for column in key]
        scanned = Counter(planned(conn, select, f"SeqScan on {table}"))
        rows = planned(conn, f"{select} ORDER BY {', '.join(key)}", f"IndexScan using {index}")
        keys = [tuple(r[i] if tables[table][i][1].startswith("VARCHAR") else int(r[i]) for i in at) for r in rows]
        if keys != sorted(keys):
            failures.append(f"{index} returns its rows out of key order")
        ordered = Counter(rows)
        if ordered != scanned:
            failures.append(
                f"{index} misses {examples((scanned - ordered).elements())} and has extra "
                f"{examples((ordered - scanned).elements())}"
            )
            continue
        for row in sorted(scanned)[:: max(1, len(scanned) // 8)]:
            where = " AND ".join(
                f"{columns[i]} = '{row[i]}'" if tables[table][i][1].startswith("VARCHAR")
                else f"{columns[i]} = {row[i]}"
                for i in at
            )
            found = Counter(planned(conn, f"{select} WHERE {where}", f"IndexScan using {index}"))
            wanted = Counter({r: n for r, n in scanned.items() if all(r[i] == row[i] for i in at)})
            if found != wanted:
                failures.append(f"{index} finds {dict(found)} for {where}, the scan {dict(wanted)}")
    query(conn, "COMMIT")
    return failures


def planned(conn, sql, plan) -> list[tuple]:
    """The rows of `sql`, which must run as `plan`."""
    explained = "\n".join(row[0] for row in query(conn, f"EXPLAIN {sql}").rows)
    if plan not in explained:
        raise Failure(f"{sql} doesn't run as {plan}:\n{explained}")
    result = query(conn, sql)
    if not isinstance(result, pgwire.Rows):
        raise Failure(f"{sql}: {result}")
    return [tuple(row) for row in result.rows]


class Bank:
    """Transfers between accounts. Each moves money between two accounts
    and records itself in `transfers`, so a transfer that half survived
    shows as a balance its rows don't explain."""

    ACCOUNTS = 100
    START = 1000
    SCHEMA = [
        "CREATE TABLE accounts (id INT PRIMARY KEY, balance INT NOT NULL)",
        "INSERT INTO accounts VALUES " + ", ".join(map(f"({{}}, {START})".format, range(ACCOUNTS))),
        "CREATE TABLE transfers (id INT PRIMARY KEY, src INT NOT NULL, dst INT NOT NULL, amount INT NOT NULL)",
        "CREATE INDEX transfers_dst ON transfers (dst)",
    ]

    def __init__(self):
        self.ids = itertools.count()
        self.outcomes: dict[int, Outcome] = {}
        self.unknown = self.landed = 0

    def worker(self, rng):
        def body(conn):
            while True:
                tid = next(self.ids)
                src, dst = rng.sample(range(self.ACCOUNTS), 2)
                amount = rng.randint(1, 100)
                statements = [
                    f"UPDATE accounts SET balance = balance - {amount} WHERE id = {src}",
                    f"UPDATE accounts SET balance = balance + {amount} WHERE id = {dst}",
                    f"INSERT INTO transfers VALUES ({tid}, {src}, {dst}, {amount})",
                ]
                try:
                    self.outcomes[tid], _ = transaction(conn, statements, commit=rng.random() < 0.9)
                except Gone as gone:
                    self.outcomes[tid] = gone.outcome
                    raise
        return body

    def check(self, conn) -> list[str]:
        transfers, accounts = read(
            conn, "SELECT id, src, dst, amount FROM transfers", "SELECT id, balance FROM accounts"
        )
        present = {int(tid): (int(src), int(dst), int(amount)) for tid, src, dst, amount in transfers}
        failures = []
        lost = [tid for tid, o in self.outcomes.items() if o is Outcome.COMMITTED and tid not in present]
        if lost:
            failures.append(f"{len(lost)} acknowledged transfers are gone: {examples(lost)}")
        for outcome in (Outcome.ABORTED, Outcome.UNCOMMITTED):
            if back := [tid for tid, o in self.outcomes.items() if o is outcome and tid in present]:
                failures.append(f"{len(back)} transfers {outcome.value} are there: {examples(back)}")
        if strays := present.keys() - self.outcomes.keys():
            failures.append(f"transfers that no client sent are there: {examples(strays)}")
        expected = dict.fromkeys(range(self.ACCOUNTS), self.START)
        for src, dst, amount in present.values():
            expected[src] -= amount
            expected[dst] += amount
        balances = {int(account): int(balance) for account, balance in accounts}
        if wrong := [a for a in expected if balances.get(a) != expected[a]]:
            failures.append(
                f"{len(wrong)} balances disagree with the transfers there, account {wrong[0]} "
                f"holds {balances.get(wrong[0])}, not {expected[wrong[0]]}"
            )
        if (total := sum(balances.values())) != self.ACCOUNTS * self.START:
            failures.append(f"the balances add up to {total}, not {self.ACCOUNTS * self.START}")
        for tid, outcome in self.outcomes.items():
            if outcome is Outcome.UNKNOWN:
                self.unknown += 1
                self.landed += tid in present
                self.outcomes[tid] = Outcome.COMMITTED if tid in present else Outcome.ABORTED
        return failures

    def summary(self) -> str:
        counts = Counter(self.outcomes.values())
        return (
            f"{counts[Outcome.COMMITTED]} transfers committed, {counts[Outcome.ABORTED]} rolled back, "
            f"{self.landed} of {self.unknown} awaiting COMMIT's reply at a kill landed"
        )


class Keys:
    """Rows that one client each writes, so each client knows what its
    committed rows hold: blocks that insert, update or delete one to three
    of them. `kv` has a primary key and three more indexes, one unique;
    `kn` has none, so its index entries point at rids. Updates change the
    keys and the length of rows, which moves some of them."""

    RANGE = 10_000
    SCHEMA = [
        "CREATE TABLE kv (k INT PRIMARY KEY, v INT NOT NULL, u INT NOT NULL, pad VARCHAR(200) NOT NULL)",
        "CREATE INDEX kv_v ON kv (v)",
        "CREATE UNIQUE INDEX kv_u ON kv (u)",
        "CREATE INDEX kv_pad ON kv (pad)",
        "CREATE TABLE kn (k INT NOT NULL, v INT NOT NULL)",
        "CREATE INDEX kn_k ON kn (k)",
        "CREATE INDEX kn_v ON kn (v)",
    ]

    def __init__(self):
        self.rows: dict[int, dict] = {}
        self.unknown: dict[int, dict] = {}
        self.counts = Counter()

    def worker(self, client, rng, linger=0.0):
        """`linger` is the share of blocks that wait a while before they end."""
        rows = self.rows.setdefault(client, {})

        def body(conn):
            while True:
                changes, statements = {}, []
                for _ in range(rng.randint(1, 3)):
                    table = rng.choice(("kv", "kv", "kn"))
                    present = [k for t, k in rows if t == table]
                    if present and rng.random() < 0.6:
                        k = rng.choice(present)
                    else:
                        k = client * self.RANGE + rng.randrange(self.RANGE)
                    if (table, k) not in changes:
                        sql, changes[(table, k)] = change(rng, table, k, rows.get((table, k)))
                        statements.append(sql)
                pause = rng.uniform(0.2, 1.0) if rng.random() < linger else 0.0
                try:
                    outcome, _ = transaction(conn, statements, rng.random() < 0.8, pause)
                except Gone as gone:
                    self.counts[gone.outcome] += 1
                    if gone.outcome is Outcome.UNKNOWN:
                        self.unknown[client] = changes
                    raise
                self.counts[outcome] += 1
                if outcome is Outcome.COMMITTED:
                    apply(rows, changes)

        return body

    def check(self, conn) -> list[str]:
        kv, kn = read(conn, "SELECT k, v, u, pad FROM kv", "SELECT k, v FROM kn")
        failures = []
        found: dict[int, dict] = {}
        for k, v, u, pad in kv:
            found.setdefault(int(k) // self.RANGE, {})[("kv", int(k))] = (int(k), int(v), int(u), pad)
        for k, v in kn:
            mine = found.setdefault(int(k) // self.RANGE, {})
            if ("kn", int(k)) in mine:
                failures.append(f"kn holds key {k} twice")
            mine[("kn", int(k))] = (int(k), int(v))
        if strays := found.keys() - self.rows.keys():
            failures.append(f"rows of clients that never ran: {examples(strays)}")
        for client, rows in self.rows.items():
            got = found.get(client, {})
            allowed = [dict(rows)]
            if client in self.unknown:
                allowed.append(apply(dict(rows), self.unknown.pop(client)))
            if got not in allowed:
                wrong = sorted(k for k in got.keys() | rows.keys() if got.get(k) != rows.get(k))
                failures.append(
                    f"client {client}: {len(wrong)} rows aren't what it committed, "
                    f"{wrong[0]} is {got.get(wrong[0])}, not {rows.get(wrong[0])}"
                )
            rows.clear()
            rows.update(got)
        return failures

    def summary(self) -> str:
        return ", ".join(f"{self.counts[o]} blocks {o.value}" for o in Outcome)


def change(rng, table, k, old):
    """A statement that inserts, updates or deletes the row of `table` at
    `k`, which now holds `old`, and the row it leaves: None if none."""
    if old is None:
        if table == "kn":
            row = (k, rng.randrange(100))
            return f"INSERT INTO kn VALUES {row}", row
        row = (k, rng.randrange(100), k * 1000, pad(rng))
        return f"INSERT INTO kv VALUES {row}", row
    if rng.random() < 0.25:
        return f"DELETE FROM {table} WHERE k = {k}", None
    if table == "kn":
        row = (k, rng.randrange(100))
        return f"UPDATE kn SET v = {row[1]} WHERE k = {k}", row
    row = (k, rng.randrange(100), k * 1000 + (old[2] + 1) % 1000, pad(rng))
    return f"UPDATE kv SET v = {row[1]}, u = {row[2]}, pad = '{row[3]}' WHERE k = {k}", row


def pad(rng) -> str:
    return "".join(rng.choices("abcdefgh", k=rng.randrange(200)))


def apply(rows: dict, changes: dict) -> dict:
    for key, row in changes.items():
        if row is None:
            rows.pop(key, None)
        else:
            rows[key] = row
    return rows


class Ids:
    """Rows that take their id from an identity column, inserted one to
    four at a time, in blocks and outside them; RETURNING says which ids.
    The ids aren't a key, so the table can show a duplicate."""

    SCHEMA = [
        "CREATE TABLE ids (id INT GENERATED BY DEFAULT AS IDENTITY, client INT NOT NULL, n INT NOT NULL)",
        "CREATE INDEX ids_id ON ids (id)",
    ]

    def __init__(self):
        self.n = itertools.count()
        self.batches: list[tuple[list, list | None, Outcome]] = []
        self.restarts: list[tuple[int, int]] = []

    def worker(self, client, rng):
        def body(conn):
            while True:
                batch = [(client, next(self.n)) for _ in range(rng.randint(1, 4))]
                values = ", ".join(map(str, batch))
                sql = f"INSERT INTO ids (client, n) VALUES {values} RETURNING id"
                try:
                    if rng.random() < 0.5:
                        outcome, result = statement(conn, sql)
                    else:
                        outcome, results = transaction(conn, [sql], rng.random() < 0.8)
                        result = results[0]
                except Gone as gone:
                    self.batches.append((batch, None, gone.outcome))
                    raise
                ids = [int(row[0]) for row in result.rows] if isinstance(result, pgwire.Rows) else None
                self.batches.append((batch, ids, outcome))
        return body

    def check(self, conn) -> list[str]:
        [present] = read(conn, "SELECT id, client, n FROM ids")
        failures = []
        held = Counter(int(id) for id, _, _ in present)
        if twice := [id for id, n in held.items() if n > 1]:
            failures.append(f"{len(twice)} ids are held by more than one row: {examples(twice)}")
        ids = {(int(client), int(n)): int(id) for id, client, n in present}
        sent = set()
        wrong, half, back = [], [], []
        for at, (batch, returned, outcome) in enumerate(self.batches):
            sent.update(batch)
            held_by = [ids.get(key) for key in batch]
            if outcome is Outcome.COMMITTED and held_by != returned:
                wrong.append(f"{batch} acknowledged with {returned} holds {held_by}")
            elif outcome is Outcome.UNKNOWN:
                landed = None not in held_by
                if not landed and held_by.count(None) < len(batch):
                    half.append(batch)
                self.batches[at] = (batch, held_by, Outcome.COMMITTED if landed else Outcome.ABORTED)
            elif outcome is not Outcome.COMMITTED and held_by.count(None) < len(batch):
                back.append(f"{batch} {outcome.value}")
        for kind, items in [
            ("acknowledged inserts aren't there as acknowledged", wrong),
            ("inserts are there in part", half),
            ("inserts that never committed are there", back),
        ]:
            if items:
                failures.append(f"{len(items)} {kind}: {examples(items, 2)}")
        if strays := ids.keys() - sent:
            failures.append(f"rows that no client inserted are there: {examples(strays)}")
        top = max([*held, *(i for _, r, o in self.batches if o is Outcome.COMMITTED for i in r)], default=0)
        probe = [(-1, next(self.n)) for _ in range(3)]
        result = query(conn, f"INSERT INTO ids (client, n) VALUES {', '.join(map(str, probe))} RETURNING id")
        if not isinstance(result, pgwire.Rows):
            return failures + [f"inserting after the restart failed: {result}"]
        new = [int(row[0]) for row in result.rows]
        self.batches.append((probe, new, Outcome.COMMITTED))
        self.restarts.append((top, min(new)))
        if min(new) <= top:
            failures.append(f"after the restart the identity column handed out {min(new)}, "
                            f"and rows held ids up to {top}")
        return failures

    def summary(self) -> str:
        committed = sum(len(b) for b, _, o in self.batches if o is Outcome.COMMITTED)
        after = ", ".join(f"{top} then {new}" for top, new in self.restarts)
        return f"{committed} ids committed; largest id before each restart, then the first after: {after}"


class Ddl:
    """Tables and indexes made one statement at a time, with three rows in
    each table, all named from `prefix`."""

    COLUMNS = [("id", "INT"), ("v", "INT"), ("s", "VARCHAR(20)")]

    def __init__(self, prefix):
        self.prefix = prefix
        self.done: list[tuple[str, str, str, Outcome]] = []

    def worker(self, conn):
        for i in itertools.count():
            t = f"{self.prefix}{i}"
            for name, sql in [
                (t, f"CREATE TABLE {t} (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, "
                    "v INT NOT NULL, s VARCHAR(20) NOT NULL)"),
                (None, f"INSERT INTO {t} (v, s) VALUES (1, 'a'), (2, 'b'), (3, 'c')"),
                (f"{t}_v", f"CREATE INDEX {t}_v ON {t} (v)"),
                (f"{t}_s", f"CREATE UNIQUE INDEX {t}_s ON {t} (s)"),
            ]:
                try:
                    outcome, _ = statement(conn, sql)
                except Gone as gone:
                    self.done.append((t, name, sql, gone.outcome))
                    raise
                self.done.append((t, name, sql, outcome))

    def check(self, conn) -> list[str]:
        tables, indexes = schema(conn)
        failures = []
        for table, name, sql, outcome in self.done:
            if name is None:
                allowed = {Outcome.COMMITTED: {"3"}, Outcome.UNKNOWN: {"0", "3"}}.get(outcome, {"0"})
                if table in tables and (count := value(conn, f"SELECT count(*) FROM {table}")) not in allowed:
                    failures.append(f"{table} holds {count} rows after {sql!r}, which was {outcome.value}")
                continue
            there = name in tables or name in indexes
            if not there and outcome is Outcome.COMMITTED:
                failures.append(f"{sql!r} was acknowledged, and {name} is gone")
            elif there and outcome in (Outcome.UNCOMMITTED, Outcome.ABORTED):
                failures.append(f"{name} is there, but {sql!r} was {outcome.value}")
            elif not there and outcome is Outcome.UNKNOWN and (name == table or table in tables):
                if statement(conn, sql)[0] is not Outcome.COMMITTED:
                    failures.append(f"{sql!r} fails after the restart")
        made = {t for t, *_ in self.done} | {n for _, n, *_ in self.done}
        tables, indexes = schema(conn)
        for table, columns in tables.items():
            if not table.startswith(self.prefix):
                continue
            if table not in made:
                failures.append(f"{table} is there, but was never made")
            if columns != self.COLUMNS or f"{table}_pkey" not in indexes:
                failures.append(f"{table} is there, but not whole: {columns}")
            elif statement(conn, f"INSERT INTO {table} (v, s) VALUES (9, 'probe')")[0] is not Outcome.COMMITTED:
                failures.append(f"{table} takes no rows")
        strays = [i for i in indexes if i.startswith(self.prefix) and i not in made and not i.endswith("_pkey")]
        failures += [f"{i} is there, but was never made" for i in strays]
        return failures

    def acknowledged(self) -> int:
        return sum(o is Outcome.COMMITTED for *_, o in self.done)

    def cut(self) -> int:
        """CREATE statements sent and not answered at the kill."""
        return sum(name is not None and o is Outcome.UNKNOWN for _, name, _, o in self.done)

    def summary(self) -> str:
        if not self.done:
            return "no DDL statements sent"
        _, _, sql, outcome = self.done[-1]
        return f"{self.acknowledged()} DDL statements acknowledged, the kill caught {sql.split(' (')[0]!r} {outcome.value}"


def acknowledged_commits_survive(rng, db, failures, notes):
    """Eight clients transfer money until a kill 2 to 4 s in."""
    setup(db.start(), Bank.SCHEMA)
    bank = Bank()
    failures += crash_while(db, [bank.worker(child(rng)) for _ in range(8)], rng.uniform(2, 4))
    at_kill = Counter(bank.outcomes.values())
    expect(failures, {
        "transfer committed": at_kill[Outcome.COMMITTED],
        "transfer rolled back": at_kill[Outcome.ABORTED],
        "transfer in flight at the kill": at_kill[Outcome.UNCOMMITTED] + at_kill[Outcome.UNKNOWN],
    })
    conn = db.start()
    failures += bank.check(conn)
    failures += check_indexes(conn)
    notes.append(bank.summary())


def rolled_back_and_open_blocks_leave_nothing(rng, db, failures, notes):
    """Six clients write their own rows until a kill 2 to 4 s in. A fifth of
    the blocks roll back, and three clients keep half their blocks open for
    up to a second."""
    setup(db.start(), Keys.SCHEMA)
    keys = Keys()
    workers = [keys.worker(c, child(rng), linger=0.5 if c < 3 else 0.0) for c in range(6)]
    failures += crash_while(db, workers, rng.uniform(2, 4))
    expect(failures, {
        "block committed": keys.counts[Outcome.COMMITTED],
        "block rolled back": keys.counts[Outcome.ABORTED],
        "block open at the kill": keys.counts[Outcome.UNCOMMITTED],
    })
    conn = db.start()
    failures += keys.check(conn)
    failures += check_indexes(conn)
    notes.append(keys.summary())


def indexes_agree_with_tables(rng, db, failures, notes):
    """Two crashes of six clients writing rows whose index keys change;
    after each, every index is read in order and searched, and the unique
    one still refuses a duplicate."""
    setup(db.start(), Keys.SCHEMA)
    keys = Keys()
    for _ in range(2):
        committed = keys.counts[Outcome.COMMITTED]
        failures += crash_while(db, [keys.worker(c, child(rng)) for c in range(6)], rng.uniform(1.5, 3))
        expect(failures, {"block committed before the kill": keys.counts[Outcome.COMMITTED] - committed})
        conn = db.start()
        failures += keys.check(conn)
        failures += check_indexes(conn)
        rows = int(value(conn, "SELECT count(*) FROM kv"))
        expect(failures, {"row in kv after the restart": rows})
        if rows:
            u = value(conn, "SELECT max(u) FROM kv")
            duplicate = query(conn, f"INSERT INTO kv VALUES (-1, 0, {u}, '')")
            if not (isinstance(duplicate, pgwire.Error) and duplicate.code == "23505"):
                failures.append(f"kv_u took a second row with u = {u}: {duplicate}")
        notes.append(f"{rows} rows in kv after a restart")
    notes.append(keys.summary())


def ddl_is_all_or_nothing(rng, db, failures, notes):
    """CREATE TABLE and CREATE INDEX from one client while three write
    rows, until a kill cuts off a CREATE; then CREATE INDEX on 20,000 rows,
    killed partway, twice; then TRUNCATE killed four ways."""
    setup(db.start(), Keys.SCHEMA)
    keys = Keys()
    acknowledged = 0
    for attempt in range(1, 7):
        ddl = Ddl(f"ddl{attempt}_")
        workers = [ddl.worker] + [keys.worker(c, child(rng)) for c in range(3)]
        failures += crash_while(db, workers, rng.uniform(0.5, 1.5))
        conn = db.start()
        failures += ddl.check(conn)
        failures += keys.check(conn)
        failures += check_indexes(conn)
        acknowledged += ddl.acknowledged()
        if ddl.cut():
            break
    notes.append(f"{ddl.summary()}, on kill {attempt}")
    expect(failures, {"DDL statement acknowledged": acknowledged, f"CREATE cut off in {attempt} kills": ddl.cut()})
    big_index_builds(rng, db, conn, failures, notes)
    truncates(rng, db, pgwire.Connection(db.port), failures, notes)


def big_index_builds(rng, db, conn, failures, notes):
    rows = 20_000
    setup(conn, ["CREATE TABLE big (id INT PRIMARY KEY, k INT NOT NULL, n INT NOT NULL, "
                 "s VARCHAR(40) NOT NULL)"])
    for first in range(0, rows, 500):
        values = ", ".join(f"({i}, {i * 7919 % rows}, {i * 31 % rows}, 'row {i * 104729 % rows:05}')"
                           for i in range(first, first + 500))
        setup(conn, [f"INSERT INTO big VALUES {values}"])
    started = time.monotonic()
    setup(conn, ["CREATE INDEX big_s ON big (s)"])
    took = time.monotonic() - started
    builds = []
    for base, create in [("big_k", "CREATE INDEX {} ON big (k)"), ("big_n", "CREATE UNIQUE INDEX {} ON big (n)")]:
        for attempt in range(3):
            name = f"{base}{attempt}"
            sql = create.format(name)
            delay = rng.uniform(0.05, 0.9) * took / 2**attempt
            outcome = crash_after_send(db, conn, sql, delay)
            conn = db.start()
            there = name in schema(conn)[1]
            if outcome is Outcome.COMMITTED and not there:
                failures.append(f"{sql!r} was acknowledged, and {name} is gone")
            if not there:
                setup(conn, [sql])
            failures += check_indexes(conn, only={"big"})
            builds.append(f"{name} killed {delay * 1000:.0f} ms in, {'there' if there else 'gone'} after")
            if not there:
                break
        else:
            failures.append(f"three kills during CREATE INDEX on big all came after it finished")
    notes.append(f"CREATE INDEX on {rows} rows takes ~{took * 1000:.0f} ms: {', '.join(builds)}")


def truncates(rng, db, conn, failures, notes):
    setup(conn, [
        "CREATE TABLE tr (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v INT NOT NULL)",
        "CREATE INDEX tr_v ON tr (v)",
        "CREATE TABLE elsewhere (n INT NOT NULL)",
    ])
    rows = 2000
    # label, statements, whether the block commits (None: no block), whether
    # an older block holds back the End, so recovery frees the old storage
    cases = [
        ("TRUNCATE while an older block runs", ["TRUNCATE tr"], None, True),
        ("TRUNCATE RESTART IDENTITY", ["TRUNCATE tr RESTART IDENTITY"], None, False),
        ("an open block", ["TRUNCATE tr RESTART IDENTITY", "INSERT INTO tr (v) VALUES (1), (2), (3)"], False, False),
        ("a committed block while an older block runs",
         ["TRUNCATE tr", "INSERT INTO tr (v) VALUES (5) RETURNING id, v"], True, True),
    ]
    outcomes = []
    for label, statements, commit, held in cases:
        if int(value(conn, "SELECT count(*) FROM tr")) < rows:
            setup(conn, ["INSERT INTO tr (v) VALUES " + ", ".join(f"({i % 97})" for i in range(rows))])
        [before] = read(conn, "SELECT id, v FROM tr")
        after = []
        if held:
            # Garbage collection frees the replaced storage, and logs the
            # truncate's End, only once this block ends.
            setup(pgwire.Connection(db.port), ["BEGIN", "SELECT count(*) FROM kv"])
        if commit is None and held:
            setup(conn, statements)
            outcome = Outcome.COMMITTED
            db.crash()
        elif commit is None:
            outcome = crash_after_send(db, conn, statements[0], rng.uniform(0, 0.003))
        elif commit:
            outcome, results = transaction(conn, statements)
            after = [tuple(row) for row in results[-1].rows]
            db.crash()
        else:
            setup(conn, ["BEGIN", *statements])
            # Another commit syncs the open block's records, so that recovery has them to undo.
            setup(pgwire.Connection(db.port), ["INSERT INTO elsewhere VALUES (1)"])
            outcome = Outcome.UNCOMMITTED
            db.crash()
        conn = db.start()
        [got] = read(conn, "SELECT id, v FROM tr")
        took = sorted(got) == sorted(after)
        if not took and sorted(got) != sorted(before):
            failures.append(f"{label}: tr holds {len(got)} rows, neither its {len(before)} before "
                            f"nor {len(after)} after")
        if took and outcome is not Outcome.COMMITTED and outcome is not Outcome.UNKNOWN \
                or not took and outcome is Outcome.COMMITTED:
            failures.append(f"{label}: the truncate was {outcome.value}, "
                            f"and {'took' if took else 'did not take'}")
        if held and outcome is not Outcome.COMMITTED:
            failures.append(f"{label}: the truncate was {outcome.value}, not committed")
        [[new]] = query(conn, "INSERT INTO tr (v) VALUES (0) RETURNING id").rows
        restarted = took and "RESTART IDENTITY" in statements[0]
        top = max([int(id) for id, _ in got], default=0)
        if restarted and int(new) != 1 + len(after) or not restarted and int(new) <= top:
            failures.append(f"{label}: the next id is {new}, with ids up to {top} there")
        failures += check_indexes(conn, only={"tr"})
        outcomes.append(f"{label} {'took' if took else 'was undone'} on {len(before)} rows")
    notes.append(f"truncates: {', '.join(outcomes)}")


def sequences_never_repeat(rng, db, failures, notes):
    """Two crashes of six clients inserting rows that take their ids from
    an identity column."""
    setup(db.start(), Ids.SCHEMA)
    ids = Ids()
    for _ in range(2):
        sent = len(ids.batches)
        failures += crash_while(db, [ids.worker(c, child(rng)) for c in range(6)], rng.uniform(1.5, 3))
        batches = ids.batches[sent:]
        expect(failures, {
            "id committed before the kill": sum(len(b) for b, _, o in batches if o is Outcome.COMMITTED),
            "insert in flight at the kill": sum(o in (Outcome.UNCOMMITTED, Outcome.UNKNOWN) for *_, o in batches),
        })
        conn = db.start()
        failures += ids.check(conn)
        failures += check_indexes(conn)
    notes.append(ids.summary())


def repeated_crashes(rng, db, failures, notes):
    """Three crashes of a mixed workload; then a crash with a block open
    that inserted 20,000 rows, and two kills while the server opens, the
    first while recovery undoes that block and the second during the sweep;
    then a clean restart, whose sweep must find nothing."""
    setup(db.start(), Bank.SCHEMA + Keys.SCHEMA + Ids.SCHEMA
          + ["CREATE TABLE bulk (id INT PRIMARY KEY, pad VARCHAR(40) NOT NULL)"])
    bank, keys, ids = Bank(), Keys(), Ids()

    def workers():
        return ([bank.worker(child(rng)) for _ in range(4)]
                + [keys.worker(c, child(rng), linger=0.3) for c in range(3)]
                + [ids.worker(c, child(rng)) for c in range(2)])

    def check(conn):
        for check in (bank.check, keys.check, ids.check, check_indexes):
            failures.extend(check(conn))

    for _ in range(3):
        failures += crash_while(db, workers(), rng.uniform(1, 2.5))
        check(db.start())
    expect(failures, {"transfer committed": Counter(bank.outcomes.values())[Outcome.COMMITTED]})

    for attempt in range(1, 4):
        loaded = threading.Event()

        def bulk(conn):
            try:
                query(conn, "BEGIN")
                for first in range(0, 20_000, 500):
                    values = ", ".join(f"({i}, 'bulk row {i}')" for i in range(first, first + 500))
                    if isinstance(result := query(conn, f"INSERT INTO bulk VALUES {values}"), pgwire.Error):
                        raise Failure(f"bulk insert: {result.message}")
            finally:
                loaded.set()
            KILLING.wait()

        failures += crash_when(db, workers() + [bulk], loaded, rng.uniform(0, 0.3))
        phases = [kill_during_recovery(rng, db), kill_during_sweep(rng, db)]
        conn = db.start()
        check(conn)
        if (left := value(conn, "SELECT count(*) FROM bulk")) != "0":
            failures.append(f"{left} rows of the block open at the kill are there")
        if phases == ["during recovery", "during the sweep"]:
            break
    else:
        failures.append(f"in 3 tries the kills while opening never landed during recovery, then "
                        f"during the sweep; the last landed {' and '.join(phases)}")
    db.stop()
    conn = db.start()
    if (swept := db.swept()) != (0, 0):
        failures.append(f"a clean restart's sweep collected {swept} tuples and entries, not none")
    check(conn)
    notes.append(bank.summary())
    notes.append(f"kills while opening landed {' and '.join(phases)} on try {attempt}; "
                 f"the clean restart swept {swept}")


def kill_during_recovery(rng, db) -> str:
    """Starts the server, and SIGKILLs it once recovery has added a few KiB
    to the log, which only undo does. Returns where the kill landed."""
    size = db.log_bytes()
    server = db.spawn()
    grow = rng.randrange(4 << 10, 128 << 10)
    wait_for("neither did recovery add to the log nor end",
             lambda: db.log_bytes() >= size + grow or logged(server, "recovered in") or server.proc.poll() is not None)
    db.crash()
    landed = phase(server)
    return landed if landed != "during recovery" or db.log_bytes() > size else "before recovery added to the log"


def kill_during_sweep(rng, db) -> str:
    """Starts the server, and SIGKILLs it once the sweep has added a few KiB
    to the log. Returns where the kill landed."""
    server = db.spawn()
    wait_for("recovery didn't end", lambda: logged(server, "recovered in") or server.proc.poll() is not None)
    size = db.log_bytes()
    grow = rng.randrange(1 << 10, 64 << 10)
    wait_for("neither did the sweep add to the log nor end",
             lambda: db.log_bytes() >= size + grow or logged(server, "swept") or server.proc.poll() is not None)
    db.crash()
    return phase(server)


def logged(server, text) -> bool:
    return any(text in line for line in server.log)


def wait_for(failure, done, seconds=60):
    """Polls `done` every millisecond, and fails with `failure` if it isn't
    true within `seconds`, so that a reworded log line fails the scenario
    instead of hanging it."""
    deadline = time.monotonic() + seconds
    while not done():
        if time.monotonic() > deadline:
            raise Failure(f"{failure} within {seconds} s")
        time.sleep(0.001)


def phase(server) -> str:
    log = "".join(server.log)
    if "listening on" in log:
        return "after opening"
    if "swept" in log:
        return "after the sweep"
    return "during the sweep" if "recovered in" in log else "during recovery"


def checkpoints_keep_the_log_bounded(rng, db, failures, notes):
    """Ten clients write with 256 KiB log segments until eight checkpoints
    have run. The log must roll over many segments while checkpoints remove
    old ones, and recovery must remove every segment but the last."""
    setup(db.start(), Bank.SCHEMA + Keys.SCHEMA)
    bank, keys = Bank(), Keys()
    samples = []
    watching = threading.Event()

    def watch():
        while not watching.is_set():
            try:
                meta = (db.dir / "crash.meta").stat()
                samples.append((db.segments(), (meta.st_ino, meta.st_mtime_ns)))
            except FileNotFoundError:
                pass
            time.sleep(0.05)

    def checkpoints() -> int:
        return len({meta for _, meta in samples}) - 1

    watcher = threading.Thread(target=watch)
    watcher.start()
    workers = [bank.worker(child(rng)) for _ in range(6)] + [keys.worker(c, child(rng)) for c in range(4)]
    started = time.monotonic()
    try:
        clients = Clients(db.port, workers)
        while checkpoints() < 8 and time.monotonic() - started < 60:
            time.sleep(0.1)
        time.sleep(rng.uniform(0, 1))
        db.crash()
        failures += clients.join()
    finally:
        watching.set()
        watcher.join()
    seconds = time.monotonic() - started
    written = max(segments[-1] for segments, _ in samples if segments)
    most = max(len(segments) for segments, _ in samples)
    seen = checkpoints()
    if written < 8 or seen < 8:
        failures.append(f"only {written} segments and {seen} checkpoints in {seconds:.0f} s")
    # A checkpoint keeps the log back to about the previous one's start, so two
    # intervals' worth plus the segment being written; three intervals leave
    # room for a slow disk, and a log that no checkpoint trims still exceeds it.
    if most > 3 * written / max(seen, 1) + 2:
        failures.append(f"{most} of the {written} log segments written were on disk at once, "
                        f"with {seen} checkpoints")
    before = db.segments()
    conn = db.start()
    failures += bank.check(conn)
    failures += keys.check(conn)
    failures += check_indexes(conn)
    after = db.segments()
    # The sweep that follows recovery can start segments of its own.
    if after[0] < before[-1]:
        failures.append(f"recovery left segments {after[0]} to {before[-1] - 1} of {before[0]} to {before[-1]}")
    notes.append(f"segments 1 to {written} written in {seconds:.0f} s, at most {most} on disk at once, "
                 f"{seen} checkpoints seen, segments {after[0]} to {after[-1]} left after reopening")
    notes.append(bank.summary())


SCENARIOS = [
    ("acknowledged commits survive", acknowledged_commits_survive, []),
    ("rolled-back and open blocks leave nothing", rolled_back_and_open_blocks_leave_nothing, SMALL),
    ("indexes agree with tables", indexes_agree_with_tables, SMALL),
    ("ddl is all or nothing", ddl_is_all_or_nothing, []),
    ("sequences never repeat", sequences_never_repeat, SMALL),
    ("repeated crashes", repeated_crashes, SMALL),
    ("checkpoints keep the log bounded", checkpoints_keep_the_log_bounded, SMALL),
]


def run(args) -> tuple[int, int]:
    """Runs the scenarios whose names contain `args.k`. Returns how many ran
    and how many failed."""
    scenarios = [s for s in SCENARIOS if not args.k or args.k in s[0]]
    if not scenarios:
        return 0, 0
    subprocess.run(["cargo", "build", "-q", "-p", "hamilton-server"], cwd=cli.ROOT, check=True)
    print(f"\nrecovery (seed {args.seed})\n")
    failed = 0
    for name, scenario, server_args in scenarios:
        dir = Path(tempfile.mkdtemp(prefix="hamilton-recovery-"))
        db = Db(dir, server_args)
        failures, notes = [], []
        started = time.monotonic()
        try:
            scenario(random.Random(f"{args.seed}:{name}"), db, failures, notes)
        except (Failure, RuntimeError) as e:
            failures.append(str(e))
        except Exception:
            failures.append(traceback.format_exc())
        finally:
            db.close()
        print(f"  {'FAIL' if failures else 'ok  '}  {name} ({time.monotonic() - started:.1f} s)")
        for line in notes + failures:
            print("        " + line.replace("\n", "\n        "))
        if failures:
            failed += 1
            print(f"        replay: uv run sqltest/run.py recovery --seed {args.seed} -k {name!r}")
            print(f"        files and server logs kept in {dir}")
            for i, server in enumerate(db.runs):
                (dir / f"server-{i}.log").write_text("".join(server.log))
        else:
            shutil.rmtree(dir)
    return len(scenarios), failed
