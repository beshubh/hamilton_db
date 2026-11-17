"""Runs a `\\session` script (the sqltest/transactions.sql statement format)
against a running hamilton-server and prints what every statement returned.

    python3 sessions.py SQLTEST_DIR PORT < script.sql

Each `\\session NAME` line switches connection, opened on first use. A
statement takes one line and may end with `-- => EXPECTED`. Exits 1 if any
expectation fails; a statement with none must not error.
"""
import sys

sys.path.insert(0, sys.argv[1])
import pgwire  # noqa: E402
import transactions  # noqa: E402

port = int(sys.argv[2])
sessions = {"main": pgwire.Connection(port)}
name, failed = "main", 0
for raw in sys.stdin.read().splitlines():
    text = raw.strip()
    if not text or text.startswith("--"):
        continue
    if text.startswith("\\session"):
        name = text.removeprefix("\\session").strip()
        if name not in sessions:
            sessions[name] = pgwire.Connection(port)
        continue
    match = transactions.STATEMENT.match(text)
    if not match:
        sys.exit(f"not a one-line statement ending in ';': {text}")
    sql, expected = match.groups()
    actual = transactions.result(sessions[name].query(sql))
    ok = transactions.matches(expected, actual)
    failed += not ok
    print(f"[{name}] {sql}\n    => {actual}" + ("" if ok else f"\n    MISMATCH, expected: {expected or 'success'}"))
for conn in sessions.values():
    conn.close()
print(f"\n{failed} mismatch(es)")
sys.exit(1 if failed else 0)
