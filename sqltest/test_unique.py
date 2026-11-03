#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""
Step-driven multi-session test runner for hamilton-server.

Spec format (see unique_tests.spec):
  === test_name            start a new test (fresh server and database)
  # ...                    comment / description
  setup: <sql>             run on a setup connection before sessions start
  s1: <sql> [=> expect]    run <sql> in session s1 (sessions created on first use)
  s2: <await> => expect    collect the result of s2's previously blocked statement

expect:
  ok                       statement succeeded, returned no rows (default)
  rows: a|b; c|d           exact query output (columns '|', rows '; '), "rows:" = empty
  err:XXXXX                failed with SQLSTATE XXXXX
  blocks                   statement has not finished after GRACE seconds

Usage: uv run sqltest/test_unique.py [spec] [-k substring] [-v]
Builds hamilton-server first. Exits non-zero if any test fails.
"""
import argparse
import re
import sys
import tempfile
from pathlib import Path

import cli
import pgwire

SPEC = Path(__file__).resolve().parent / "unique_tests.spec"
GRACE = 0.4          # seconds a statement may run before it counts as blocked
TIMEOUT = 10.0       # hard cap when awaiting a blocked statement


def classify(results):
    for r in results:
        if isinstance(r, pgwire.Error):
            return "err:" + (r.code or r.message)
    rows = [r for r in results if isinstance(r, pgwire.Rows) and r.rows]
    if not rows:
        return "ok"
    return "rows: " + "; ".join("|".join(v or "" for v in row) for row in rows[-1].rows)


def normalize(expect):
    e = expect.strip()
    if e.startswith("rows:"):
        body = e[5:].strip()
        return "rows: " + body if body else "ok"
    return e


def parse(path):
    tests, cur = [], None
    for n, raw in enumerate(open(path), 1):
        line = raw.rstrip("\n")
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if line.startswith("==="):
            cur = {"name": line[3:].strip(), "setup": [], "steps": []}
            tests.append(cur)
            continue
        m = re.match(r"\s*(\w+):\s*(.*)$", line)
        if not m or cur is None:
            raise SyntaxError(f"{path}:{n}: cannot parse: {line}")
        who, rest = m.groups()
        sql, _, expect = rest.partition("=>")
        sql, expect = sql.strip(), normalize(expect or "ok")
        if who == "setup":
            cur["setup"].append(sql)
        else:
            cur["steps"].append((n, who, sql, expect))
    return tests


def run_test(t, verbose):
    with tempfile.TemporaryDirectory() as tmp, cli.Server(Path(tmp) / "test.db") as server:
        sessions = {}
        try:
            return run_steps(t, server, sessions, verbose)
        except ConnectionError as e:
            return [f"connection lost: {e}"]
        finally:
            for s in sessions.values():
                try:
                    s.close()
                except OSError:
                    pass


def run_steps(t, server, sessions, verbose):
    if t["setup"]:
        setup = pgwire.Connection(server.port)
        try:
            for sql in t["setup"]:
                for r in setup.query(sql):
                    if isinstance(r, pgwire.Error):
                        print(f"   !! setup: {sql}\n        {r.code}: {r.message}")
                        return [f"setup failed: {sql}"]
        finally:
            setup.close()
    pending, failures = set(), []
    for n, who, sql, expect in t["steps"]:
        if who not in sessions:
            sessions[who] = pgwire.Connection(server.port)
        s = sessions[who]
        if sql == "<await>":
            if who not in pending:
                failures.append(f"line {n}: {who} has nothing to await")
                continue
            out = s.results(TIMEOUT)
            got = "timeout" if out is None else classify(out)
            pending.discard(who)
        else:
            if who in pending:
                failures.append(f"line {n}: {who} is still blocked; add '{who}: <await>' first")
                break
            s.send(sql)
            out = s.results(GRACE)
            if out is None:
                got = "blocks"
                pending.add(who)
            else:
                got = classify(out)
        ok = got == expect
        if verbose or not ok:
            mark = "  " if ok else "!!"
            print(f"   {mark} line {n:<4} {who}: {sql}\n        expected {expect!r}, got {got!r}")
        if not ok:
            failures.append(f"line {n}: {who}: {sql}  expected {expect!r}, got {got!r}")
    return failures


def main():
    parser = argparse.ArgumentParser(
        description=__doc__.strip().split("\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("spec", nargs="?", type=Path, default=SPEC)
    parser.add_argument("-k", metavar="TEXT", help="run only tests whose name contains TEXT")
    parser.add_argument("-v", action="store_true", help="print every step, not only failures")
    args = parser.parse_args()

    cli.build()
    tests = [t for t in parse(args.spec) if not args.k or args.k in t["name"]]
    passed = 0
    for t in tests:
        f = run_test(t, args.v)
        print(("PASS " if not f else "FAIL ") + t["name"])
        passed += not f
    print(f"\n{passed}/{len(tests)} passed")
    sys.exit(passed != len(tests))


if __name__ == "__main__":
    main()
