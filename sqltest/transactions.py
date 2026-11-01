"""The transactions suite: runs transactions.sql and checks each statement's
result.

The file is a schema, then cases. Each case starts with `-- case: <name>`
and runs on a fresh database with the schema loaded. Statements take one
line each, and may end with their expected result:

    SELECT balance FROM accounts WHERE id = 1;   -- => 100

    -- => 100                 rows: values joined by ", ", rows by "; "
    -- => UPDATE 1            a command tag
    -- => error: conflicts    an error containing the text

A statement with no `-- =>` must not fail. A `\\session x` line sends the
statements after it down connection x, opened on first use; until the
first, they go down `main`, as the schema does. Each statement waits for
its result before the next is sent, so the order of statements across
connections is the order in the file.
"""
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path

import cli
import pgwire

FILE = Path(__file__).resolve().parent / "transactions.sql"
STATEMENT = re.compile(r"^(.*?;)\s*(?:--\s*=>\s*(.*))?$")


@dataclass
class Line:
    number: int
    text: str
    expected: str | None = None


def parse(path: Path) -> tuple[str, list[tuple[str, list[Line]]]]:
    schema, cases = [], []
    for number, text in enumerate(path.read_text().splitlines(), 1):
        if text.startswith("-- case: "):
            cases.append((text.removeprefix("-- case: ").strip(), []))
        elif not text.strip() or text.startswith("--"):
            continue
        elif not cases:
            schema.append(text)
        elif text.startswith("\\"):
            cases[-1][1].append(Line(number, text.strip()))
        else:
            sql, expected = STATEMENT.match(text.strip()).groups()
            cases[-1][1].append(Line(number, sql, expected))
    return "\n".join(schema), cases


def result(results: list) -> str:
    """A statement's result, in the `-- =>` form."""
    if len(results) != 1:
        return f"{len(results)} results: {results}"
    match results[0]:
        case pgwire.Error(message=message):
            return f"error: {message}"
        case pgwire.Rows(rows=rows):
            return "; ".join(", ".join(v or "" for v in row) for row in rows) or "(no rows)"
        case pgwire.Command(tag=tag):
            # As the CLI prints it, without INSERT's oid.
            return re.sub(r"^INSERT 0 ", "INSERT ", tag)


def matches(expected: str | None, actual: str) -> bool:
    if expected is None:
        return not actual.startswith("error:")
    if expected.startswith("error:"):
        return actual.startswith("error:") and expected.removeprefix("error:").strip() in actual
    return actual == expected


def run_case(schema: str, lines: list[Line]) -> list[str]:
    """Runs a case on a fresh database and returns its failures."""
    with tempfile.TemporaryDirectory() as tmp, cli.Server(Path(tmp) / "test.db") as server:
        sessions = {"main": pgwire.Connection(server.port)}
        try:
            return run_lines(server, sessions, schema, lines)
        finally:
            for conn in sessions.values():
                conn.close()


def run_lines(server, sessions: dict, schema: str, lines: list[Line]) -> list[str]:
    errors = [r.message for r in sessions["main"].query(schema) if isinstance(r, pgwire.Error)]
    failures = [f"schema: {e}" for e in errors]
    conn = sessions["main"]
    for line in lines:
        if line.text.startswith("\\session"):
            name = line.text.removeprefix("\\session").strip()
            if name not in sessions:
                sessions[name] = pgwire.Connection(server.port)
            conn = sessions[name]
            continue
        actual = result(conn.query(line.text))
        if not matches(line.expected, actual):
            failures.append(
                f"line {line.number}: {line.text}\n"
                f"    expected: {line.expected or 'success'}\n    actual:   {actual}"
            )
    return failures


def run(args) -> tuple[int, int]:
    """Runs the cases whose names contain `args.k`. Returns how many ran and
    how many failed."""
    schema, cases = parse(FILE)
    cases = [(name, lines) for name, lines in cases if not args.k or args.k in name]
    if cases:
        print("\ntransactions\n")
    failed = 0
    for name, lines in cases:
        failures = run_case(schema, lines)
        print(f"  {'FAIL' if failures else 'ok  '}  {name}")
        for failure in failures:
            print("        " + failure.replace("\n", "\n        "))
        failed += bool(failures)
    return len(cases), failed
