"""The index storage suite: runs index_storage.sql, checks each case, then
times it.

Each case starts with `-- case: <name>`, may name the index its second query
must use with `-- index: <name>`, and holds two queries: one on the plain
table, one on the indexed table. A case passes if
  - both queries return the same output,
  - the plain query's plan uses no index, and the indexed query's plan uses
    the named index.
Each query then runs --warmup times untimed and --runs times timed, and the
median is reported.
"""
import re
import statistics
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

import cli
import gen_data

HERE = Path(__file__).resolve().parent
FILE = HERE / "index_storage.sql"


@dataclass
class Case:
    name: str
    index: str | None
    plain: str
    indexed: str


def parse(path: Path) -> list[Case]:
    cases = []
    for block in re.split(r"^-- case: ", path.read_text(), flags=re.M)[1:]:
        name, _, body = block.partition("\n")
        index = re.search(r"^-- index: (\S+)", body, flags=re.M)
        sql = "\n".join(l for l in body.splitlines() if not l.startswith("--"))
        queries = [q.strip() + ";" for q in sql.split(";") if q.strip()]
        if len(queries) != 2:
            sys.exit(f"{path}: case '{name}' needs 2 queries, has {len(queries)}")
        cases.append(Case(name.strip(), index and index[1], *queries))
    return cases


def load(db: Path) -> list[str]:
    """Loads the schema and rows into `db`. Returns the errors."""
    blocks = cli.run(db, (HERE / "schema.sql").read_text() + gen_data.sql())
    return [b for b in blocks if b.startswith("error:")]


def run_case(db: Path, case: Case, warmup: int, runs: int):
    """Returns (plain ms, indexed ms, row count, problems)."""
    n = warmup + runs
    script = "\n".join(
        ["\\timing", f"EXPLAIN {case.plain}", f"EXPLAIN {case.indexed}"]
        + [case.plain] * n
        + [case.indexed] * n
    )
    results = [cli.split_time(b) for b in cli.run(db, script)[1:]]  # Skip "Timing is on."
    (plain_plan, _), (indexed_plan, _) = results[:2]
    plain, indexed = results[2 : 2 + n], results[2 + n :]

    problems = [f"error: {b}" for b, _ in results if b.startswith("error:")][:1]
    if problems:
        return 0.0, 0.0, None, problems
    if "IndexScan" in plain_plan:
        problems.append(f"plain query uses an index:\n{plain_plan}")
    if case.index and f"IndexScan using {case.index}" not in indexed_plan:
        problems.append(f"indexed query doesn't use {case.index}:\n{indexed_plan}")
    if plain[0][0] != indexed[0][0]:
        problems.append(f"results differ:\n--- plain\n{plain[0][0]}\n--- indexed\n{indexed[0][0]}")
    rows = re.search(r"\((\d+) rows?\)", plain[0][0])
    median = lambda results: statistics.median(ms for _, ms in results[warmup:])
    return median(plain), median(indexed), rows and int(rows[1]), problems


def run(args) -> tuple[int, int]:
    """Runs the cases whose names contain `args.k`. Returns how many ran and
    how many failed."""
    cases = [c for c in parse(FILE) if not args.k or args.k in c.name]
    if not cases:
        return 0, 0
    print("\nindex_storage\n")
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "sqltest.db"
        if errors := load(db):
            print(f"  FAIL  loading the data: {errors[0]}")
            return len(cases), 1
        print(f"  {'':4}  {'case (median of %d runs, ms)' % args.runs:40} "
              f"{'rows':>6} {'no index':>10} {'index':>10} {'speedup':>8}")
        failed = 0
        for case in cases:
            plain, indexed, rows, problems = run_case(db, case, args.warmup, args.runs)
            status = "FAIL" if problems else "ok  "
            speedup = f"{plain / indexed:7.1f}x" if indexed else ""
            print(
                f"  {status}  {case.name:40} {rows if rows is not None else '':>6} "
                f"{plain:10.3f} {indexed:10.3f} {speedup:>8}"
            )
            for problem in problems:
                print("        " + problem.replace("\n", "\n        "))
            failed += bool(problems)
    return len(cases), failed
