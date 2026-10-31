#!/usr/bin/env python3
"""Writes the 10k-row data set as SQL INSERTs, to stdout.

The same rows go into `users_plain` (no indexes) and `users` (primary key
plus secondary indexes), so every query can be compared on both. Rows are
inserted out of id order, and emails don't sort like ids, so neither the heap
order nor the id order gives a free sort.
"""
import sys

ROWS = 10_000
BATCH = 1_000


def rows():
    # 7919 is coprime with ROWS, so this visits every id once, shuffled.
    for i in range(ROWS):
        id_ = (i * 7919) % ROWS + 1
        age = 18 + (id_ * 37) % 63
        # An odd multiplier mod 2^32 is a bijection, so emails are unique.
        email = f"user{(id_ * 2654435761) % 2**32:08x}@example.com"
        score = (id_ * 7) % 1000
        yield f"({id_}, {age}, '{email}', {score})"


def main():
    data = list(rows())
    for table in ("users_plain", "users"):
        for start in range(0, ROWS, BATCH):
            batch = ",\n".join(data[start : start + BATCH])
            sys.stdout.write(f"INSERT INTO {table} VALUES\n{batch};\n")


if __name__ == "__main__":
    main()
