# Unique-constraint tests for Postgres 15+.
# Every expectation below was verified against Postgres 16.
# Format: see test_unique.py. Each test runs in a fresh database.

############################################################################
# A. NULL semantics
############################################################################

=== a01_multiple_nulls_allowed
setup: CREATE TABLE t (v int UNIQUE);
s1: INSERT INTO t VALUES (NULL), (NULL), (NULL);
s1: INSERT INTO t VALUES (1);
s1: INSERT INTO t VALUES (1);                      => err:23505
s1: SELECT count(*) FROM t WHERE v IS NULL;        => rows: 3

=== a02_nulls_not_distinct_allows_one_null
setup: CREATE TABLE t (v int UNIQUE NULLS NOT DISTINCT);
s1: INSERT INTO t VALUES (NULL);
s1: INSERT INTO t VALUES (NULL);                   => err:23505

=== a03_composite_key_any_null_column_escapes_check
# (1,NULL) never equals (1,NULL): the row escapes uniqueness entirely.
setup: CREATE TABLE t (a int, b int, UNIQUE (a, b));
s1: INSERT INTO t VALUES (1, NULL), (1, NULL), (NULL, NULL);
s1: INSERT INTO t VALUES (1, 2), (1, 2);           => err:23505
s1: SELECT count(*) FROM t;                        => rows: 3

=== a04_composite_key_nulls_not_distinct
setup: CREATE TABLE t (a int, b int, UNIQUE NULLS NOT DISTINCT (a, b));
s1: INSERT INTO t VALUES (1, NULL);
s1: INSERT INTO t VALUES (2, NULL);
s1: INSERT INTO t VALUES (1, NULL);                => err:23505

=== a05_update_value_to_null_and_back
setup: CREATE TABLE t (id int, v int UNIQUE);
setup: INSERT INTO t VALUES (1, 10), (2, 20);
s1: UPDATE t SET v = NULL;
s1: UPDATE t SET v = 10 WHERE id = 1;
s1: UPDATE t SET v = 10 WHERE id = 2;              => err:23505

############################################################################
# B. What counts as "equal" (equality comes from the type's btree opclass,
#    not from the bytes on disk)
############################################################################

=== b01_text_is_case_sensitive
setup: CREATE TABLE t (v text UNIQUE);
s1: INSERT INTO t VALUES ('abc'), ('ABC'), ('abc ');
s1: INSERT INTO t VALUES ('abc');                  => err:23505

=== b02_char_n_ignores_trailing_spaces
setup: CREATE TABLE t (v char(5) UNIQUE);
s1: INSERT INTO t VALUES ('ab');
s1: INSERT INTO t VALUES ('ab   ');                => err:23505

=== b03_numeric_scale_does_not_matter
setup: CREATE TABLE t (v numeric UNIQUE);
s1: INSERT INTO t VALUES (1.0);
s1: INSERT INTO t VALUES (1.000);                  => err:23505

=== b04_float_negative_zero_equals_zero
setup: CREATE TABLE t (v float8 UNIQUE);
s1: INSERT INTO t VALUES ('0');
s1: INSERT INTO t VALUES ('-0');                   => err:23505

=== b05_float_nan_equals_nan
# IEEE says NaN != NaN, but Postgres btree treats NaN as equal to itself.
setup: CREATE TABLE t (v float8 UNIQUE);
s1: INSERT INTO t VALUES ('NaN');
s1: INSERT INTO t VALUES ('NaN');                  => err:23505

=== b06_expression_index_lower
setup: CREATE TABLE t (email text);
setup: CREATE UNIQUE INDEX t_lower ON t (lower(email));
s1: INSERT INTO t VALUES ('Bob@x.com');
s1: INSERT INTO t VALUES ('bob@X.COM');            => err:23505

=== b07_jsonb_key_order_is_irrelevant
setup: CREATE TABLE t (v jsonb UNIQUE);
s1: INSERT INTO t VALUES ('{"a":1,"b":2}');
s1: INSERT INTO t VALUES ('{"b":2, "a":1}');       => err:23505

############################################################################
# C. Single statement / single transaction
############################################################################

=== c01_duplicate_within_one_insert
setup: CREATE TABLE t (v int UNIQUE);
s1: INSERT INTO t VALUES (1), (2), (1);            => err:23505
s1: SELECT count(*) FROM t;                        => rows: 0

=== c02_non_deferrable_checks_per_row_not_per_statement
# SQL standard says check at end of statement. Postgres checks per row for
# NON-deferrable constraints, so a shift that is valid overall can fail
# depending on physical row order.
setup: CREATE TABLE t (v int UNIQUE);
setup: INSERT INTO t SELECT generate_series(1, 5);
s1: UPDATE t SET v = v + 1;                        => err:23505
s1: SELECT string_agg(v::text, ',' ORDER BY v) FROM t;  => rows: 1,2,3,4,5

=== c03_deferrable_initially_immediate_checks_at_statement_end
setup: CREATE TABLE t (v int UNIQUE DEFERRABLE INITIALLY IMMEDIATE);
setup: INSERT INTO t SELECT generate_series(1, 5);
s1: UPDATE t SET v = v + 1;
s1: SELECT string_agg(v::text, ',' ORDER BY v) FROM t;  => rows: 2,3,4,5,6

=== c04_swap_two_values
setup: CREATE TABLE t (id int, v int UNIQUE);
setup: INSERT INTO t VALUES (1, 10), (2, 20);
s1: UPDATE t SET v = CASE v WHEN 10 THEN 20 ELSE 10 END;   => err:23505
s1: ALTER TABLE t DROP CONSTRAINT t_v_key;
s1: ALTER TABLE t ADD CONSTRAINT t_v_key UNIQUE (v) DEFERRABLE;
s1: UPDATE t SET v = CASE v WHEN 10 THEN 20 ELSE 10 END;
s1: SELECT id, v FROM t ORDER BY id;               => rows: 1|20; 2|10

=== c05_delete_then_reinsert_same_txn
setup: CREATE TABLE t (v int UNIQUE);
setup: INSERT INTO t VALUES (1);
s1: BEGIN;
s1: DELETE FROM t WHERE v = 1;
s1: INSERT INTO t VALUES (1);
s1: DELETE FROM t WHERE v = 1;
s1: INSERT INTO t VALUES (1);
s1: COMMIT;
s1: SELECT count(*) FROM t;                        => rows: 1

=== c06_savepoint_rollback_frees_the_key
setup: CREATE TABLE t (v int UNIQUE);
s1: BEGIN;
s1: SAVEPOINT sp;
s1: INSERT INTO t VALUES (1);
s1: ROLLBACK TO sp;
s1: INSERT INTO t VALUES (1);
s1: COMMIT;
s1: SELECT count(*) FROM t;                        => rows: 1

=== c07_violation_aborts_whole_transaction
setup: CREATE TABLE t (v int UNIQUE);
s1: BEGIN;
s1: INSERT INTO t VALUES (1);
s1: INSERT INTO t VALUES (1);                      => err:23505
s1: INSERT INTO t VALUES (2);                      => err:25P02
s1: COMMIT;
s1: SELECT count(*) FROM t;                        => rows: 0

=== c08_violation_inside_savepoint_keeps_outer_work
setup: CREATE TABLE t (v int UNIQUE);
s1: BEGIN;
s1: INSERT INTO t VALUES (1);
s1: SAVEPOINT sp;
s1: INSERT INTO t VALUES (1);                      => err:23505
s1: ROLLBACK TO sp;
s1: INSERT INTO t VALUES (2);
s1: COMMIT;
s1: SELECT string_agg(v::text, ',' ORDER BY v) FROM t;  => rows: 1,2

=== c09_update_non_key_column_does_not_self_conflict
setup: CREATE TABLE t (v int UNIQUE, n int);
setup: INSERT INTO t VALUES (1, 0);
s1: UPDATE t SET n = n + 1 WHERE v = 1;
s1: UPDATE t SET v = v WHERE v = 1;
s1: SELECT v, n FROM t;                            => rows: 1|1

############################################################################
# D. ON CONFLICT
############################################################################

=== d01_do_nothing_with_dupes_in_same_statement
setup: CREATE TABLE t (v int UNIQUE);
s1: INSERT INTO t VALUES (1), (1), (2) ON CONFLICT DO NOTHING;
s1: SELECT string_agg(v::text, ',' ORDER BY v) FROM t;  => rows: 1,2

=== d02_do_update_cannot_hit_same_row_twice
setup: CREATE TABLE t (v int UNIQUE, n int);
s1: INSERT INTO t VALUES (1, 1), (1, 2) ON CONFLICT (v) DO UPDATE SET n = excluded.n;  => err:21000
s1: SELECT count(*) FROM t;                        => rows: 0

=== d03_deferrable_constraint_cannot_be_arbiter
setup: CREATE TABLE t (v int UNIQUE DEFERRABLE);
s1: INSERT INTO t VALUES (1) ON CONFLICT (v) DO NOTHING;   => err:55000

=== d04_conflict_target_must_match_an_index
setup: CREATE TABLE t (a int, b int UNIQUE);
s1: INSERT INTO t VALUES (1, 1) ON CONFLICT (a) DO NOTHING;  => err:42P10

=== d05_do_nothing_still_raises_on_other_unique_index
# Arbiter is only v; the conflict on w is not handled.
setup: CREATE TABLE t (v int UNIQUE, w int UNIQUE);
setup: INSERT INTO t VALUES (1, 1);
s1: INSERT INTO t VALUES (2, 1) ON CONFLICT (v) DO NOTHING;  => err:23505
s1: INSERT INTO t VALUES (2, 1) ON CONFLICT DO NOTHING;
s1: SELECT count(*) FROM t;                        => rows: 1

=== d06_partial_index_needs_matching_where_in_conflict_target
setup: CREATE TABLE t (v int, deleted bool DEFAULT false);
setup: CREATE UNIQUE INDEX t_live ON t (v) WHERE NOT deleted;
s1: INSERT INTO t VALUES (1) ON CONFLICT (v) DO NOTHING;   => err:42P10
s1: INSERT INTO t VALUES (1) ON CONFLICT (v) WHERE NOT deleted DO NOTHING;
s1: INSERT INTO t VALUES (1) ON CONFLICT (v) WHERE NOT deleted DO NOTHING;
s1: SELECT count(*) FROM t;                        => rows: 1

############################################################################
# E. Deferred constraints
############################################################################

=== e01_deferred_dupes_fixed_before_commit
setup: CREATE TABLE t (id int, v int UNIQUE DEFERRABLE INITIALLY DEFERRED);
s1: BEGIN;
s1: INSERT INTO t VALUES (1, 1), (2, 1);
s1: UPDATE t SET v = 2 WHERE id = 2;
s1: COMMIT;
s1: SELECT count(*) FROM t;                        => rows: 2

=== e02_deferred_violation_fails_at_commit_and_rolls_back
setup: CREATE TABLE t (v int UNIQUE DEFERRABLE INITIALLY DEFERRED);
s1: BEGIN;
s1: INSERT INTO t VALUES (5);
s1: INSERT INTO t VALUES (1);
s1: INSERT INTO t VALUES (1);
s1: COMMIT;                                        => err:23505
s1: SELECT count(*) FROM t;                        => rows: 0

=== e03_set_constraints_immediate_checks_pending_violations
setup: CREATE TABLE t (v int CONSTRAINT t_v_key UNIQUE DEFERRABLE INITIALLY DEFERRED);
s1: BEGIN;
s1: INSERT INTO t VALUES (1), (1);
s1: SET CONSTRAINTS t_v_key IMMEDIATE;             => err:23505
s1: ROLLBACK;

=== e04_deferred_violation_then_delete_both
setup: CREATE TABLE t (v int UNIQUE DEFERRABLE INITIALLY DEFERRED);
s1: BEGIN;
s1: INSERT INTO t VALUES (1), (1), (1);
s1: DELETE FROM t;
s1: COMMIT;
s1: SELECT count(*) FROM t;                        => rows: 0

############################################################################
# F. ALTER TABLE / CREATE INDEX on existing data
############################################################################

=== f01_add_unique_on_existing_dupes_fails_and_leaves_nothing
setup: CREATE TABLE t (v int);
setup: INSERT INTO t VALUES (1), (2), (1);
s1: ALTER TABLE t ADD CONSTRAINT t_v_key UNIQUE (v);   => err:23505
s1: SELECT count(*) FROM pg_constraint WHERE conname = 't_v_key';  => rows: 0
s1: INSERT INTO t VALUES (2);

=== f02_add_unique_with_many_nulls_ok_but_not_nulls_not_distinct
setup: CREATE TABLE t (v int);
setup: INSERT INTO t VALUES (NULL), (NULL), (1);
s1: ALTER TABLE t ADD CONSTRAINT k1 UNIQUE NULLS NOT DISTINCT (v);  => err:23505
s1: ALTER TABLE t ADD CONSTRAINT k2 UNIQUE (v);

=== f03_add_column_unique_on_non_empty_table
# No default: every row gets NULL -> fine. Constant default: every row collides.
setup: CREATE TABLE t (id int);
setup: INSERT INTO t VALUES (1), (2);
s1: ALTER TABLE t ADD COLUMN a int UNIQUE;
s1: ALTER TABLE t ADD COLUMN b int DEFAULT 0 UNIQUE;  => err:23505
s1: SELECT count(*) FROM information_schema.columns WHERE table_name = 't' AND column_name = 'b';  => rows: 0

=== f04_add_column_unique_with_volatile_default
setup: CREATE TABLE t (id int);
setup: INSERT INTO t SELECT generate_series(1, 1000);
setup: CREATE SEQUENCE s;
s1: ALTER TABLE t ADD COLUMN c int DEFAULT nextval('s') UNIQUE;
s1: SELECT count(DISTINCT c) FROM t;               => rows: 1000

=== f05_alter_type_collapses_distinct_values
# 1.001 and 1.004 are distinct as numeric, both round to 1.00.
setup: CREATE TABLE t (v numeric UNIQUE);
setup: INSERT INTO t VALUES (1.001), (1.004);
s1: ALTER TABLE t ALTER COLUMN v TYPE numeric(5,2);    => err:23505
s1: SELECT string_agg(v::text, ',' ORDER BY v) FROM t;  => rows: 1.001,1.004

=== f06_alter_type_text_to_int_collapses
setup: CREATE TABLE t (v text UNIQUE);
setup: INSERT INTO t VALUES ('1'), ('01'), (' 1');
s1: ALTER TABLE t ALTER COLUMN v TYPE int USING v::int;  => err:23505

=== f07_ddl_is_transactional
setup: CREATE TABLE t (v int);
s1: BEGIN;
s1: ALTER TABLE t ADD CONSTRAINT t_v_key UNIQUE (v);
s1: INSERT INTO t VALUES (1);
s1: INSERT INTO t VALUES (1);                      => err:23505
s1: ROLLBACK;
s1: INSERT INTO t VALUES (1), (1);

=== f08_drop_constraint_then_readd_fails
setup: CREATE TABLE t (v int CONSTRAINT t_v_key UNIQUE);
setup: INSERT INTO t VALUES (1);
s1: ALTER TABLE t DROP CONSTRAINT t_v_key;
s1: INSERT INTO t VALUES (1);
s1: ALTER TABLE t ADD CONSTRAINT t_v_key UNIQUE (v);   => err:23505

=== f09_add_constraint_using_existing_index
setup: CREATE TABLE t (v int);
setup: CREATE UNIQUE INDEX t_v_idx ON t (v);
s1: ALTER TABLE t ADD CONSTRAINT t_v_key UNIQUE USING INDEX t_v_idx;
s1: SELECT conname FROM pg_constraint WHERE conrelid = 't'::regclass;  => rows: t_v_key
s1: INSERT INTO t VALUES (1), (1);                 => err:23505

=== f10_failed_create_index_concurrently_leaves_invalid_index_that_still_enforces
# CIC fails during the initial build: an INVALID, not-ready index is left
# behind. It is not maintained, so it enforces nothing until dropped/rebuilt.
setup: CREATE TABLE t (v int);
setup: INSERT INTO t VALUES (1), (1);
s1: CREATE UNIQUE INDEX CONCURRENTLY t_v_idx ON t (v);     => err:23505
s1: SELECT indisvalid, indisready FROM pg_index WHERE indexrelid = 't_v_idx'::regclass;  => rows: false|false
s1: INSERT INTO t VALUES (2);
s1: INSERT INTO t VALUES (2);
s1: DROP INDEX t_v_idx;
s1: INSERT INTO t VALUES (2);

=== f11_reindex_detects_duplicates_after_unique_index_was_dropped_and_recreated
setup: CREATE TABLE t (v int);
setup: INSERT INTO t VALUES (1), (1);
s1: CREATE UNIQUE INDEX t_v_idx ON t (v);          => err:23505
s1: DELETE FROM t WHERE ctid = (SELECT min(ctid) FROM t);
s1: CREATE UNIQUE INDEX t_v_idx ON t (v);
s1: REINDEX INDEX t_v_idx;

############################################################################
# G. Partial unique index (soft-delete pattern)
############################################################################

=== g01_soft_delete_allows_reuse
setup: CREATE TABLE t (id serial, email text, deleted_at timestamptz);
setup: CREATE UNIQUE INDEX t_live ON t (email) WHERE deleted_at IS NULL;
s1: INSERT INTO t (email) VALUES ('a');
s1: INSERT INTO t (email) VALUES ('a');            => err:23505
s1: UPDATE t SET deleted_at = now() WHERE email = 'a';
s1: INSERT INTO t (email) VALUES ('a');
s1: UPDATE t SET deleted_at = NULL WHERE id = 1;   => err:23505

############################################################################
# H. Concurrency: two sessions on the same key
############################################################################

=== h01_concurrent_insert_first_commits
setup: CREATE TABLE t (v int UNIQUE);
s1: BEGIN;
s1: INSERT INTO t VALUES (1);
s2: INSERT INTO t VALUES (1);                      => blocks
s1: COMMIT;
s2: <await>                                        => err:23505

=== h02_concurrent_insert_first_rolls_back
setup: CREATE TABLE t (v int UNIQUE);
s1: BEGIN;
s1: INSERT INTO t VALUES (1);
s2: INSERT INTO t VALUES (1);                      => blocks
s1: ROLLBACK;
s2: <await>
s1: SELECT count(*) FROM t;                        => rows: 1

=== h04_concurrent_nulls_never_block
setup: CREATE TABLE t (v int UNIQUE);
s1: BEGIN;
s1: INSERT INTO t VALUES (NULL);
s2: INSERT INTO t VALUES (NULL);
s1: COMMIT;

=== h05_concurrent_nulls_block_with_nulls_not_distinct
setup: CREATE TABLE t (v int UNIQUE NULLS NOT DISTINCT);
s1: BEGIN;
s1: INSERT INTO t VALUES (NULL);
s2: INSERT INTO t VALUES (NULL);                   => blocks
s1: COMMIT;
s2: <await>                                        => err:23505

=== h06_different_keys_never_block
setup: CREATE TABLE t (v int UNIQUE);
s1: BEGIN;
s1: INSERT INTO t VALUES (1);
s2: INSERT INTO t VALUES (2);
s1: COMMIT;

=== h07_insert_waits_on_uncommitted_delete_commit
setup: CREATE TABLE t (v int UNIQUE);
setup: INSERT INTO t VALUES (1);
s1: BEGIN;
s1: DELETE FROM t WHERE v = 1;
s2: INSERT INTO t VALUES (1);                      => blocks
s1: COMMIT;
s2: <await>

=== h08_insert_waits_on_uncommitted_delete_rollback
setup: CREATE TABLE t (v int UNIQUE);
setup: INSERT INTO t VALUES (1);
s1: BEGIN;
s1: DELETE FROM t WHERE v = 1;
s2: INSERT INTO t VALUES (1);                      => blocks
s1: ROLLBACK;
s2: <await>                                        => err:23505

=== h09_key_moved_away_by_uncommitted_update
setup: CREATE TABLE t (v int UNIQUE);
setup: INSERT INTO t VALUES (1);
s1: BEGIN;
s1: UPDATE t SET v = 2 WHERE v = 1;
s2: BEGIN;
s2: INSERT INTO t VALUES (1);                      => blocks
s1: COMMIT;
s2: <await>
s2: INSERT INTO t VALUES (2);                      => err:23505
s2: ROLLBACK;

=== h10_key_moved_into_by_uncommitted_update
setup: CREATE TABLE t (v int UNIQUE);
setup: INSERT INTO t VALUES (1);
s1: BEGIN;
s1: UPDATE t SET v = 2 WHERE v = 1;
s2: INSERT INTO t VALUES (2);                      => blocks
s1: ROLLBACK;
s2: <await>

=== h11_non_key_update_still_owns_the_key
# s1's UPDATE writes a new version of the row with the same key. A naive
# engine sees the old version "deleted by in-progress txn" and lets s2 in.
setup: CREATE TABLE t (v int UNIQUE, n int);
setup: INSERT INTO t VALUES (1, 0);
s1: BEGIN;
s1: UPDATE t SET n = 1 WHERE v = 1;
s2: INSERT INTO t VALUES (1, 9);                   => blocks
s1: COMMIT;
s2: <await>                                        => err:23505

=== h12_delete_and_reinsert_in_one_txn_while_other_waits
setup: CREATE TABLE t (v int UNIQUE, src text);
setup: INSERT INTO t VALUES (1, 'orig');
s1: BEGIN;
s1: DELETE FROM t WHERE v = 1;
s1: INSERT INTO t VALUES (1, 's1');
s2: INSERT INTO t VALUES (1, 's2');                => blocks
s1: COMMIT;
s2: <await>                                        => err:23505
s2: SELECT src FROM t;                             => rows: s1

=== h13_aborted_subtransaction_releases_key_immediately
# s1 is still running, but the subxact that wrote the key aborted.
# s2 must not wait on s1's top-level xid.
setup: CREATE TABLE t (v int UNIQUE);
s1: BEGIN;
s1: SAVEPOINT sp;
s1: INSERT INTO t VALUES (1);
s1: ROLLBACK TO sp;
s2: INSERT INTO t VALUES (1);
s1: INSERT INTO t VALUES (1);                      => err:23505
s1: ROLLBACK;

=== h14_committed_subtransaction_holds_key_until_top_commit
setup: CREATE TABLE t (v int UNIQUE);
s1: BEGIN;
s1: SAVEPOINT sp;
s1: INSERT INTO t VALUES (1);
s1: RELEASE sp;
s2: INSERT INTO t VALUES (1);                      => blocks
s1: ROLLBACK;
s2: <await>

=== h15_unique_deadlock
setup: CREATE TABLE t (v int UNIQUE);
s1: SET deadlock_timeout = '10s';
s1: BEGIN;
s1: INSERT INTO t VALUES (1);
s2: BEGIN;
s2: INSERT INTO t VALUES (2);
s1: INSERT INTO t VALUES (2);                      => blocks
s2: INSERT INTO t VALUES (1);                      => err:40P01
s1: <await>
s1: COMMIT;
s1: SELECT string_agg(v::text, ',' ORDER BY v) FROM t;  => rows: 1,2

############################################################################
# I. Concurrency + isolation levels (invisible rows still conflict)
############################################################################

=== i01_repeatable_read_cannot_see_row_but_conflicts_with_it
setup: CREATE TABLE t (v int UNIQUE);
s1: BEGIN ISOLATION LEVEL REPEATABLE READ;
s1: SELECT count(*) FROM t;                        => rows: 0
s2: INSERT INTO t VALUES (1);
s1: SELECT count(*) FROM t WHERE v = 1;            => rows: 0
s1: INSERT INTO t VALUES (1);                      => err:23505
s1: ROLLBACK;

=== i02_repeatable_read_do_nothing_on_invisible_row
# The conflicting row is invisible to s1's snapshot. Postgres cannot silently
# skip a row it cannot see under RR, so it raises a serialization failure.
setup: CREATE TABLE t (v int UNIQUE);
s1: BEGIN ISOLATION LEVEL REPEATABLE READ;
s1: SELECT 1;                                      => rows: 1
s2: INSERT INTO t VALUES (1);
s1: INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING;   => err:40001
s1: ROLLBACK;

=== i03_repeatable_read_do_update_on_invisible_row_is_serialization_failure
setup: CREATE TABLE t (v int UNIQUE, n int);
s1: BEGIN ISOLATION LEVEL REPEATABLE READ;
s1: SELECT 1;                                      => rows: 1
s2: INSERT INTO t VALUES (1, 0);
s1: INSERT INTO t VALUES (1, 0) ON CONFLICT (v) DO UPDATE SET n = t.n + 1;  => err:40001
s1: ROLLBACK;

=== i04_serializable_read_then_insert_is_serialization_failure
# s1 read "v=1 does not exist", then hit a duplicate. Under SERIALIZABLE
# Postgres reports 40001 (retryable) instead of 23505.
setup: CREATE TABLE t (v int UNIQUE);
# Needs an index scan (predicate lock on the key) and s2 also SERIALIZABLE.
s1: SET enable_seqscan = off;
s1: BEGIN ISOLATION LEVEL SERIALIZABLE;
s1: SELECT count(*) FROM t WHERE v = 1;            => rows: 0
s2: BEGIN ISOLATION LEVEL SERIALIZABLE;
s2: INSERT INTO t VALUES (1);
s2: COMMIT;
s1: INSERT INTO t VALUES (1);                      => err:40001
s1: ROLLBACK;

=== i05_serializable_without_prior_read_is_plain_unique_violation
setup: CREATE TABLE t (v int UNIQUE);
s1: BEGIN ISOLATION LEVEL SERIALIZABLE;
s1: SELECT 1;                                      => rows: 1
s2: INSERT INTO t VALUES (1);
s1: INSERT INTO t VALUES (1);                      => err:23505
s1: ROLLBACK;

=== i06_repeatable_read_waiter_after_commit
# s2 is blocked waiting on s1; after s1 commits, s2 (RR) still gets 23505.
setup: CREATE TABLE t (v int UNIQUE);
s1: BEGIN;
s1: INSERT INTO t VALUES (1);
s2: BEGIN ISOLATION LEVEL REPEATABLE READ;
s2: SELECT 1;                                      => rows: 1
s2: INSERT INTO t VALUES (1);                      => blocks
s1: COMMIT;
s2: <await>                                        => err:23505
s2: ROLLBACK;

############################################################################
# J. Concurrency + ON CONFLICT
############################################################################

=== j01_do_nothing_waits_then_skips
setup: CREATE TABLE t (v int UNIQUE);
s1: BEGIN;
s1: INSERT INTO t VALUES (1);
s2: INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING RETURNING v;   => blocks
s1: COMMIT;
s2: <await>                                        => ok

=== j02_do_nothing_waits_then_inserts_after_rollback
setup: CREATE TABLE t (v int UNIQUE);
s1: BEGIN;
s1: INSERT INTO t VALUES (1);
s2: INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING RETURNING v;   => blocks
s1: ROLLBACK;
s2: <await>                                        => rows: 1

=== j03_concurrent_upserts_do_not_lose_updates
setup: CREATE TABLE t (v int UNIQUE, n int);
s1: BEGIN;
s1: INSERT INTO t VALUES (1, 1) ON CONFLICT (v) DO UPDATE SET n = t.n + 1;
s2: BEGIN;
s2: INSERT INTO t VALUES (1, 1) ON CONFLICT (v) DO UPDATE SET n = t.n + 1;  => blocks
s3: INSERT INTO t VALUES (1, 1) ON CONFLICT (v) DO UPDATE SET n = t.n + 1;  => blocks
s1: COMMIT;
s2: <await>
s2: COMMIT;
s3: <await>
s1: SELECT n FROM t;                               => rows: 3

=== j04_upsert_against_concurrently_deleted_row
# s1 deletes the conflicting row; s2's upsert waits, then takes the INSERT path.
setup: CREATE TABLE t (v int UNIQUE, n int);
setup: INSERT INTO t VALUES (1, 100);
s1: BEGIN;
s1: DELETE FROM t WHERE v = 1;
s2: INSERT INTO t VALUES (1, 1) ON CONFLICT (v) DO UPDATE SET n = t.n + 1 RETURNING n;  => blocks
s1: COMMIT;
s2: <await>                                        => rows: 1

############################################################################
# K. Concurrency + deferred constraints
############################################################################

=== k01_deferred_does_not_block_and_loser_fails_at_commit
setup: CREATE TABLE t (v int UNIQUE DEFERRABLE INITIALLY DEFERRED);
s1: BEGIN;
s1: INSERT INTO t VALUES (1);
s2: BEGIN;
s2: INSERT INTO t VALUES (1);
s1: COMMIT;
s2: COMMIT;                                        => err:23505
s1: SELECT count(*) FROM t;                        => rows: 1

=== k02_deferred_commit_waits_for_inflight_peer
setup: CREATE TABLE t (v int UNIQUE DEFERRABLE INITIALLY DEFERRED);
s1: BEGIN;
s1: INSERT INTO t VALUES (1);
s2: BEGIN;
s2: INSERT INTO t VALUES (1);
s2: COMMIT;                                        => blocks
s1: ROLLBACK;
s2: <await>
s1: SELECT count(*) FROM t;                        => rows: 1

############################################################################
# L. Concurrency + DDL
############################################################################

=== l01_add_unique_waits_for_uncommitted_dupe_commit
setup: CREATE TABLE t (v int);
setup: INSERT INTO t VALUES (1);
s1: BEGIN;
s1: INSERT INTO t VALUES (1);
s2: ALTER TABLE t ADD CONSTRAINT t_v_key UNIQUE (v);   => blocks
s1: COMMIT;
s2: <await>                                        => err:23505

=== l02_add_unique_waits_for_uncommitted_dupe_rollback
setup: CREATE TABLE t (v int);
setup: INSERT INTO t VALUES (1);
s1: BEGIN;
s1: INSERT INTO t VALUES (1);
s2: ALTER TABLE t ADD CONSTRAINT t_v_key UNIQUE (v);   => blocks
s1: ROLLBACK;
s2: <await>
s1: INSERT INTO t VALUES (1);                      => err:23505

=== l03_insert_blocked_behind_uncommitted_add_unique
setup: CREATE TABLE t (v int);
setup: INSERT INTO t VALUES (1);
s1: BEGIN;
s1: ALTER TABLE t ADD CONSTRAINT t_v_key UNIQUE (v);
s2: INSERT INTO t VALUES (1);                      => blocks
s1: COMMIT;
s2: <await>                                        => err:23505

=== l04_insert_blocked_behind_add_unique_that_rolls_back
setup: CREATE TABLE t (v int);
setup: INSERT INTO t VALUES (1);
s1: BEGIN;
s1: ALTER TABLE t ADD CONSTRAINT t_v_key UNIQUE (v);
s2: INSERT INTO t VALUES (1);                      => blocks
s1: ROLLBACK;
s2: <await>

=== l05_drop_constraint_concurrent_with_insert
setup: CREATE TABLE t (v int CONSTRAINT t_v_key UNIQUE);
setup: INSERT INTO t VALUES (1);
s1: BEGIN;
s1: ALTER TABLE t DROP CONSTRAINT t_v_key;
s2: INSERT INTO t VALUES (1);                      => blocks
s1: COMMIT;
s2: <await>
s2: SELECT count(*) FROM t;                        => rows: 2

=== l06_cic_waits_for_inflight_writer_and_catches_its_dupe
setup: CREATE TABLE t (v int);
setup: INSERT INTO t VALUES (1);
s1: BEGIN;
s1: INSERT INTO t VALUES (1);
s2: CREATE UNIQUE INDEX CONCURRENTLY t_v_idx ON t (v); => blocks
s1: COMMIT;
s2: <await>                                        => err:23505
s2: SELECT indisvalid FROM pg_index WHERE indexrelid = 't_v_idx'::regclass;  => rows: false
