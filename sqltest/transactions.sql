-- Concurrent transactions: the anomalies MVTO must prevent.
--
-- `\session x` switches to session x. A transaction's age is set by when
-- its BEGIN runs. Under MVTO the result must equal running the transactions
-- in that order, so where they clash, one fails with a conflict and rolls
-- back. `-- =>` is the statement's expected result; check.py runs each case
-- on a fresh database with the tables below.

CREATE TABLE accounts (id INT PRIMARY KEY, balance INT NOT NULL);
INSERT INTO accounts VALUES (1, 100), (2, 100);
CREATE TABLE doctors (name VARCHAR(16) PRIMARY KEY, on_call BOOL NOT NULL);
INSERT INTO doctors VALUES ('alice', true), ('bob', true);


-- case: non-repeatable read: a newer update, committed or not, isn't seen
\session a
BEGIN;
SELECT balance FROM accounts WHERE id = 1;                -- => 100
\session b
BEGIN;
UPDATE accounts SET balance = 50 WHERE id = 1;            -- => UPDATE 1
\session a
SELECT balance FROM accounts WHERE id = 1;                -- => 100
\session b
COMMIT;
\session a
SELECT balance FROM accounts WHERE id = 1;                -- => 100
SELECT sum(balance) FROM accounts;                        -- => 200
COMMIT;
SELECT balance FROM accounts WHERE id = 1;                -- => 50


-- case: non-repeatable read: an older transaction can't update a row a newer one read
\session b
BEGIN;
\session a
BEGIN;
SELECT balance FROM accounts WHERE id = 1;                -- => 100
\session b
UPDATE accounts SET balance = 50 WHERE id = 1;            -- => error: conflicts
\session a
SELECT balance FROM accounts WHERE id = 1;                -- => 100
COMMIT;
\session b
COMMIT;                                                   -- => ROLLBACK
SELECT balance FROM accounts WHERE id = 1;                -- => 100


-- case: lost update: both read 100 and write back a deposit, the older write fails
-- a deposits 10 and b deposits 20; committing both would lose a's.
\session a
BEGIN;
\session b
BEGIN;
\session a
SELECT balance FROM accounts WHERE id = 1;                -- => 100
\session b
SELECT balance FROM accounts WHERE id = 1;                -- => 100
\session a
UPDATE accounts SET balance = 110 WHERE id = 1;           -- => error: conflicts
\session b
UPDATE accounts SET balance = 120 WHERE id = 1;           -- => UPDATE 1
COMMIT;
\session a
COMMIT;                                                   -- => ROLLBACK
SELECT balance FROM accounts WHERE id = 1;                -- => 120


-- case: lost update: concurrent increments, the second fails and its retry counts
\session a
BEGIN;
\session b
BEGIN;
\session a
UPDATE accounts SET balance = balance + 10 WHERE id = 1;  -- => UPDATE 1
\session b
UPDATE accounts SET balance = balance + 20 WHERE id = 1;  -- => error: conflicts
ROLLBACK;
\session a
COMMIT;
\session b
UPDATE accounts SET balance = balance + 20 WHERE id = 1;  -- => UPDATE 1
SELECT balance FROM accounts WHERE id = 1;                -- => 130


-- case: write skew: each doctor goes off call after seeing two on call
-- Committing both would leave nobody on call.
\session a
BEGIN;
\session b
BEGIN;
\session a
SELECT count(*) FROM doctors WHERE on_call = true;        -- => 2
\session b
SELECT count(*) FROM doctors WHERE on_call = true;        -- => 2
\session a
UPDATE doctors SET on_call = false WHERE name = 'alice';  -- => error: conflicts
\session b
UPDATE doctors SET on_call = false WHERE name = 'bob';    -- => UPDATE 1
COMMIT;
\session a
COMMIT;                                                   -- => ROLLBACK
SELECT name FROM doctors WHERE on_call = true;            -- => alice


-- case: no false conflict: writes to different rows both commit
\session a
BEGIN;
\session b
BEGIN;
\session a
UPDATE accounts SET balance = balance - 30 WHERE id = 1;  -- => UPDATE 1
\session b
UPDATE accounts SET balance = balance + 30 WHERE id = 2;  -- => UPDATE 1
\session a
COMMIT;
\session b
COMMIT;
SELECT id, balance FROM accounts;                         -- => 1, 70; 2, 130
