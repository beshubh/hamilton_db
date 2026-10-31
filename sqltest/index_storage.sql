-- Index storage: lookups and sorts, with and without an index.
--
-- Each case is two queries that must return the same rows: the first on
-- users_plain (no indexes), the second on users (primary key on id, indexes
-- on age and email). `-- index:` names the index the second query must use.
-- run.py checks both, then times them.

-- case: primary key lookup
-- index: users_pkey
SELECT * FROM users_plain WHERE id = 4242;
SELECT * FROM users WHERE id = 4242;

-- case: primary key lookup, no match
-- index: users_pkey
SELECT * FROM users_plain WHERE id = 20000;
SELECT * FROM users WHERE id = 20000;

-- case: primary key range
-- index: users_pkey
SELECT count(*) FROM users_plain WHERE id >= 5000 AND id < 5100;
SELECT count(*) FROM users WHERE id >= 5000 AND id < 5100;

-- case: int secondary lookup
-- index: users_age
SELECT id FROM users_plain WHERE age = 42 ORDER BY id;
SELECT id FROM users WHERE age = 42 ORDER BY id;

-- case: int secondary range
-- index: users_age
SELECT count(*) FROM users_plain WHERE age >= 30 AND age < 33;
SELECT count(*) FROM users WHERE age >= 30 AND age < 33;

-- case: varchar secondary lookup
-- index: users_email
SELECT * FROM users_plain WHERE email = 'userb33e76f2@example.com';
SELECT * FROM users WHERE email = 'userb33e76f2@example.com';

-- case: varchar secondary lookup, no match
-- index: users_email
SELECT * FROM users_plain WHERE email = 'nobody@example.com';
SELECT * FROM users WHERE email = 'nobody@example.com';

-- case: varchar secondary range
-- index: users_email
SELECT count(*) FROM users_plain WHERE email >= 'user1' AND email < 'user2';
SELECT count(*) FROM users WHERE email >= 'user1' AND email < 'user2';

-- case: sort by primary key
-- index: users_pkey
SELECT id, score FROM users_plain ORDER BY id;
SELECT id, score FROM users ORDER BY id;

-- case: sort by primary key, top 10
-- index: users_pkey
SELECT id, score FROM users_plain ORDER BY id LIMIT 10;
SELECT id, score FROM users ORDER BY id LIMIT 10;

-- case: sort by int secondary
-- index: users_age
SELECT age FROM users_plain ORDER BY age;
SELECT age FROM users ORDER BY age;

-- case: sort by varchar secondary
-- index: users_email
SELECT id, email FROM users_plain ORDER BY email;
SELECT id, email FROM users ORDER BY email;

-- case: sort by varchar secondary, top 10
-- index: users_email
SELECT id, email FROM users_plain ORDER BY email LIMIT 10;
SELECT id, email FROM users ORDER BY email LIMIT 10;
