-- Two copies of the same table: one with no indexes, one with a primary key
-- and two secondary indexes. Indexes must exist before the rows go in:
-- CREATE INDEX doesn't index rows already in the table.

CREATE TABLE users_plain (
    id INT NOT NULL,
    age INT NOT NULL,
    email VARCHAR(64) NOT NULL,
    score INT NOT NULL
);

CREATE TABLE users (
    id INT PRIMARY KEY,
    age INT NOT NULL,
    email VARCHAR(64) NOT NULL,
    score INT NOT NULL
);
CREATE INDEX users_age ON users (age);
CREATE INDEX users_email ON users (email);
