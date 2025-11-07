--
-- MySql
-- GROUP BY with aggregate pushdown tests
--
\set ECHO none
\ir sql/configs/mysql_parameters.conf
\set ECHO all

--Testcase 1:
CREATE EXTENSION IF NOT EXISTS :DB_EXTENSIONNAME;

--Testcase 2:
CREATE SERVER IF NOT EXISTS :DB_SERVERNAME FOREIGN DATA WRAPPER :DB_EXTENSIONNAME OPTIONS(
    drivername :DB_DRIVERNAME,
    url :DB_URL,
    querytimeout '10',
    jarfile :DB_DRIVERPATH,
    maxheapsize '600'
);

--Testcase 3:
CREATE USER MAPPING IF NOT EXISTS FOR public SERVER :DB_SERVERNAME OPTIONS(username :DB_USER, password :DB_PASS);

--Testcase 4:
CREATE FOREIGN TABLE group_test (
    id int4,
    category text,
    value int4,
    amount numeric
) SERVER :DB_SERVERNAME OPTIONS (table_name 'group_test');

-- Basic GROUP BY with single aggregate
--Testcase 5:
EXPLAIN VERBOSE
SELECT category, sum(value) FROM group_test GROUP BY category;

--Testcase 6:
SELECT category, sum(value) FROM group_test GROUP BY category ORDER BY category;

-- Multiple aggregates with GROUP BY
--Testcase 7:
EXPLAIN VERBOSE
SELECT category, sum(value), avg(value), count(*)
FROM group_test
GROUP BY category;

--Testcase 8:
SELECT category, sum(value), avg(value), count(*)
FROM group_test
GROUP BY category
ORDER BY category;

-- GROUP BY with WHERE clause
--Testcase 9:
EXPLAIN VERBOSE
SELECT category, sum(value)
FROM group_test
WHERE value > 10
GROUP BY category;

--Testcase 10:
SELECT category, sum(value)
FROM group_test
WHERE value > 10
GROUP BY category
ORDER BY category;

-- Multiple GROUP BY columns
--Testcase 11:
EXPLAIN VERBOSE
SELECT category, id, sum(value)
FROM group_test
GROUP BY category, id;

--Testcase 12:
SELECT category, id, sum(value)
FROM group_test
GROUP BY category, id
ORDER BY category, id;

-- GROUP BY with expressions
--Testcase 13:
EXPLAIN VERBOSE
SELECT id + 1, sum(value)
FROM group_test
GROUP BY id + 1;

--Testcase 14:
SELECT id + 1, sum(value)
FROM group_test
GROUP BY id + 1
ORDER BY 1;

--Testcase 15:
DROP FOREIGN TABLE group_test;
--Testcase 16:
DROP USER MAPPING FOR public SERVER :DB_SERVERNAME;
--Testcase 17:
DROP SERVER :DB_SERVERNAME;
--Testcase 18:
DROP EXTENSION :DB_EXTENSIONNAME CASCADE;
