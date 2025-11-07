--
-- GROUP BY with aggregate pushdown tests
--
--Testcase 1:
CREATE EXTENSION IF NOT EXISTS jdbc_fdw;

--Testcase 2:
CREATE SERVER IF NOT EXISTS jdbc_test_server FOREIGN DATA WRAPPER jdbc_fdw OPTIONS(
    drivername 'org.postgresql.Driver',
    url 'jdbc:postgresql://localhost:5432/postgres',
    querytimeout '10',
    jarfile '/path/to/postgresql.jar',
    maxheapsize '600'
);

--Testcase 3:
CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER jdbc_test_server
    OPTIONS(username 'postgres', password 'postgres');

--Testcase 4:
CREATE FOREIGN TABLE group_test (
    id int4,
    category text,
    value int4,
    amount numeric
) SERVER jdbc_test_server OPTIONS (table_name 'group_test');

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

-- Complex real-world scenarios

--Testcase 15:
CREATE FOREIGN TABLE sales_data (
    sale_id int4,
    product_category text,
    region text,
    sale_date date,
    quantity int4,
    revenue numeric
) SERVER jdbc_test_server OPTIONS (table_name 'sales_data');

-- Multi-level aggregation with multiple dimensions
--Testcase 16:
EXPLAIN VERBOSE
SELECT product_category, region,
       sum(revenue) as total_revenue,
       avg(quantity) as avg_quantity,
       count(*) as num_sales
FROM sales_data
WHERE sale_date >= '2024-01-01'
GROUP BY product_category, region;

--Testcase 17:
SELECT product_category, region,
       sum(revenue) as total_revenue,
       avg(quantity) as avg_quantity,
       count(*) as num_sales
FROM sales_data
WHERE sale_date >= '2024-01-01'
GROUP BY product_category, region
ORDER BY product_category, region;

-- Grouping with computed columns
--Testcase 18:
EXPLAIN VERBOSE
SELECT region,
       CASE
           WHEN quantity > 100 THEN 'high'
           WHEN quantity > 50 THEN 'medium'
           ELSE 'low'
       END as volume_category,
       sum(revenue) as total_revenue
FROM sales_data
GROUP BY region,
         CASE
             WHEN quantity > 100 THEN 'high'
             WHEN quantity > 50 THEN 'medium'
             ELSE 'low'
         END;

--Testcase 19:
DROP FOREIGN TABLE sales_data;

--Testcase 20:
DROP FOREIGN TABLE group_test;
--Testcase 21:
DROP USER MAPPING FOR CURRENT_USER SERVER jdbc_test_server;
--Testcase 22:
DROP SERVER jdbc_test_server;
--Testcase 23:
DROP EXTENSION jdbc_fdw CASCADE;
