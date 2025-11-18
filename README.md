# JDBC Foreign Data Wrapper for PostgreSQL

This is a foreign data wrapper (FDW) to connect [PostgreSQL](https://www.postgresql.org/)
to any Java DataBase Connectivity (JDBC) data source.

This `jdbc_fdw` is based on [JDBC_FDW](http://github.com/atris/JDBC_FDW.git) and [jdbc2_fdw](https://github.com/heimir-sverrisson/jdbc2_fdw),
with significant enhancements including intelligent database-specific type mapping, comprehensive support for modern databases
(MSSQL, ClickHouse, BigQuery, Snowflake), and advanced query pushdown capabilities.

<img src="https://upload.wikimedia.org/wikipedia/commons/2/29/Postgresql_elephant.svg" align="center" height="100" alt="PostgreSQL"/> + <img src="https://1000logos.net/wp-content/uploads/2020/09/Java-Logo-500x313.png" align="center" height="100" alt="JDBC"/>

## Contents

1. [Features](#features)
2. [Supported platforms](#supported-platforms)
3. [Supported databases](#supported-databases)
4. [Installation](#installation)
5. [Usage](#usage)
6. [Functions](#functions)
7. [Identifier case handling](#identifier-case-handling)
8. [Generated columns](#generated-columns)
9. [Character set handling](#character-set-handling)
10. [Examples](#examples)
11. [Limitations](#limitations)
12. [Contributing](#contributing)
13. [Useful links](#useful-links)
14. [License](#license)

## Features

### Common features

#### Write-able FDW

The existing JDBC FDWs are only read-only, this version provides the write capability.
The user can now issue an insert, update, and delete statement for the foreign tables using the jdbc_fdw.

#### Arbitrary SQL query execution via function

Support execute the whole sql query and get results from the DB behind jdbc connection. This function returns a set of records.

Syntax:

```
jdbc_exec(text connname, text sql);
```

Example:  
To get a set of record, use the below sql query:

```
SELECT jdbc_exec(jdbc_svr, 'SELECT * FROM tbl');
              jdbc_exec
----------------------------------------
 (1,abc,"Fri Dec 31 16:00:00 1999 PST")
 (2,def,"Fri Dec 31 16:00:00 1999 PST")
```

To get a set of record with separate data for each column, use the below sql query:

```
SELECT * FROM jdbc_exec(jdbc_svr, 'SELECT * FROM tbl') as t(id int, c1 text, c2 timestamptz);
 id |  c1 |              c2
----+-----+------------------------------
  1 | abc | Fri Dec 31 16:00:00 1999 PST
  2 | def | Fri Dec 31 16:00:00 1999 PST
```

### Intelligent Type Mapping

`jdbc_fdw` includes database-specific type mappers that intelligently convert native database types to appropriate PostgreSQL types:

- **Automatic type detection**: Determines the remote database type and applies the appropriate mapper
- **Comprehensive coverage**: Supports 100+ type conversions across different databases
- **Warning system**: Emits PostgreSQL WARNING messages during IMPORT FOREIGN SCHEMA for potentially lossy conversions
- **Special type handling**:
  - **ClickHouse**: UInt64 → NUMERIC(20,0), IPv4/IPv6 → INET, Array(T) → PostgreSQL arrays, complex types → JSONB
  - **MSSQL**: GEOGRAPHY/GEOMETRY → TEXT (WKT), HIERARCHYID → TEXT, JSON, UNIQUEIDENTIFIER → UUID
  - **BigQuery**: STRUCT → JSONB, GEOGRAPHY → TEXT (WKT), ARRAY<T> with element type preservation
  - **Snowflake**: VARIANT/OBJECT/ARRAY → JSONB, GEOGRAPHY → TEXT, VECTOR → JSONB

Examples of type conversion warnings:

```
WARNING:  UInt64 mapped to NUMERIC(20,0): PostgreSQL BIGINT cannot represent full range of UInt64
WARNING:  GEOGRAPHY mapped to TEXT (WKT format): Consider using PostGIS for spatial operations
```

### Pushdowning

#### WHERE clause push-down

The jdbc_fdw will push-down the foreign table where clause to the foreign server.
The where condition on the foreign table will be executed on the foreign server, hence there will be fewer rows to bring across to PostgreSQL.
This is a performance feature.

#### Column push-down

The existing JDBC FDWs are fetching all the columns from the target foreign table.
The latest version does the column push-down and only brings back the columns that are part of the select target list.
This is a performance feature.

#### Aggregate function push-down

List of aggregate functions push-down:

```
sum, avg, stddev, stddev_pop, stddev_samp, var_pop, var_samp, variance, max, min, count.
```

#### GROUP BY clause push-down

The jdbc_fdw supports pushing down GROUP BY clauses to the remote server.
When combined with aggregate functions, both the aggregation and grouping
are performed on the remote server, reducing data transfer and improving performance.

Supported:

- Single and multiple column grouping
- Grouping by expressions
- GROUP BY with WHERE clause
- Standard SQL GROUP BY syntax

Not supported:

- GROUPING SETS
- CUBE/ROLLUP
- HAVING clause (planned for future release)

#### Database-specific SQL generation

`jdbc_fdw` automatically detects the remote database type and generates appropriate SQL syntax:

- **MySQL**: Uses `LIMIT n OFFSET m` syntax (detected via backtick quote character)
- **MSSQL/Oracle**: Uses SQL:2008 standard `OFFSET m ROWS FETCH NEXT n ROWS ONLY` syntax
- **MSSQL ORDER BY requirement**: Automatically injects `ORDER BY (SELECT NULL)` when using OFFSET/FETCH without explicit ordering

### Notes about features

#### Maximum digits storing float value of MySQL

Maximum digits storing float value of MySQL is 6 digits. The stored value may not be the same as the value inserted.

#### Variance function

Variance function: For MySQL, variance function is alias for var_pop(). For PostgreSQL/GridDB, variance function is alias for var_samp(). Due to the different meaning of variance function between MySQL and PostgreSQL/GridDB, the result will be different.

#### Concatenation Operator

The || operator as a concatenation operator is standard SQL, however in MySQL, it represents the OR operator (logical operator). If the PIPES_AS_CONCAT SQL mode is enabled, || signifies the SQL-standard string concatenation operator (like CONCAT()). User needs to enable PIPES_AS_CONCAT mode in MySQL for concatenation.

#### Timestamp range

The MySQL timestamp range is not the same as the PostgreSQL timestamp range, so be careful if using this type. In MySQL, TIMESTAMP has a range of '1970-01-01 00:00:01' UTC to '2038-01-19 03:14:07' UTC.

#### JDBC connection handling

`jdbc_fdw` features robust connection management:

- **C-side connection caching**: Connections are cached per (server, user) mapping for efficient reuse
- **Dynamic driver loading**: JDBC drivers are loaded dynamically via `JDBCDriverLoader`
- **Query timeout support**: Configurable via server options to prevent hung queries
- **UTC timezone handling**: All timestamps are processed in UTC to avoid JVM timezone issues
- **Prepared statements**: Full support for parameter binding (INSERT/UPDATE operations)

**Security note**: `jdbc_fdw` runs on the PostgreSQL server and uses its IP address to make connections, which can make many trusted requests to the server's network. For example, when trusted local authentication is enabled in the PostgreSQL server, jdbc_fdw can connect to the loopback server (127.0.0.1) with any username (including the root account) without requiring a password. Be careful when granting permissions of `FOREIGN SERVER` and `USER MAPPING` to non-superusers.

Reference

#### Write-able FDW

The user can issue an update and delete statement for the foreign table, which has set the primary key option.

## Supported platforms

`jdbc_fdw` was developed on Linux, and should run on any
reasonably POSIX-compliant system.

`jdbc_fdw` is designed to be compatible with PostgreSQL 12 ~ 17.
Java 21 or later is required.

## Supported Databases

`jdbc_fdw` includes intelligent, database-specific type mapping and optimizations for:

- **MySQL** - Full support with LIMIT syntax and comprehensive type mapping
- **PostgreSQL** - Full support via JDBC (for cross-version or cross-server scenarios)
- **Microsoft SQL Server (MSSQL)** - Catalog-aware schema import, 40+ type mappings including GEOGRAPHY, HIERARCHYID, JSON
- **ClickHouse** - 30+ native type mappings including UInt variants, IPv4/IPv6, Arrays, complex types
- **BigQuery** - Array and complex type handling, GEOGRAPHY, semi-structured data support
- **Snowflake** - Semi-structured data (VARIANT, OBJECT, ARRAY), GEOGRAPHY, VECTOR types
- **Oracle** - SQL:2008 OFFSET/FETCH syntax support
- **GridDB** - Original implementation with generic type mapping
- **Any JDBC-compliant database** - Generic type mapping for databases not listed above

Each database-specific mapper provides intelligent type conversion with warnings for potentially lossy mappings (e.g., unsigned integer overflow, complex type conversion to JSONB).

## Installation

### Source installation

Prerequisites:

- **JDBC driver** (.jar file) of the target database is required. Download the appropriate driver:

  - [MySQL](https://dev.mysql.com/downloads/connector/j/) - MySQL Connector/J
  - [PostgreSQL](https://jdbc.postgresql.org/) - PostgreSQL JDBC Driver
  - [MSSQL](https://learn.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server) - Microsoft JDBC Driver for SQL Server
  - [ClickHouse](https://github.com/ClickHouse/clickhouse-java) - ClickHouse JDBC Driver
  - [BigQuery](https://cloud.google.com/bigquery/docs/reference/odbc-jdbc-drivers) - Google BigQuery JDBC Driver (Simba)
  - [Snowflake](https://docs.snowflake.com/en/developer-guide/jdbc/jdbc) - Snowflake JDBC Driver
  - [Oracle](https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html) - Oracle JDBC Driver (ojdbc)
  - [GridDB](https://github.com/griddb/jdbc) - GridDB JDBC Driver

- **Java Development Kit (JDK)** 21 or later
- **PostgreSQL** 12 or later with development headers (`postgresql-server-dev` or equivalent)

#### 1. To build jdbc_fdw, you need to symbolic link the jvm library to your path.

```
sudo ln -s /usr/lib/jvm/[java version]/jre/lib/amd64/server/libjvm.so /usr/lib64/libjvm.so
```

#### 2. Build and install jdbc_fdw

Add a directory of `pg_config` to PATH and build and install `jdbc_fdw`.

```sh
make USE_PGXS=1
make install USE_PGXS=1
```

If you want to build `jdbc_fdw` in a source tree of PostgreSQL, use

```sh
make
make install
```

You may have to change to root/installation privileges before executing 'make install'

## Usage

## CREATE SERVER options

`jdbc_fdw` accepts the following options via the `CREATE SERVER` command:

- **drivername** as _string_
  The name of the JDBC driver.
  Note that the driver name has to be specified for jdbc_fdw to work.
  It can be found in JDBC driver documentation.

  - [PostgreSQL](https://jdbc.postgresql.org/documentation/head/load.html) `drivername 'org.postgresql.Driver'`
  - [GridDB](http://www.toshiba-sol.co.jp/en/pro/griddb/docs-en/v4_1/GridDB_AE_JDBC_DriverGuide.html) `drivername 'com.toshiba.mwcloud.gs.sql.Driver'`

- **url** as _string_

  The JDBC URL that shall be used to connect to the foreign database.
  Note that URL has to be specified for jdbc_fdw to work.
  It can be found in JDBC driver documentation.

  - [PostgreSQL](https://jdbc.postgresql.org/documentation/head/load.html) `url 'jdbc:postgresql://[host]:[port]/[database]'`
  - [GridDB](http://www.toshiba-sol.co.jp/en/pro/griddb/docs-en/v4_1/GridDB_AE_JDBC_DriverGuide.html) `url 'jdbc:gs://[host]:[port]/[clusterName]/[databaseName]'`

- **querytimeout** as _integer_

  The number of seconds that an SQL statement may execute before timing out.
  The option can be used for terminating hung queries.

- **jarfile** as _string_

  The path and name(e.g. folder1/folder2/abc.jar) of the JAR file of the JDBC driver to be used of the foreign database.
  Note that the path must be absolute path.

```
/[path]/[jarfilename].jar
```

- **maxheapsize** as _integer_

  The value of the option shall be set to the maximum heap size of the JVM which is being used in jdbc fdw. It can be set from 1 Mb onwards. This option is used for setting the maximum heap size of the JVM manually.

## CREATE USER MAPPING options

`jdbc_fdw` accepts the following options via the `CREATE USER MAPPING`
command:

- **username**

  The JDBC username to connect as.

- **password**

  The JDBC user's password.

## CREATE FOREIGN TABLE options

`jdbc_fdw` accepts the no table-level options via the
`CREATE FOREIGN TABLE` command.

The following column-level options are available:

- **key** as _boolean_

  The primary key options can be set while creating a JDBC foreign table object with OPTIONS(key 'true')

```
CREATE FOREIGN TABLE [table name]([column name] [column type] OPTIONS(key 'true')) SERVER [server name];
```

Note that while PostgreSQL allows a foreign table to be defined without
any columns, `jdbc_fdw` _can_ raise an error as soon as any operations
are carried out on it.

## IMPORT FOREIGN SCHEMA options

`jdbc_fdw` supports [IMPORT FOREIGN SCHEMA](https://www.postgresql.org/docs/current/sql-importforeignschema.html)
(when running with PostgreSQL 9.5 or later) and accepts the following custom options:

- **recreate** as _boolean_

  If 'true', table schema will be updated.
  After schema is imported, we can access tables.

- **remote_schema** as _string_

  The schema name on the remote database to import from. If not specified, uses the schema name provided in the IMPORT FOREIGN SCHEMA command.

### Database-Specific Behavior

- **MSSQL**: The schema parameter is interpreted as the _catalog_ (database name) for table discovery, enabling cross-database imports. Example: `IMPORT FOREIGN SCHEMA "mydatabase" ...` imports tables from the "mydatabase" database.
- **Snowflake**: Table name matching uses uppercase for case-insensitive matching.
- **ClickHouse**: Discovers both tables and views with explicit type filtering.
- **All databases**: Type conversion warnings are emitted during import to alert users of potentially lossy type mappings.

## TRUNCATE support

`jdbc_fdw` yet don't implements the foreign data wrapper `TRUNCATE` API, available
from PostgreSQL 14.

## Functions

Functions from this FDW in PostgreSQL catalog are **yet not described**.

## Identifier case handling

PostgreSQL folds identifiers to lower case by default, JDBC data source can have different behaviour.
Rules and problems **yet not tested and described**.

## Generated columns

Behaviour within generated columns **yet not tested and described**.

`jdbc_fdw` potentially can provide support for PostgreSQL's generated
columns (PostgreSQL 12+).

Note that while `jdbc_fdw` will insert or update the generated column value
in JDBC, there is nothing to stop the value being modified within JDBC,
and hence no guarantee that in subsequent `SELECT` operations the column will
still contain the expected generated value. This limitation also applies to
`postgres_fdw`.

For more details on generated columns see:

- [Generated Columns](https://www.postgresql.org/docs/current/ddl-generated-columns.html)
- [CREATE FOREIGN TABLE](https://www.postgresql.org/docs/current/sql-createforeigntable.html)

## Character set handling

**Yet not described**. JDBC is UTF-8 by default.

## Examples

Install the extension:

```sql
CREATE EXTENSION jdbc_fdw;
```

### Example 1: MySQL Connection

```sql
-- Create server
CREATE SERVER mysql_server FOREIGN DATA WRAPPER jdbc_fdw
  OPTIONS (
    drivername 'com.mysql.cj.jdbc.Driver',
    url 'jdbc:mysql://localhost:3306/mydb',
    querytimeout '30',
    jarfile '/usr/share/java/mysql-connector-java.jar'
  );

-- Create user mapping
CREATE USER MAPPING FOR CURRENT_USER SERVER mysql_server
  OPTIONS (username 'mysqluser', password 'mysqlpass');

-- Import schema
IMPORT FOREIGN SCHEMA myschema
  FROM SERVER mysql_server
  INTO public
  OPTIONS (recreate 'true');
```

### Example 2: Microsoft SQL Server (MSSQL)

```sql
-- Create server
CREATE SERVER mssql_server FOREIGN DATA WRAPPER jdbc_fdw
  OPTIONS (
    drivername 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
    url 'jdbc:sqlserver://localhost:1433',
    jarfile '/usr/share/java/mssql-jdbc.jar'
  );

-- Create user mapping
CREATE USER MAPPING FOR CURRENT_USER SERVER mssql_server
  OPTIONS (username 'sa', password 'YourPassword123');

-- Import from specific database (catalog)
IMPORT FOREIGN SCHEMA "AdventureWorks"
  FROM SERVER mssql_server
  INTO public
  OPTIONS (recreate 'true');
```

### Example 3: ClickHouse

```sql
-- Create server
CREATE SERVER clickhouse_server FOREIGN DATA WRAPPER jdbc_fdw
  OPTIONS (
    drivername 'com.clickhouse.jdbc.ClickHouseDriver',
    url 'jdbc:clickhouse://localhost:8123/default',
    jarfile '/usr/share/java/clickhouse-jdbc.jar'
  );

-- Create user mapping
CREATE USER MAPPING FOR CURRENT_USER SERVER clickhouse_server
  OPTIONS (username 'default', password '');

-- Import schema with type conversion warnings
IMPORT FOREIGN SCHEMA default
  FROM SERVER clickhouse_server
  INTO public
  OPTIONS (recreate 'true');
-- Example warning output:
-- WARNING:  UInt64 mapped to NUMERIC(20,0): PostgreSQL BIGINT cannot represent full range of UInt64
-- WARNING:  Array(Int32) mapped to INTEGER[]
```

### Example 4: Google BigQuery

```sql
-- Create server
CREATE SERVER bigquery_server FOREIGN DATA WRAPPER jdbc_fdw
  OPTIONS (
    drivername 'com.simba.googlebigquery.jdbc.Driver',
    url 'jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId=myproject;OAuthType=0;',
    jarfile '/usr/share/java/GoogleBigQueryJDBC42.jar'
  );

-- Create user mapping
CREATE USER MAPPING FOR CURRENT_USER SERVER bigquery_server
  OPTIONS (username '', password '');

-- Import schema
IMPORT FOREIGN SCHEMA mydataset
  FROM SERVER bigquery_server
  INTO public
  OPTIONS (recreate 'true');
```

### Example 5: Snowflake

```sql
-- Create server
CREATE SERVER snowflake_server FOREIGN DATA WRAPPER jdbc_fdw
  OPTIONS (
    drivername 'net.snowflake.client.jdbc.SnowflakeDriver',
    url 'jdbc:snowflake://myaccount.snowflakecomputing.com/?db=mydb&warehouse=mywh',
    jarfile '/usr/share/java/snowflake-jdbc.jar'
  );

-- Create user mapping
CREATE USER MAPPING FOR CURRENT_USER SERVER snowflake_server
  OPTIONS (username 'myuser', password 'mypass');

-- Import schema (note: Snowflake uses uppercase matching)
IMPORT FOREIGN SCHEMA "PUBLIC"
  FROM SERVER snowflake_server
  INTO public
  OPTIONS (recreate 'true');
```

### Example 6: GridDB

```sql
-- Create server
CREATE SERVER griddb_server FOREIGN DATA WRAPPER jdbc_fdw
  OPTIONS (
    drivername 'com.toshiba.mwcloud.gs.sql.Driver',
    url 'jdbc:gs://239.0.0.1:41999/myCluster/public',
    jarfile '/usr/share/java/gridstore-jdbc.jar'
  );

-- Create user mapping
CREATE USER MAPPING FOR CURRENT_USER SERVER griddb_server
  OPTIONS (username 'admin', password 'admin');

-- Import schema
IMPORT FOREIGN SCHEMA public
  FROM SERVER griddb_server
  INTO public
  OPTIONS (recreate 'true');
```

### Manual Table Creation

If you prefer to create tables manually instead of using IMPORT FOREIGN SCHEMA:

```sql
-- Create foreign table with primary key
CREATE FOREIGN TABLE customer (
  id INTEGER OPTIONS (key 'true'),
  name TEXT,
  email TEXT,
  created_at TIMESTAMP
) SERVER mysql_server OPTIONS (table_name 'customers');

-- Query the foreign table
SELECT * FROM customer WHERE created_at > '2024-01-01';

-- Execute arbitrary SQL on remote server
SELECT * FROM jdbc_exec('mysql_server', 'SELECT COUNT(*) FROM orders')
  AS t(order_count BIGINT);
```

## Limitations

#### Unsupported clause

The following clauses are not supported in jdbc_fdw:
RETURNING, ORDER BY clauses, casting type, transaction control, GROUPING SETS, HAVING

#### Array Type

Array types are supported for databases with native array support (e.g., ClickHouse Array(T) types, BigQuery ARRAY<T>). The array element type is intelligently mapped to the appropriate PostgreSQL array type (e.g., `Array(Int32)` → `INTEGER[]`). Generic JDBC array type support may vary depending on the specific JDBC driver implementation.

#### Floating-point value comparison

Floating-point numbers are approximate and not stored as exact values. A floating-point value as written in an SQL statement may not be the same as the value represented internally.

For example:

```
SELECT float4.f1 FROM FLOAT4_TBL tbl06 WHERE float4.f1 <> '1004.3';
     f1
-------------
           0
      1004.3
      -34.84
 1.23457e+20
 1.23457e-20
(5 rows)
```

In order to get correct result, can decide on an acceptable tolerance for differences between the numbers and then do the comparison against the tolerance value to that can get the correct result.

```
SELECT float4.f1 FROM tbl06 float4 WHERE float4.f1 <> '1004.3' GROUP BY float4.id, float4.f1 HAVING abs(f1 - 1004.3) > 0.001 ORDER BY float4.id;
     f1
-------------
           0
      -34.84
 1.23457e+20
 1.23457e-20
(4 rows)
```

#### IMPORT FOREIGN SCHEMA

IMPORT FOREIGN SCHEMA is supported for all JDBC-compliant databases. Database-specific type mappers are available for MySQL, PostgreSQL, MSSQL, ClickHouse, BigQuery, Snowflake, Oracle, and GridDB. Other databases use a generic JDBC type mapper.

## Contributing

Opening issues and pull requests on GitHub are welcome.

## Useful links

### Source code

- [JDBC_FDW](http://github.com/atris/JDBC_FDW.git)
- [jdbc2_fdw](https://github.com/heimir-sverrisson/jdbc2_fdw)

Reference FDW realisation, `postgres_fdw`

- https://git.postgresql.org/gitweb/?p=postgresql.git;a=tree;f=contrib/postgres_fdw;hb=HEAD

### General FDW Documentation

- https://www.postgresql.org/docs/current/ddl-foreign-data.html
- https://www.postgresql.org/docs/current/sql-createforeigndatawrapper.html
- https://www.postgresql.org/docs/current/sql-createforeigntable.html
- https://www.postgresql.org/docs/current/sql-importforeignschema.html
- https://www.postgresql.org/docs/current/fdwhandler.html
- https://www.postgresql.org/docs/current/postgres-fdw.html

### Other FDWs

- https://wiki.postgresql.org/wiki/Fdw
- https://pgxn.org/tag/fdw/

## License

Copyright (c) 2021, TOSHIBA CORPORATION
Copyright (c) 2012 - 2016, Atri Sharma atri.jiit@gmail.com

Permission to use, copy, modify, and distribute this software and its documentation for any purpose, without fee, and without a written agreement is hereby granted, provided that the above copyright notice and this paragraph and the following two paragraphs appear in all copies.

See the [`LICENSE`][1] file for full details.

[1]: LICENSE.md
