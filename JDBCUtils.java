/*-------------------------------------------------------------------------
 *
 *                foreign-data wrapper for JDBC
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author: Atri Sharma <atri.jiit@gmail.com>
 * Changes by: Heimir Sverrisson <heimir.sverrisson@gmail.com>, 2015-04-17
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *                jdbc_fdw/JDBCUtils.java
 *
 *-------------------------------------------------------------------------
 */

import java.io.*;
import java.sql.*;
import java.time.LocalTime;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;

public class JDBCUtils {
  private JDBCConnection conn = null;
  private Statement tmpStmt;
  private PreparedStatement tmpPstmt;
  private static int resultSetKey = 1;
  private static ConcurrentHashMap<Integer, resultSetInfo> resultSetInfoMap =
      new ConcurrentHashMap<Integer, resultSetInfo>();
  private Map<String, List<String>> typeConversionWarnings = new HashMap<String, List<String>>();

  /* Constructor */
  public JDBCUtils() {
    // Constructor initialization
  }

  /*
   * createConnection
   *      Initiates the connection to the foreign database after setting
   *      up initial configuration.
   *      key - the serverid for the connection cache identifying
   *      Caller will pass in a six element array with the following elements:
   *          0 - Driver class name, 1 - JDBC URL, 2 - Username
   *          3 - Password, 4 - Query timeout in seconds, 5 - jarfile
   *
   */
  public void createConnection(int key, long server_hashvalue, long mapping_hashvalue, String[] options) throws Exception {
    try {
      this.conn = JDBCConnection.getConnection(key, server_hashvalue, mapping_hashvalue, options);
    } catch (Throwable e) {
      // Connection failed - error will be propagated to PostgreSQL
      throw e;
    }
  }

  /*
   * createStatement
   *      Create a statement object based on the query
   */
  public void createStatement(String query) throws SQLException {
    /*
     *  Set the query select all columns for creating the same size of the result table
     *  because jvm can only return String[] - resultRow.
     *  Todo: return only necessary column.
     */
    try {
      checkConnExist();
      tmpStmt = conn.getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      if (conn.getQueryTimeout() != 0) {
        tmpStmt.setQueryTimeout(conn.getQueryTimeout());
      }
      tmpStmt.executeQuery(query);
    } catch (Throwable e) {
      // Query execution failed - error will be propagated to PostgreSQL
      throw e;
    }
  }

  /*
   * createStatementID
   *      Create a statement object based on the query
   *      with a specific resultID and return back to the calling C function
   *      Returns:
   *          resultID on success
   */
  public int createStatementID(String query) throws Exception {
    ResultSet tmpResultSet;
    int tmpNumberOfColumns;
    int tmpNumberOfAffectedRows = 0;
    ResultSetMetaData rSetMetadata;
    int tmpResultSetKey;
    try {
      checkConnExist();
      tmpStmt = conn.getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      if (conn.getQueryTimeout() != 0) {
        tmpStmt.setQueryTimeout(conn.getQueryTimeout());
      }
      tmpResultSet = tmpStmt.executeQuery(query);
      rSetMetadata = tmpResultSet.getMetaData();
      tmpNumberOfColumns = rSetMetadata.getColumnCount();
      tmpResultSetKey = initResultSetKey();
      resultSetInfoMap.put(
          tmpResultSetKey,
          new resultSetInfo(
              tmpResultSet, tmpNumberOfColumns, tmpNumberOfAffectedRows, null));
      return tmpResultSetKey;
    } catch (Throwable e) {
      // Query execution failed - error will be propagated to PostgreSQL
      throw e;
    }
  }

  /*
   * clearResultSetID
   *      clear ResultSetID
   */
  public void clearResultSetID(int resultSetID) throws SQLException {
    try {
      checkConnExist();
      resultSetInfoMap.remove(resultSetID);
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * createPreparedStatement
   *      Create a PreparedStatement object based on the query
   *      with a specific resultID and return back to the calling C function
   *      Returns:
   *          resultID on success
   */
  public int createPreparedStatement(String query) throws Exception {
    try {
      checkConnExist();
      PreparedStatement tmpPstmt = (PreparedStatement) conn.getConnection().prepareStatement(query);
      if (conn.getQueryTimeout() != 0) {
        tmpPstmt.setQueryTimeout(conn.getQueryTimeout());
      }
      int tmpResultSetKey = initResultSetKey();
      resultSetInfoMap.put(tmpResultSetKey, new resultSetInfo(null, null, 0, tmpPstmt));
      return tmpResultSetKey;
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * execPreparedStatement
   *      Create a PreparedStatement object based on the query
   */
  public void execPreparedStatement(int resultSetID) throws SQLException {
    try {
      checkConnExist();
      PreparedStatement tmpPstmt = resultSetInfoMap.get(resultSetID).getPstmt();
      checkPstmt(tmpPstmt);
      int tmpNumberOfAffectedRows = tmpPstmt.executeUpdate();
      tmpPstmt.clearParameters();

      resultSetInfoMap.get(resultSetID).setPstmt(tmpPstmt);
      resultSetInfoMap.get(resultSetID).setNumberOfAffectedRows(tmpNumberOfAffectedRows);
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * getNumberOfColumns
   *      Returns arrayOfNumberOfColumns[resultSetID]
   *      Returns:
   *          NumberOfColumns on success
   */
  public int getNumberOfColumns(int resultSetID) throws SQLException {
    try {
      return resultSetInfoMap.get(resultSetID).getNumberOfColumns();
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * getNumberOfAffectedRows
   *      Returns numberOfAffectedRows
   *      Returns:
   *          NumberOfAffectedRows on success
   */
  public int getNumberOfAffectedRows(int resultSetID) throws SQLException {
    try {
      return resultSetInfoMap.get(resultSetID).getNumberOfAffectedRows();
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * getResultSet
   *      Returns the result set that is returned from the foreign database
   *      after execution of the query to C code. One row is returned at a time
   *      as an Object array. For binary related types (BINARY, LONGVARBINARY, VARBINARY,
   *      BLOB), Object corresponds to byte array. For other types, Object corresponds to
   *      String. After last row null is returned.
   */
  public Object[] getResultSet(int resultSetID) throws SQLException {
    int i = 0;
    try {
      ResultSet tmpResultSet = resultSetInfoMap.get(resultSetID).getResultSet();
      int tmpNumberOfColumns = resultSetInfoMap.get(resultSetID).getNumberOfColumns();
      Object[] tmpArrayOfResultRow = new Object[tmpNumberOfColumns];
      ResultSetMetaData mtData = tmpResultSet.getMetaData();

      /* Row-by-row processing is done in jdbc_fdw.One row
       * at a time is returned to the C code. */
      if (tmpResultSet.next()) {
        for (i = 0; i < tmpNumberOfColumns; i++) {
          int columnType = mtData.getColumnType(i + 1);

          switch (columnType) {
            case Types.BINARY:
            case Types.LONGVARBINARY:
            case Types.VARBINARY:
            case Types.BLOB:
              /* Get byte array */
              tmpArrayOfResultRow[i] = tmpResultSet.getBytes(i + 1);
              break;
            case Types.TIMESTAMP:
              /*
               * Get the timestamp in UTC time zone by default
               * to avoid being affected by the remote server's time zone.
               */
              java.util.Calendar cal = Calendar.getInstance();
              cal.setTimeZone(TimeZone.getTimeZone("UTC"));
              Timestamp resTimestamp = tmpResultSet.getTimestamp(i + 1, cal);
              if (resTimestamp != null) {
                /* Timestamp is returned as text in ISO 8601 style */
                tmpArrayOfResultRow[i] = resTimestamp.toInstant().toString();
              } else {
                tmpArrayOfResultRow[i] = null;
              }
              break;
            default:
              /* Convert all columns to String */
              tmpArrayOfResultRow[i] = tmpResultSet.getString(i + 1);
          }
        }
        /* The current row in resultSet is returned
         * to the C code in a Java String array that
         * has the value of the fields of the current
         * row as it values. */
        return tmpArrayOfResultRow;
      } else {
        /*
         * All of resultSet's rows have been returned to the C code.
         * Close tmpResultSet's statement
         */
        tmpResultSet.getStatement().close();
        clearResultSetID(resultSetID);
        return null;
      }
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * getColumnTypesByResultSetID
   *      Returns the column types
   */
  public String[] getColumnTypesByResultSetID(int resultSetID) throws SQLException {
    int i = 0;
    try {
      ResultSet tmpResultSet = resultSetInfoMap.get(resultSetID).getResultSet();
      ResultSetMetaData resultSetMetaData = tmpResultSet.getMetaData();
      int columnNumber = resultSetMetaData.getColumnCount();
      String[] tmpColumnTypesList = new String[columnNumber];

      for (i = 0; i < columnNumber; i++)
      {
        /* Column's index start from 1, so param of getColumnTypeName is (i + 1)  */
        int columnType = resultSetMetaData.getColumnType(i + 1);
        switch (columnType) {
          case Types.ARRAY:
            /* Array get from postgres server e.g interger[], bool[],... */
            tmpColumnTypesList[i] = tmpResultSet.getString("TYPE_NAME");
            break;
          case Types.BIGINT:
            tmpColumnTypesList[i] = "BIGINT";
            break;
          case Types.BINARY:
          case Types.BLOB:
          case Types.LONGVARBINARY:
          case Types.VARBINARY:
            tmpColumnTypesList[i] = "BYTEA";
            break;
          case Types.BIT:
          case Types.BOOLEAN:
            tmpColumnTypesList[i] = "BOOL";
            break;
          case Types.CHAR:
          case Types.LONGVARCHAR:
          case Types.VARCHAR:
            tmpColumnTypesList[i] = "TEXT";
            break;
          case Types.DATE:
            tmpColumnTypesList[i] = "DATE";
            break;
          case Types.DECIMAL:
          case Types.NUMERIC:
            tmpColumnTypesList[i] = "NUMERIC";
            break;
          case Types.DOUBLE:
            tmpColumnTypesList[i] = "FLOAT8";
            break;
          case Types.FLOAT:
          case Types.REAL:
            tmpColumnTypesList[i] = "FLOAT4";
            break;
          case Types.INTEGER:
            tmpColumnTypesList[i] = "INT4";
            break;
          case Types.SMALLINT:
          case Types.TINYINT:
            tmpColumnTypesList[i] = "INT2";
            break;
          case Types.TIME:
            tmpColumnTypesList[i] = "TIME";
            break;
          case Types.TIMESTAMP:
            /* timestamp need to mapping to timestamptz by default */
            tmpColumnTypesList[i] = "TIMESTAMPTZ";
            break;
          case Types.OTHER:
          {
            /* get type name from remote server */
            switch (tmpResultSet.getString("TYPE_NAME")) {
              /*mapping type for gridDB*/
              case "BOOL_ARRAY":
                tmpColumnTypesList[i] = "BOOL[]";
                break;
              case "STRING_ARRAY":
                tmpColumnTypesList[i] = "TEXT[]";
                break;
              case "BYTE_ARRAY":
              case "SHORT_ARRAY":
                tmpColumnTypesList[i] = "INT2[]";
                break;
              case "INTEGER_ARRAY":
                tmpColumnTypesList[i] = "INTEGER[]";
                break;
              case "LONG_ARRAY":
                tmpColumnTypesList[i] = "BIGINT[]";
                break;
              case "FLOAT_ARRAY":
                tmpColumnTypesList[i] = "FLOAT4[]";
                break;
              case "DOUBLE_ARRAY":
                tmpColumnTypesList[i] = "FLOAT8[]";
                break;
              case "TIMESTAMP_ARRAY":
                /* Timestamp array from GridDB */
                tmpColumnTypesList[i] = "TIMESTAMPTZ[]";
                break;
              default:
                tmpColumnTypesList[i] = tmpResultSet.getString("TYPE_NAME");
                break;
            }
            break;
          }
          default:
            tmpColumnTypesList[i] = tmpResultSet.getString("TYPE_NAME");
            break;
        }
      }
      return tmpColumnTypesList;
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * getColumnNamesByResultSetID
   *      Returns the column name based on ResultSet
   */
  public String[] getColumnNamesByResultSetID(int resultSetID) throws SQLException {
    try {
      ResultSet tmpResultSet = resultSetInfoMap.get(resultSetID).getResultSet();
      ResultSetMetaData resultSetMetaData = tmpResultSet.getMetaData();
      int columnNumber = resultSetMetaData.getColumnCount();
      String[] tmpColumnNames = new String[columnNumber];

      for (int i = 0; i < columnNumber; i++)
      {
        /* Column's index start from 1, so param of getColumnName is (i + 1)  */
        tmpColumnNames[i] = resultSetMetaData.getColumnName(i + 1);
      }
      return tmpColumnNames;
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * getTableNames
   *      Returns the column name
   */
  public String[] getTableNames() throws SQLException {
    return getTableNames(null, null);
  }

  /*
   * getTableNames
   *      Returns the table names filtered by schema and table patterns
   *      Parameters:
   *        schemaPattern - schema name pattern (null means all schemas)
   *        tableNames - comma-separated list of table names to filter (null means all tables)
   */
  public String[] getTableNames(String schemaPattern, String tableNames) throws SQLException {
    try {
      checkConnExist();
      DatabaseMetaData md = conn.getConnection().getMetaData();
      String databaseProduct = getDatabaseProduct();

      // If no specific tables requested, use wildcard pattern
      String tablePattern = (tableNames == null || tableNames.isEmpty()) ? "%" : null;

      // Parse comma-separated table names if provided
      // Note: We store them in original case for most databases
      // Snowflake requires uppercasing for case-insensitive matching
      Set<String> requestedTablesOriginal = new HashSet<String>();
      Set<String> requestedTablesUpper = new HashSet<String>();
      if (tableNames != null && !tableNames.isEmpty()) {
        String[] tables = tableNames.split(",");
        for (String table : tables) {
          String trimmed = table.trim();
          requestedTablesOriginal.add(trimmed);
          requestedTablesUpper.add(trimmed.toUpperCase());
        }
      }
      // Debug logging (commented out to reduce log spam)
      // System.err.println("DEBUG requestedTables (original): " + requestedTablesOriginal);
      // System.err.println("DEBUG requestedTables (uppercased): " + requestedTablesUpper);

      /*
       * MSSQL uses catalog.schema.table naming where:
       * - catalog = database name (e.g., "master")
       * - schema = schema name (e.g., "dbo")
       * For MSSQL, we need to interpret schemaPattern as catalog name if it doesn't contain a dot
       */
      String catalogPattern = null;
      String actualSchemaPattern = schemaPattern;
      boolean isMSSQL = databaseProduct.contains("sql server") ||
                        databaseProduct.contains("sqlserver") ||
                        databaseProduct.contains("microsoft");

      if (isMSSQL && schemaPattern != null) {
        // For MSSQL, treat schemaPattern as catalog (database) name
        // and use null/% for schema to get all schemas in that database
        catalogPattern = schemaPattern;
        actualSchemaPattern = null;  // Get all schemas within the catalog
        // Debug logging for MSSQL detection
        // System.err.println("DEBUG MSSQL detected: using catalog=" + catalogPattern + ", schema=" + actualSchemaPattern);
      }

      /*
       * Optimization: If schema is specified, pass it to getTables to reduce result set.
       * Otherwise fetch all schemas and filter on Java side for case-insensitive matching.
       */
      ResultSet tmpResultSet;

      // ClickHouse and some databases require explicit table types
      String[] tableTypes = new String[] { "TABLE", "VIEW" };

      if (tablePattern != null) {
        // No specific tables - use pattern matching
        tmpResultSet = md.getTables(catalogPattern, actualSchemaPattern, tablePattern, tableTypes);
      } else {
        // Specific tables requested - fetch all tables from catalog/schema
        tmpResultSet = md.getTables(catalogPattern, actualSchemaPattern, "%", tableTypes);
      }

      List<String> tmpTableNamesList = new ArrayList<String>();
      int rowCount = 0;
      while (tmpResultSet.next()) {
        String tableName = tmpResultSet.getString(3);
        String schemaName = tmpResultSet.getString(2);
        String catalogName = tmpResultSet.getString(1);
        rowCount++;
        // Reduce log spam - only log matched tables
        // System.err.println("DEBUG Row " + rowCount + ": catalog=" + catalogName + ", schema=" + schemaName + ", table=" + tableName);

        // For MSSQL, we already filtered by catalog in getTables(), so skip schema matching
        // For other databases, filter by schema name (case-insensitive)
        boolean schemaMatches = false;
        if (isMSSQL) {
          schemaMatches = true;  // Already filtered by catalog
        } else if (actualSchemaPattern == null) {
          schemaMatches = true;  // No schema filter
        } else if (schemaName != null && schemaName.equalsIgnoreCase(actualSchemaPattern)) {
          schemaMatches = true;
        }

        if (!schemaMatches) {
          // System.err.println("DEBUG   -> schema doesn't match '" + actualSchemaPattern + "', skipping");
          continue;
        }

        // If specific tables were requested, filter to only those
        // For Snowflake: uppercase matching (case-insensitive via uppercase)
        // For others: try exact case match first, then case-insensitive as fallback
        boolean tableMatches = false;
        boolean isSnowflake = databaseProduct.contains("snowflake");

        if (requestedTablesOriginal.isEmpty() && requestedTablesUpper.isEmpty()) {
          tableMatches = true;  // No table filter
        } else if (isSnowflake) {
          // Snowflake: case-insensitive via uppercase
          tableMatches = requestedTablesUpper.contains(tableName.toUpperCase());
        } else {
          // Others: try exact match first (for case-sensitive DBs), then case-insensitive
          tableMatches = requestedTablesOriginal.contains(tableName) ||
                        requestedTablesUpper.contains(tableName.toUpperCase());
        }

        if (tableMatches) {
          tmpTableNamesList.add(tableName);
        }
      }

      String[] tmpTableNames = new String[tmpTableNamesList.size()];
      for (int i = 0; i < tmpTableNamesList.size(); i++) {
        tmpTableNames[i] = tmpTableNamesList.get(i);
      }
      return tmpTableNames;
    } catch (Throwable e) {
      // Log the full exception for debugging
      System.err.println("ERROR in getTableNames: " + e.getClass().getName() + ": " + e.getMessage());
      e.printStackTrace(System.err);
      throw e;
    }
  }

  /*
   * getColumnNames
   *      Returns the column name
   */
  public String[] getColumnNames(String tableName) throws SQLException {
    try {
      checkConnExist();
      DatabaseMetaData md = conn.getConnection().getMetaData();
      ResultSet tmpResultSet = md.getColumns(null, null, tableName, null);
      List<String> tmpColumnNamesList = new ArrayList<String>();
      while (tmpResultSet.next()) {
        tmpColumnNamesList.add(tmpResultSet.getString("COLUMN_NAME"));
      }
      String[] tmpColumnNames = new String[tmpColumnNamesList.size()];
      for (int i = 0; i < tmpColumnNamesList.size(); i++) {
        tmpColumnNames[i] = tmpColumnNamesList.get(i);
      }
      return tmpColumnNames;
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * getDatabaseProduct
   *      Returns the database product name in lowercase
   */
  private String getDatabaseProduct() throws SQLException {
    try {
      checkConnExist();
      Connection c = conn.getConnection();
      DatabaseMetaData md = c.getMetaData();
      String product = md.getDatabaseProductName();
      return product.toLowerCase();
    } catch (Throwable e) {
      System.err.println("=== getDatabaseProduct FAILED: " + e.getClass().getName() + ": " + e.getMessage());
      e.printStackTrace(System.err);
      throw e;
    }
  }

  /*
   * getColumnTypes
   *      Returns the column name
   */
  public String[] getColumnTypes(String tableName) throws SQLException {
    try {
      String databaseProduct = getDatabaseProduct();

      // Route to database-specific type mapper
      if (databaseProduct.contains("bigquery") || databaseProduct.contains("google bigquery")) {
        return getBigQueryColumnTypes(tableName);
      } else if (databaseProduct.contains("snowflake")) {
        return getSnowflakeColumnTypes(tableName);
      } else if (databaseProduct.contains("clickhouse")) {
        return getClickHouseColumnTypes(tableName);
      } else if (databaseProduct.contains("sql server") ||
                 databaseProduct.contains("sqlserver") ||
                 databaseProduct.contains("microsoft")) {
        return getMSSQLColumnTypes(tableName);
      } else {
        // Default/generic mapper (original implementation for GridDB, etc.)
        return getGenericColumnTypes(tableName);
      }
    } catch (Throwable e) {
      // Log the full exception for debugging
      System.err.println("ERROR in getColumnTypes: " + e.getClass().getName() + ": " + e.getMessage());
      e.printStackTrace(System.err);
      throw e;
    }
  }

  /*
   * getGenericColumnTypes
   *      Returns PostgreSQL column types for generic JDBC databases (GridDB, etc.)
   */
  private String[] getGenericColumnTypes(String tableName) throws SQLException {
    checkConnExist();
    DatabaseMetaData md = conn.getConnection().getMetaData();
    ResultSet tmpResultSet = md.getColumns(null, null, tableName, null);
    List<String> tmpColumnTypesList = new ArrayList<String>();
    while (tmpResultSet.next()) {
      tmpColumnTypesList.add(tmpResultSet.getString("TYPE_NAME"));
    }
    String[] tmpColumnTypes = new String[tmpColumnTypesList.size()];
    for (int i = 0; i < tmpColumnTypesList.size(); i++) {
      switch (tmpColumnTypesList.get(i)) {
        case "BYTE":
        case "SHORT":
          tmpColumnTypes[i] = "SMALLINT";
          break;
        case "LONG":
          tmpColumnTypes[i] = "BIGINT";
          break;
        case "CHAR":
          tmpColumnTypes[i] = "CHAR (1)";
          break;
        case "STRING":
          tmpColumnTypes[i] = "TEXT";
          break;
        case "FLOAT":
          tmpColumnTypes[i] = "FLOAT4";
          break;
        case "DOUBLE":
          tmpColumnTypes[i] = "FLOAT8";
          break;
        case "BLOB":
          tmpColumnTypes[i] = "BYTEA";
          break;
        case "BOOL_ARRAY":
          tmpColumnTypes[i] = "BOOL[]";
          break;
        case "STRING_ARRAY":
          tmpColumnTypes[i] = "TEXT[]";
          break;
        case "BYTE_ARRAY":
        case "SHORT_ARRAY":
          tmpColumnTypes[i] = "SMALLINT[]";
          break;
        case "INTEGER_ARRAY":
          tmpColumnTypes[i] = "INTEGER[]";
          break;
        case "LONG_ARRAY":
          tmpColumnTypes[i] = "BIGINT[]";
          break;
        case "FLOAT_ARRAY":
          tmpColumnTypes[i] = "FLOAT4[]";
          break;
        case "DOUBLE_ARRAY":
          tmpColumnTypes[i] = "FLOAT8[]";
          break;
        case "TIMESTAMP_ARRAY":
          tmpColumnTypes[i] = "TIMESTAMP[]";
          break;
        default:
          tmpColumnTypes[i] = tmpColumnTypesList.get(i);
      }
    }
    return tmpColumnTypes;
  }

  /*
   * getBigQueryColumnTypes
   *      Returns PostgreSQL column types for BigQuery database with intelligent fallbacks
   */
  private String[] getBigQueryColumnTypes(String tableName) throws SQLException {
    checkConnExist();
    DatabaseMetaData md = conn.getConnection().getMetaData();
    ResultSet rs = md.getColumns(null, null, tableName, null);
    List<String> types = new ArrayList<String>();
    List<String> warnings = new ArrayList<String>();

    while (rs.next()) {
      String typeName = rs.getString("TYPE_NAME");
      String columnName = rs.getString("COLUMN_NAME");
      String pgType;

      if (typeName == null) {
        typeName = "";
      }

      switch (typeName.toUpperCase()) {
        case "INT64":
        case "INTEGER":
          pgType = "BIGINT";
          break;
        case "FLOAT64":
        case "FLOAT":
          pgType = "FLOAT8";
          break;
        case "NUMERIC":
          pgType = "NUMERIC";
          break;
        case "BIGNUMERIC":
          pgType = "NUMERIC";
          warnings.add("Column '" + columnName + "': BIGNUMERIC may lose precision in NUMERIC");
          break;
        case "STRING":
          pgType = "TEXT";
          break;
        case "BYTES":
          pgType = "BYTEA";
          break;
        case "BOOL":
        case "BOOLEAN":
          pgType = "BOOLEAN";
          break;
        case "DATE":
          pgType = "DATE";
          break;
        case "TIME":
          pgType = "TIME";
          break;
        case "DATETIME":
          pgType = "TIMESTAMP";
          break;
        case "TIMESTAMP":
          pgType = "TIMESTAMPTZ";
          break;
        case "JSON":
          pgType = "JSONB";
          break;
        case "GEOGRAPHY":
          pgType = "TEXT";
          warnings.add("Column '" + columnName + "': GEOGRAPHY mapped to TEXT (WKT format)");
          break;
        case "INTERVAL":
          pgType = "INTERVAL";
          warnings.add("Column '" + columnName + "': INTERVAL semantics may differ");
          break;
        default:
          // Handle complex types: STRUCT, ARRAY
          if (typeName.toUpperCase().startsWith("STRUCT")) {
            pgType = "JSONB";
            warnings.add("Column '" + columnName + "': STRUCT mapped to JSONB (structure flattened)");
          } else if (typeName.toUpperCase().startsWith("ARRAY")) {
            pgType = handleBigQueryArray(typeName, columnName, warnings);
          } else {
            pgType = "TEXT";
            warnings.add("Column '" + columnName + "': Unknown type '" + typeName + "' mapped to TEXT");
          }
      }

      types.add(pgType);
    }

    // Store warnings for later retrieval
    if (!warnings.isEmpty()) {
      typeConversionWarnings.put(tableName, warnings);
    }

    return types.toArray(new String[0]);
  }

  /*
   * handleBigQueryArray
   *      Handles BigQuery ARRAY types, converting simple arrays to PostgreSQL arrays
   *      and complex arrays (ARRAY<STRUCT>) to JSONB
   */
  private String handleBigQueryArray(String typeName, String columnName, List<String> warnings) {
    // Check if complex array (contains STRUCT)
    if (typeName.toUpperCase().contains("STRUCT")) {
      warnings.add("Column '" + columnName + "': ARRAY<STRUCT> mapped to JSONB");
      return "JSONB";
    }

    // Parse ARRAY<TYPE> to TYPE[]
    if (typeName.startsWith("ARRAY<") && typeName.endsWith(">")) {
      String elementType = typeName.substring(6, typeName.length() - 1).toUpperCase();
      switch (elementType) {
        case "INT64":
        case "INTEGER":
          return "BIGINT[]";
        case "FLOAT64":
        case "FLOAT":
          return "FLOAT8[]";
        case "STRING":
          return "TEXT[]";
        case "BOOL":
        case "BOOLEAN":
          return "BOOLEAN[]";
        case "TIMESTAMP":
          return "TIMESTAMPTZ[]";
        case "DATE":
          return "DATE[]";
        case "NUMERIC":
          return "NUMERIC[]";
        default:
          warnings.add("Column '" + columnName + "': Complex ARRAY type mapped to JSONB");
          return "JSONB";
      }
    }

    // Fallback for unrecognized array format
    warnings.add("Column '" + columnName + "': ARRAY type mapped to JSONB");
    return "JSONB";
  }

  /*
   * getSnowflakeColumnTypes
   *      Returns PostgreSQL column types for Snowflake database with intelligent fallbacks
   */
  private String[] getSnowflakeColumnTypes(String tableName) throws SQLException {
    checkConnExist();
    DatabaseMetaData md = conn.getConnection().getMetaData();
    ResultSet rs = md.getColumns(null, null, tableName, null);
    List<String> types = new ArrayList<String>();
    List<String> warnings = new ArrayList<String>();

    while (rs.next()) {
      String typeName = rs.getString("TYPE_NAME");
      String columnName = rs.getString("COLUMN_NAME");
      int precision = rs.getInt("COLUMN_SIZE");
      int scale = rs.getInt("DECIMAL_DIGITS");
      String pgType;

      if (typeName == null) {
        typeName = "";
      }

      switch (typeName.toUpperCase()) {
        case "NUMBER":
          if (scale == 0) {
            // Integer types - map based on precision
            if (precision <= 4) {
              pgType = "SMALLINT";
            } else if (precision <= 9) {
              pgType = "INTEGER";
            } else {
              pgType = "BIGINT";
            }
          } else {
            // Decimal types
            pgType = String.format("NUMERIC(%d,%d)", precision, scale);
          }
          break;
        case "FLOAT":
        case "FLOAT4":
        case "FLOAT8":
        case "DOUBLE":
        case "DOUBLE PRECISION":
        case "REAL":
          pgType = "FLOAT8";
          break;
        case "VARCHAR":
        case "STRING":
        case "TEXT":
        case "CHAR":
        case "CHARACTER":
          pgType = "TEXT";
          break;
        case "BINARY":
        case "VARBINARY":
          pgType = "BYTEA";
          break;
        case "BOOLEAN":
          pgType = "BOOLEAN";
          break;
        case "DATE":
          pgType = "DATE";
          break;
        case "TIME":
          pgType = "TIME";
          break;
        case "DATETIME":
        case "TIMESTAMP":
        case "TIMESTAMP_NTZ":
        case "TIMESTAMPNTZ":
          pgType = "TIMESTAMP";
          break;
        case "TIMESTAMP_LTZ":
        case "TIMESTAMPLTZ":
        case "TIMESTAMP_TZ":
        case "TIMESTAMPTZ":
          pgType = "TIMESTAMPTZ";
          break;
        case "VARIANT":
          pgType = "JSONB";
          warnings.add("Column '" + columnName + "': VARIANT mapped to JSONB");
          break;
        case "OBJECT":
          pgType = "JSONB";
          warnings.add("Column '" + columnName + "': OBJECT mapped to JSONB");
          break;
        case "ARRAY":
          pgType = "JSONB";
          warnings.add("Column '" + columnName + "': Semi-structured ARRAY mapped to JSONB");
          break;
        case "MAP":
          pgType = "JSONB";
          warnings.add("Column '" + columnName + "': MAP mapped to JSONB");
          break;
        case "GEOGRAPHY":
        case "GEOMETRY":
          pgType = "TEXT";
          warnings.add("Column '" + columnName + "': " + typeName + " mapped to TEXT (WKT/GeoJSON format)");
          break;
        case "VECTOR":
          pgType = "JSONB";
          warnings.add("Column '" + columnName + "': VECTOR mapped to JSONB (consider pgvector extension)");
          break;
        case "FILE":
        case "UNSTRUCTURED":
          pgType = "TEXT";
          warnings.add("Column '" + columnName + "': " + typeName + " mapped to TEXT (file reference)");
          break;
        default:
          pgType = "TEXT";
          warnings.add("Column '" + columnName + "': Unknown type '" + typeName + "' mapped to TEXT");
      }

      types.add(pgType);
    }

    // Store warnings for later retrieval
    if (!warnings.isEmpty()) {
      typeConversionWarnings.put(tableName, warnings);
    }

    return types.toArray(new String[0]);
  }

  /*
   * getClickHouseColumnTypes
   *      Returns PostgreSQL column types for ClickHouse database with intelligent fallbacks
   */
  private String[] getClickHouseColumnTypes(String tableName) throws SQLException {
    checkConnExist();
    DatabaseMetaData md = conn.getConnection().getMetaData();
    ResultSet rs = md.getColumns(null, null, tableName, null);
    List<String> types = new ArrayList<String>();
    List<String> warnings = new ArrayList<String>();

    while (rs.next()) {
      String typeName = rs.getString("TYPE_NAME");
      String columnName = rs.getString("COLUMN_NAME");
      int precision = rs.getInt("COLUMN_SIZE");
      int scale = rs.getInt("DECIMAL_DIGITS");
      String pgType;

      if (typeName == null) {
        typeName = "";
      }

      // ClickHouse wraps nullable types as Nullable(T), strip wrapper if present
      String baseType = typeName;
      boolean isNullable = false;
      if (typeName.toUpperCase().startsWith("NULLABLE(") && typeName.endsWith(")")) {
        baseType = typeName.substring(9, typeName.length() - 1);
        isNullable = true;
      }

      // Normalize to uppercase for comparison
      String normalizedType = baseType.toUpperCase().trim();

      // Handle ClickHouse data types
      if (normalizedType.equals("INT8") || normalizedType.equals("TINYINT")) {
        pgType = "SMALLINT";
      } else if (normalizedType.equals("INT16") || normalizedType.equals("SMALLINT")) {
        pgType = "SMALLINT";
      } else if (normalizedType.equals("INT32") || normalizedType.equals("INT") || normalizedType.equals("INTEGER")) {
        pgType = "INTEGER";
      } else if (normalizedType.equals("INT64") || normalizedType.equals("BIGINT")) {
        pgType = "BIGINT";
      } else if (normalizedType.equals("UINT8")) {
        pgType = "SMALLINT";
        warnings.add("Column '" + columnName + "': UInt8 mapped to SMALLINT (may overflow if value > 127)");
      } else if (normalizedType.equals("UINT16")) {
        pgType = "INTEGER";
        warnings.add("Column '" + columnName + "': UInt16 mapped to INTEGER (may overflow if value > 32767)");
      } else if (normalizedType.equals("UINT32")) {
        pgType = "BIGINT";
        warnings.add("Column '" + columnName + "': UInt32 mapped to BIGINT (may overflow if value > 2147483647)");
      } else if (normalizedType.equals("UINT64")) {
        pgType = "NUMERIC(20,0)";
        warnings.add("Column '" + columnName + "': UInt64 mapped to NUMERIC(20,0) (PostgreSQL BIGINT cannot represent full range)");
      } else if (normalizedType.equals("FLOAT32") || normalizedType.equals("FLOAT")) {
        pgType = "REAL";
      } else if (normalizedType.equals("FLOAT64") || normalizedType.equals("DOUBLE")) {
        pgType = "DOUBLE PRECISION";
      } else if (normalizedType.startsWith("DECIMAL") || normalizedType.startsWith("NUMERIC")) {
        if (precision > 0 && scale >= 0) {
          pgType = String.format("NUMERIC(%d,%d)", precision, scale);
        } else {
          pgType = "NUMERIC";
        }
      } else if (normalizedType.equals("STRING") || normalizedType.equals("TEXT") ||
                 normalizedType.equals("VARCHAR") || normalizedType.equals("CHAR")) {
        pgType = "TEXT";
      } else if (normalizedType.startsWith("FIXEDSTRING")) {
        // FixedString(N) -> VARCHAR(N)
        if (precision > 0) {
          pgType = String.format("VARCHAR(%d)", precision);
        } else {
          pgType = "VARCHAR";
        }
      } else if (normalizedType.equals("UUID")) {
        pgType = "UUID";
      } else if (normalizedType.equals("DATE") || normalizedType.equals("DATE32")) {
        pgType = "DATE";
      } else if (normalizedType.equals("DATETIME") || normalizedType.startsWith("DATETIME64")) {
        pgType = "TIMESTAMP";
      } else if (normalizedType.equals("TIMESTAMP")) {
        pgType = "TIMESTAMP";
      } else if (normalizedType.startsWith("INTERVAL")) {
        pgType = "INTERVAL";
      } else if (normalizedType.equals("BOOLEAN") || normalizedType.equals("BOOL")) {
        pgType = "BOOLEAN";
      } else if (normalizedType.startsWith("ENUM8") || normalizedType.startsWith("ENUM16")) {
        pgType = "TEXT";
        warnings.add("Column '" + columnName + "': " + typeName + " mapped to TEXT (consider creating PostgreSQL ENUM)");
      } else if (normalizedType.startsWith("ARRAY(")) {
        // Extract element type from Array(T)
        String elementType = normalizedType.substring(6, normalizedType.length() - 1).trim();

        // Map common element types to PostgreSQL array types
        if (elementType.equals("INT8") || elementType.equals("INT16")) {
          pgType = "SMALLINT[]";
        } else if (elementType.equals("INT32") || elementType.equals("INT")) {
          pgType = "INTEGER[]";
        } else if (elementType.equals("INT64")) {
          pgType = "BIGINT[]";
        } else if (elementType.equals("FLOAT32")) {
          pgType = "REAL[]";
        } else if (elementType.equals("FLOAT64")) {
          pgType = "DOUBLE PRECISION[]";
        } else if (elementType.equals("STRING")) {
          pgType = "TEXT[]";
        } else if (elementType.equals("UUID")) {
          pgType = "UUID[]";
        } else if (elementType.equals("DATE")) {
          pgType = "DATE[]";
        } else if (elementType.equals("BOOLEAN")) {
          pgType = "BOOLEAN[]";
        } else {
          pgType = "JSONB";
          warnings.add("Column '" + columnName + "': Complex ARRAY type mapped to JSONB");
        }
      } else if (normalizedType.startsWith("TUPLE(")) {
        pgType = "JSONB";
        warnings.add("Column '" + columnName + "': TUPLE type mapped to JSONB (structure flattened)");
      } else if (normalizedType.startsWith("MAP(")) {
        pgType = "JSONB";
        warnings.add("Column '" + columnName + "': MAP type mapped to JSONB");
      } else if (normalizedType.equals("JSON") || normalizedType.startsWith("JSON(")) {
        pgType = "JSONB";
      } else if (normalizedType.startsWith("NESTED(")) {
        pgType = "JSONB";
        warnings.add("Column '" + columnName + "': NESTED type mapped to JSONB (nested structure flattened)");
      } else if (normalizedType.equals("IPV4")) {
        pgType = "INET";
      } else if (normalizedType.equals("IPV6")) {
        pgType = "INET";
      } else if (normalizedType.startsWith("LOWCARDINALITY(")) {
        // LowCardinality(T) - extract base type and map that
        String innerType = normalizedType.substring(15, normalizedType.length() - 1).trim();
        if (innerType.equals("STRING")) {
          pgType = "TEXT";
        } else {
          pgType = "TEXT";
          warnings.add("Column '" + columnName + "': LowCardinality(" + innerType + ") mapped to TEXT");
        }
      } else if (normalizedType.startsWith("AGGREGATEFUNCTION")) {
        pgType = "TEXT";
        warnings.add("Column '" + columnName + "': AggregateFunction type mapped to TEXT (not queryable)");
      } else if (normalizedType.startsWith("SIMPLEAGGREGATEFUNCTION")) {
        pgType = "TEXT";
        warnings.add("Column '" + columnName + "': SimpleAggregateFunction type mapped to TEXT");
      } else {
        pgType = "TEXT";
        warnings.add("Column '" + columnName + "': Unknown ClickHouse type '" + typeName + "' mapped to TEXT");
      }

      types.add(pgType);
    }

    // Store warnings for later retrieval
    if (!warnings.isEmpty()) {
      typeConversionWarnings.put(tableName, warnings);
    }

    return types.toArray(new String[0]);
  }

  /*
   * getMSSQLColumnTypes
   *      Returns PostgreSQL column types for Microsoft SQL Server database with intelligent fallbacks
   */
  private String[] getMSSQLColumnTypes(String tableName) throws SQLException {
    checkConnExist();
    DatabaseMetaData md = conn.getConnection().getMetaData();

    // For MSSQL, try to get columns with different catalog/schema combinations
    // First try: null catalog, null schema (current database)
    ResultSet rs = md.getColumns(null, null, tableName, null);

    // Check if we got any results
    boolean hasResults = false;
    try {
      hasResults = rs.next();
      // Reset to before first row if we have results
      if (hasResults) {
        rs.close();
        rs = md.getColumns(null, null, tableName, null);
      }
    } catch (SQLException e) {
      System.err.println("WARN: Error checking if columns ResultSet has data: " + e.getMessage());
    }

    // If no results, try with % for schema to search all schemas
    if (!hasResults) {
      rs.close();
      rs = md.getColumns(null, "%", tableName, null);
    }

    List<String> types = new ArrayList<String>();
    List<String> warnings = new ArrayList<String>();

    while (rs.next()) {
      String typeName = rs.getString("TYPE_NAME");
      String columnName = rs.getString("COLUMN_NAME");
      int precision = rs.getInt("COLUMN_SIZE");
      int scale = rs.getInt("DECIMAL_DIGITS");
      String pgType;

      if (typeName == null) {
        typeName = "";
      }

      // Normalize to uppercase for comparison
      String normalizedType = typeName.toUpperCase().trim();

      // Handle MSSQL data types
      if (normalizedType.equals("TINYINT")) {
        // MSSQL tinyint is 0-255 (unsigned), PostgreSQL smallint is -32768 to 32767
        pgType = "SMALLINT";
      } else if (normalizedType.equals("SMALLINT")) {
        pgType = "SMALLINT";
      } else if (normalizedType.equals("INT")) {
        pgType = "INTEGER";
      } else if (normalizedType.equals("BIGINT")) {
        pgType = "BIGINT";
      } else if (normalizedType.equals("DECIMAL") || normalizedType.equals("NUMERIC")) {
        if (precision > 0 && scale >= 0) {
          pgType = String.format("NUMERIC(%d,%d)", precision, scale);
        } else {
          pgType = "NUMERIC";
        }
      } else if (normalizedType.equals("MONEY") || normalizedType.equals("SMALLMONEY")) {
        pgType = "NUMERIC(19,4)";
        warnings.add("Column '" + columnName + "': " + typeName + " mapped to NUMERIC(19,4)");
      } else if (normalizedType.equals("FLOAT") || normalizedType.equals("DOUBLE PRECISION")) {
        pgType = "DOUBLE PRECISION";
      } else if (normalizedType.equals("REAL")) {
        pgType = "REAL";
      } else if (normalizedType.equals("BIT")) {
        pgType = "BOOLEAN";
      } else if (normalizedType.equals("CHAR") || normalizedType.equals("NCHAR")) {
        if (precision > 0) {
          pgType = String.format("CHAR(%d)", precision);
        } else {
          pgType = "CHAR";
        }
      } else if (normalizedType.equals("VARCHAR") || normalizedType.equals("NVARCHAR")) {
        if (precision > 0 && precision < 2147483647) {
          pgType = String.format("VARCHAR(%d)", precision);
        } else {
          // varchar(max) or nvarchar(max)
          pgType = "TEXT";
          if (precision == 2147483647) {
            warnings.add("Column '" + columnName + "': " + typeName + "(MAX) mapped to TEXT");
          }
        }
      } else if (normalizedType.equals("TEXT") || normalizedType.equals("NTEXT")) {
        pgType = "TEXT";
        warnings.add("Column '" + columnName + "': " + typeName + " (deprecated in MSSQL) mapped to TEXT");
      } else if (normalizedType.equals("BINARY") || normalizedType.equals("VARBINARY")) {
        if (precision > 0 && precision < 2147483647) {
          pgType = "BYTEA";
        } else {
          // varbinary(max)
          pgType = "BYTEA";
          if (precision == 2147483647) {
            warnings.add("Column '" + columnName + "': VARBINARY(MAX) mapped to BYTEA");
          }
        }
      } else if (normalizedType.equals("IMAGE")) {
        pgType = "BYTEA";
        warnings.add("Column '" + columnName + "': IMAGE (deprecated in MSSQL) mapped to BYTEA");
      } else if (normalizedType.equals("DATE")) {
        pgType = "DATE";
      } else if (normalizedType.equals("TIME")) {
        pgType = "TIME";
      } else if (normalizedType.equals("DATETIME") || normalizedType.equals("SMALLDATETIME")) {
        pgType = "TIMESTAMP";
      } else if (normalizedType.equals("DATETIME2")) {
        pgType = "TIMESTAMP";
      } else if (normalizedType.equals("DATETIMEOFFSET")) {
        pgType = "TIMESTAMPTZ";
      } else if (normalizedType.equals("UNIQUEIDENTIFIER")) {
        pgType = "UUID";
      } else if (normalizedType.equals("XML")) {
        pgType = "XML";
      } else if (normalizedType.equals("JSON")) {
        pgType = "JSONB";
      } else if (normalizedType.equals("GEOGRAPHY")) {
        pgType = "TEXT";
        warnings.add("Column '" + columnName + "': GEOGRAPHY mapped to TEXT (WKT format, consider PostGIS)");
      } else if (normalizedType.equals("GEOMETRY")) {
        pgType = "TEXT";
        warnings.add("Column '" + columnName + "': GEOMETRY mapped to TEXT (WKT format, consider PostGIS)");
      } else if (normalizedType.equals("HIERARCHYID")) {
        pgType = "TEXT";
        warnings.add("Column '" + columnName + "': HIERARCHYID mapped to TEXT (path representation)");
      } else if (normalizedType.equals("SQL_VARIANT")) {
        pgType = "TEXT";
        warnings.add("Column '" + columnName + "': SQL_VARIANT mapped to TEXT (type information lost)");
      } else if (normalizedType.equals("TIMESTAMP") || normalizedType.equals("ROWVERSION")) {
        // MSSQL timestamp/rowversion is a binary row version, not a date/time
        pgType = "BYTEA";
        warnings.add("Column '" + columnName + "': " + typeName + " (binary row version) mapped to BYTEA");
      } else {
        pgType = "TEXT";
        warnings.add("Column '" + columnName + "': Unknown MSSQL type '" + typeName + "' mapped to TEXT");
      }

      types.add(pgType);
    }

    // Store warnings for later retrieval
    if (!warnings.isEmpty()) {
      typeConversionWarnings.put(tableName, warnings);
    }

    if (types.isEmpty()) {
      System.err.println("ERROR: No columns found for MSSQL table: " + tableName);
      throw new SQLException("No columns found for table: " + tableName);
    }

    return types.toArray(new String[0]);
  }

  /*
   * getPrimaryKey
   *      Returns the column name
   */
  public String[] getPrimaryKey(String tableName) throws SQLException {
    try {
      checkConnExist();
      DatabaseMetaData md = conn.getConnection().getMetaData();
      ResultSet tmpResultSet = md.getPrimaryKeys(null, null, tableName);
      List<String> tmpPrimaryKeyList = new ArrayList<String>();
      while (tmpResultSet.next()) {
        tmpPrimaryKeyList.add(tmpResultSet.getString("COLUMN_NAME"));
      }
      String[] tmpPrimaryKey = new String[tmpPrimaryKeyList.size()];
      for (int i = 0; i < tmpPrimaryKeyList.size(); i++) {
        tmpPrimaryKey[i] = tmpPrimaryKeyList.get(i);
      }
      return tmpPrimaryKey;
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * closeStatement
   *      Releases the resources used by statement. Keeps the connection
   *      open for another statement to be executed.
   */
  public void closeStatement() throws SQLException {
    try {
      if (tmpStmt != null) {
        tmpStmt.close();
        tmpStmt = null;
      }
      if (tmpPstmt != null) {
        tmpPstmt.close();
        tmpPstmt = null;
      }
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * cancel
   *      Cancels the query and releases the resources in case query
   *      cancellation is requested by the user.
   */
  public void cancel() throws SQLException {
    try {
      closeStatement();
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * cleanup
   *      Closes the JDBC connection and releases resources.
   *      Called when the JDBCUtils instance is being destroyed.
   */
  public void cleanup() throws SQLException {
    try {
      closeStatement();
      if (conn != null) {
        Connection underlyingConn = conn.getConnection();
        if (underlyingConn != null) {
          underlyingConn.close();
        }
        conn = null;
      }
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * checkConnExist
   *      Check the cennection exist or not.
   *      throw error message when the connection dosn't exist.
   */
  public void checkConnExist() throws IllegalArgumentException {
    if (conn == null) {
      throw new IllegalArgumentException(
          "Must create connection before creating a prepared statment");
    }
  }

  /*
   * checkPstmt
   *      Check the Prepared Statement exists or not.
   *      throw error message when the Prepared Statement doesn't exist.
   */
  public void checkPstmt(PreparedStatement pstmt) throws IllegalArgumentException {
    if (pstmt == null) {
      throw new IllegalArgumentException(
          "Must create a prior prepared statement before execute it");
    }
  }

  /*
   * bindNullPreparedStatement
   *      Bind the value to the PreparedStatement object based on the query
   */
  public void bindNullPreparedStatement(int attnum, int resultSetID) throws SQLException {

    try {
      checkConnExist();
      PreparedStatement tmpPstmt = resultSetInfoMap.get(resultSetID).getPstmt();
      checkPstmt(tmpPstmt);
      tmpPstmt.setNull(attnum, Types.NULL);
      resultSetInfoMap.get(resultSetID).setPstmt(tmpPstmt);
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * bindIntPreparedStatement
   *      Bind the value to the PreparedStatement object based on the query
   */
  public void bindIntPreparedStatement(int values, int attnum, int resultSetID)
      throws SQLException {

    try {
      checkConnExist();
      PreparedStatement tmpPstmt = resultSetInfoMap.get(resultSetID).getPstmt();
      checkPstmt(tmpPstmt);
      tmpPstmt.setInt(attnum, values);
      resultSetInfoMap.get(resultSetID).setPstmt(tmpPstmt);
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * bindLongPreparedStatement
   *      Bind the value to the PreparedStatement object based on the query
   */
  public void bindLongPreparedStatement(long values, int attnum, int resultSetID)
      throws SQLException {

    try {
      checkConnExist();
      PreparedStatement tmpPstmt = resultSetInfoMap.get(resultSetID).getPstmt();
      checkPstmt(tmpPstmt);
      tmpPstmt.setLong(attnum, values);
      resultSetInfoMap.get(resultSetID).setPstmt(tmpPstmt);
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * bindFloatPreparedStatement
   *      Bind the value to the PreparedStatement object based on the query
   */
  public void bindFloatPreparedStatement(float values, int attnum, int resultSetID)
      throws SQLException {

    try {
      checkConnExist();
      PreparedStatement tmpPstmt = resultSetInfoMap.get(resultSetID).getPstmt();
      checkPstmt(tmpPstmt);
      tmpPstmt.setFloat(attnum, values);
      resultSetInfoMap.get(resultSetID).setPstmt(tmpPstmt);
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * bindDoublePreparedStatement
   *      Bind the value to the PreparedStatement object based on the query
   */
  public void bindDoublePreparedStatement(double values, int attnum, int resultSetID)
      throws SQLException {

    try {
      checkConnExist();
      PreparedStatement tmpPstmt = resultSetInfoMap.get(resultSetID).getPstmt();
      checkPstmt(tmpPstmt);
      tmpPstmt.setDouble(attnum, values);
      resultSetInfoMap.get(resultSetID).setPstmt(tmpPstmt);
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * bindBooleanPreparedStatement
   *      Bind the value to the PreparedStatement object based on the query
   */
  public void bindBooleanPreparedStatement(boolean values, int attnum, int resultSetID)
      throws SQLException {

    try {
      checkConnExist();
      PreparedStatement tmpPstmt = resultSetInfoMap.get(resultSetID).getPstmt();
      checkPstmt(tmpPstmt);
      tmpPstmt.setBoolean(attnum, values);
      resultSetInfoMap.get(resultSetID).setPstmt(tmpPstmt);
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * bindStringPreparedStatement
   *      Bind the value to the PreparedStatement object based on the query
   */
  public void bindStringPreparedStatement(String values, int attnum, int resultSetID)
      throws SQLException {

    try {
      checkConnExist();
      PreparedStatement tmpPstmt = resultSetInfoMap.get(resultSetID).getPstmt();
      checkPstmt(tmpPstmt);
      tmpPstmt.setString(attnum, values);
      resultSetInfoMap.get(resultSetID).setPstmt(tmpPstmt);
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * bindByteaPreparedStatement
   *      Bind the value to the PreparedStatement object based on the query
   */
  public void bindByteaPreparedStatement(byte[] dat, long length, int attnum, int resultSetID)
      throws SQLException {

    try {
      checkConnExist();
      PreparedStatement tmpPstmt = resultSetInfoMap.get(resultSetID).getPstmt();
      checkPstmt(tmpPstmt);
      InputStream targetStream = new ByteArrayInputStream(dat);
      tmpPstmt.setBinaryStream(attnum, targetStream, length);
      resultSetInfoMap.get(resultSetID).setPstmt(tmpPstmt);
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * bindTimePreparedStatement
   *      Bind the value to the PreparedStatement object based on the query
   */
  public void bindTimePreparedStatement(String values, int attnum, int resultSetID)
      throws SQLException {

    try {
      checkConnExist();
      PreparedStatement tmpPstmt = resultSetInfoMap.get(resultSetID).getPstmt();
      checkPstmt(tmpPstmt);
      String pattern = "[HH:mm:ss][.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S][z][XXX][X]";
      LocalTime localTime = LocalTime.parse(values, DateTimeFormatter.ofPattern(pattern));
      tmpPstmt.setObject(attnum, localTime);
      resultSetInfoMap.get(resultSetID).setPstmt(tmpPstmt);
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * bindTimeTZPreparedStatement
   *      Bind the value to the PreparedStatement object based on the query
   *      set with localtime: might lost time-zone
   */
  public void bindTimeTZPreparedStatement(String values, int attnum, int resultSetID)
      throws SQLException {

    try {
      checkConnExist();
      PreparedStatement tmpPstmt = resultSetInfoMap.get(resultSetID).getPstmt();
      checkPstmt(tmpPstmt);

      String pattern = "[HH:mm:ss][.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S][z][XXX][X]";
      LocalTime localTime = LocalTime.parse(values, DateTimeFormatter.ofPattern(pattern));
      tmpPstmt.setObject(attnum, localTime);
      resultSetInfoMap.get(resultSetID).setPstmt(tmpPstmt);
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * Set timestamp to prepared statement
   * Use the UTC time zone as default to avoid being affected by the JVM time zone
   */
  private void setTimestamp(PreparedStatement preparedStatement, int attnum, Timestamp timestamp)
    throws SQLException {
      java.util.Calendar cal = Calendar.getInstance();
      cal.setTimeZone(TimeZone.getTimeZone("UTC"));
      try {
        /* Specify time zone (cal) if possible */
        preparedStatement.setTimestamp(attnum, timestamp, cal);
      } catch (SQLFeatureNotSupportedException e) {
        /* GridDB only, no calendar support in setTimestamp() */
        preparedStatement.setTimestamp(attnum, timestamp);
      } catch (Throwable e) {
        throw e;
      }
    }

  /*
   * bindTimestampPreparedStatement
   *      Bind the value to the PreparedStatement object based on the query
   */
  public void bindTimestampPreparedStatement(long usec, int attnum, int resultSetID)
    throws SQLException {
    try {
      checkConnExist();
      PreparedStatement tmpPstmt = resultSetInfoMap.get(resultSetID).getPstmt();
      checkPstmt(tmpPstmt);
      Instant instant = Instant.EPOCH.plus(usec, ChronoUnit.MICROS);
      Timestamp timestamp = Timestamp.from(instant);
      setTimestamp(tmpPstmt, attnum, timestamp);
      resultSetInfoMap.get(resultSetID).setPstmt(tmpPstmt);
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * bindDatePreparedStatement
   *      Bind the value to the PreparedStatement object based on the query
   */
  public void bindDatePreparedStatement(String values, int attnum, int resultSetID)
    throws SQLException {
    try {
      checkConnExist();
      PreparedStatement tmpPstmt = resultSetInfoMap.get(resultSetID).getPstmt();
      checkPstmt(tmpPstmt);
      tmpPstmt.setDate(attnum, java.sql.Date.valueOf(values));
      resultSetInfoMap.get(resultSetID).setPstmt(tmpPstmt);
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * Avoid race case.
   */
  synchronized public int initResultSetKey() throws Exception{
    try{
      int datum = resultSetKey;
      while (resultSetInfoMap.containsKey(resultSetKey)) {
        /* avoid giving minus key */
        if (resultSetKey == Integer.MAX_VALUE) {
          resultSetKey = 1;
        }
        resultSetKey++;
        /* resultSetKey full */
        if (resultSetKey == datum) {
          throw new SQLException("resultSetKey is full");
        }
      }
      return resultSetKey;
    } catch (Throwable e) {
      throw e;
    }
  }

  /*
   * Get identifier quote char from remote server
   */
  public String getIdentifierQuoteString() throws SQLException{
    try{
      checkConnExist();
      DatabaseMetaData md = conn.getConnection().getMetaData();
      return md.getIdentifierQuoteString();
    } catch (Throwable e) {
      throw e;
    }
  }

  /* finalize all actived connection */
  public static void finalizeAllConns(long hashvalue) throws Exception {
    JDBCConnection.finalizeAllConns(hashvalue);
  }

  /* finalize connection have given server_hashvalue */
  public static void finalizeAllServerConns(long hashvalue) throws Exception {
    JDBCConnection.finalizeAllServerConns(hashvalue);
  }

  /* finalize connection have given mapping_hashvalue */
  public static void finalizeAllUserMapingConns(long hashvalue) throws Exception {
    JDBCConnection.finalizeAllUserMapingConns(hashvalue);
  }

  /* finalize cached result set */
  public static void finalizeAllResultSet() {
    resultSetInfoMap.clear();
  }

  /*
   * getTypeConversionWarnings
   *      Returns the type conversion warnings for a given table
   */
  public String[] getTypeConversionWarnings(String tableName) {
    List<String> warnings = typeConversionWarnings.get(tableName);
    if (warnings == null || warnings.isEmpty()) {
      return new String[0];
    }
    return warnings.toArray(new String[0]);
  }
}
