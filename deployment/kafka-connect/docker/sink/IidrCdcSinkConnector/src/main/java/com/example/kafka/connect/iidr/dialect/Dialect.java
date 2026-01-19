package com.example.kafka.connect.iidr.dialect;

import com.example.kafka.connect.iidr.writer.JdbcWriter.ProcessedRecord;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Interface for database-specific dialect logic.
 *
 * This allows the connector to support multiple target databases by abstracting
 * away differences in SQL syntax, data types, and DDL operations.
 */
public interface Dialect {

    /**
     * Get the name of the dialect.
     */
    String getName();

    /**
     * Build an INSERT SQL statement for the given table and columns.
     */
    String buildInsertSql(String tableName, List<String> columns);

    /**
     * Build an UPDATE SQL statement for the given table and columns.
     */
    String buildUpdateSql(String tableName, List<String> columns, List<String> pkColumns);

    /**
     * Build an UPSERT SQL statement for the given table and columns.
     */
    String buildUpsertSql(String tableName, List<String> columns, List<String> pkColumns);

    /**
     * Build a DELETE SQL statement for the given table and columns.
     */
    String buildDeleteSql(String tableName, List<String> pkColumns);

    /**
     * Build a CREATE TABLE statement for the given table and record schema.
     */
    String buildCreateTableSql(String tableName, ProcessedRecord sample);

    /**
     * Build an ALTER TABLE statement to add new columns.
     */
    String buildAlterTableSql(String tableName, List<String> missingColumns, ProcessedRecord sample);

    /**
     * Get the database-specific column type for a given Kafka Connect schema type.
     */
    String getColumnType(org.apache.kafka.connect.data.Schema schema);
}
