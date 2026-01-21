package com.example.kafka.connect.iidr.dialect;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Dialect for PostgreSQL.
 */
public class PostgreSqlDialect extends GenericDialect {

    @Override
    public String getName() {
        return "PostgreSQL";
    }

    @Override
    public String buildUpsertSql(String tableName, List<String> columns, List<String> pkColumns) {
        String cols = String.join(", ", columns);
        String pkCols = String.join(", ", pkColumns);
        String placeholders = String.join(", ", java.util.Collections.nCopies(columns.size(), "?"));

        List<String> nonPkColumns = columns.stream()
                .filter(c -> !pkColumns.contains(c))
                .collect(Collectors.toList());

        String updateClause;
        if (nonPkColumns.isEmpty()) {
            // All columns are part of the primary key, so there's nothing to update.
            // We can use a special "DO NOTHING" clause.
            return String.format("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO NOTHING",
                                 tableName, cols, placeholders, pkCols);
        } else {
            updateClause = nonPkColumns.stream()
                .map(col -> String.format("%s = EXCLUDED.%s", col, col))
                .collect(Collectors.joining(", "));
             return String.format("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
                                 tableName, cols, placeholders, pkCols, updateClause);
        }
    }

    @Override
    public String getColumnType(org.apache.kafka.connect.data.Schema schema) {
        if (schema == null) {
            return "TEXT";
        }
        switch (schema.type()) {
            case INT8:
                return "SMALLINT";
            case INT16:
                return "SMALLINT";
            case INT32:
                return "INT";
            case INT64:
                return "BIGINT";
            case FLOAT32:
                return "REAL";
            case FLOAT64:
                return "DOUBLE PRECISION";
            case BOOLEAN:
                return "BOOLEAN";
            case STRING:
                return "VARCHAR(255)";
            case BYTES:
                return "BYTEA";
            default:
                return "TEXT";
        }
    }

    @Override
    protected String inferColumnType(Object value) {
        if (value == null) {
            return "TEXT";
        }
        if (value instanceof Integer || value instanceof Long) {
            return "BIGINT";
        }
        if (value instanceof Double || value instanceof Float) {
            return "DOUBLE PRECISION";
        }
        if (value instanceof Boolean) {
            return "BOOLEAN";
        }
        if (value instanceof String) {
            String str = (String) value;
            if (str.length() > 255) {
                return "TEXT";
            }
            return "VARCHAR(1024)";
        }
        return "TEXT";
    }

    @Override
    public String normalizeIdentifierForMetadata(String identifier) {
        // PostgreSQL stores unquoted identifiers in lowercase
        return identifier != null ? identifier.toLowerCase() : null;
    }
}
