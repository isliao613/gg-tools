package com.example.kafka.connect.iidr.dialect;

import java.util.List;

/**
 * Dialect for MySQL.
 */
public class MySqlDialect extends GenericDialect {

    @Override
    public String getName() {
        return "MySQL";
    }

    @Override
    public String buildUpsertSql(String tableName, List<String> columns, List<String> pkColumns) {
        String cols = String.join(", ", columns);
        String placeholders = String.join(", ", java.util.Collections.nCopies(columns.size(), "?"));

        StringBuilder updateClause = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                updateClause.append(", ");
            }
            String col = columns.get(i);
            updateClause.append(col).append(" = VALUES(").append(col).append(")");
        }

        return String.format("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
                tableName, cols, placeholders, updateClause);
    }

    @Override
    public String getColumnType(org.apache.kafka.connect.data.Schema schema) {
        if (schema == null) {
            return "TEXT";
        }
        switch (schema.type()) {
            case INT8:
                return "TINYINT";
            case INT16:
                return "SMALLINT";
            case INT32:
                return "INT";
            case INT64:
                return "BIGINT";
            case FLOAT32:
                return "FLOAT";
            case FLOAT64:
                return "DOUBLE";
            case BOOLEAN:
                return "BOOLEAN";
            case STRING:
                return "VARCHAR(255)";
            case BYTES:
                return "VARBINARY(255)";
            default:
                return "TEXT";
        }
    }
}
