package com.example.kafka.connect.iidr.dialect;

import com.example.kafka.connect.iidr.writer.JdbcWriter.ProcessedRecord;
import org.apache.kafka.connect.data.Schema;

import java.util.List;
import java.util.logging.Logger;

/**
 * A generic dialect for databases that are not explicitly supported.
 * Provides basic functionality but may not be optimal.
 */
public class GenericDialect implements Dialect {

    private static final Logger log = Logger.getLogger(GenericDialect.class.getName());

    @Override
    public String getName() {
        return "Generic";
    }

    @Override
    public String buildInsertSql(String tableName, List<String> columns) {
        String cols = String.join(", ", columns);
        String placeholders = String.join(", ", java.util.Collections.nCopies(columns.size(), "?"));
        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, cols, placeholders);
    }

    @Override
    public String buildUpdateSql(String tableName, List<String> columns, List<String> pkColumns) {
        List<String> nonPkColumns = new java.util.ArrayList<>(columns);
        nonPkColumns.removeAll(pkColumns);

        StringBuilder setClause = new StringBuilder();
        for (int i = 0; i < nonPkColumns.size(); i++) {
            if (i > 0) {
                setClause.append(", ");
            }
            setClause.append(nonPkColumns.get(i)).append(" = ?");
        }

        StringBuilder whereClause = new StringBuilder();
        for (int i = 0; i < pkColumns.size(); i++) {
            if (i > 0) {
                whereClause.append(" AND ");
            }
            whereClause.append(pkColumns.get(i)).append(" = ?");
        }

        return String.format("UPDATE %s SET %s WHERE %s", tableName, setClause, whereClause);
    }


    @Override
    public String buildUpsertSql(String tableName, List<String> columns, List<String> pkColumns) {
        log.warning("UPSERT not supported by the generic dialect. " +
                    "Consider using a database-specific dialect for better performance.");
        // Fallback to a simple INSERT, which might fail on duplicates
        String cols = String.join(", ", columns);
        String placeholders = String.join(", ", java.util.Collections.nCopies(columns.size(), "?"));
        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, cols, placeholders);
    }

    @Override
    public String buildDeleteSql(String tableName, List<String> pkColumns) {
        StringBuilder whereClause = new StringBuilder();
        for (int i = 0; i < pkColumns.size(); i++) {
            if (i > 0) {
                whereClause.append(" AND ");
            }
            whereClause.append(pkColumns.get(i)).append(" = ?");
        }
        return String.format("DELETE FROM %s WHERE %s", tableName, whereClause);
    }


    @Override
    public String buildCreateTableSql(String tableName, ProcessedRecord sample) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(tableName).append(" (");

        List<String> columns = new java.util.ArrayList<>();
        if (sample.getValue() instanceof org.apache.kafka.connect.data.Struct) {
            org.apache.kafka.connect.data.Struct struct = (org.apache.kafka.connect.data.Struct) sample.getValue();
            for (org.apache.kafka.connect.data.Field field : struct.schema().fields()) {
                columns.add(field.name());
            }
        } else if (sample.getValue() instanceof java.util.Map) {
            columns.addAll(((java.util.Map) sample.getValue()).keySet());
        }

        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                ddl.append(", ");
            }
            String columnName = columns.get(i);
            Schema fieldSchema = sample.getValueSchema() != null ? sample.getValueSchema().field(columnName).schema() : null;
            ddl.append(columnName).append(" ").append(getColumnType(fieldSchema));
        }
        ddl.append(")");
        return ddl.toString();
    }

    @Override
    public String buildAlterTableSql(String tableName, List<String> missingColumns, ProcessedRecord sample) {
        StringBuilder ddl = new StringBuilder();
        for (String column : missingColumns) {
            Schema fieldSchema = sample.getValueSchema() != null ? sample.getValueSchema().field(column).schema() : null;
            ddl.append("ALTER TABLE ").append(tableName).append(" ADD COLUMN ")
               .append(column).append(" ").append(getColumnType(fieldSchema)).append(";");
        }
        return ddl.toString();
    }

    @Override
    public String getColumnType(Schema schema) {
        if (schema == null) {
            return "VARCHAR(1024)";
        }
        switch (schema.type()) {
            case INT8:
            case INT16:
            case INT32:
                return "INTEGER";
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
            default:
                return "VARCHAR(1024)";
        }
    }
}
