package com.example.kafka.connect.iidr.writer;

import com.example.kafka.connect.iidr.IidrCdcSinkConfig;
import com.example.kafka.connect.iidr.dialect.Dialect;
import com.example.kafka.connect.iidr.operation.CdcOperation;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

/**
 * Writes CDC records to the target database using JDBC.
 */
public class JdbcWriter implements AutoCloseable {

    private static final Logger log = Logger.getLogger(JdbcWriter.class.getName());

    private final Connection connection;
    private final IidrCdcSinkConfig config;
    private final Dialect dialect;
    private final Map<String, PreparedStatement> statementCache;

    public JdbcWriter(Connection connection, IidrCdcSinkConfig config, Dialect dialect) {
        this.connection = connection;
        this.config = config;
        this.dialect = dialect;
        this.statementCache = new HashMap<>();
    }

    /**
     * Write a batch of processed records to the target table.
     */
    public void write(String tableName, List<ProcessedRecord> records) throws SQLException {
        if (records.isEmpty()) {
            return;
        }

        log.fine("Writing " + records.size() + " records to table " + tableName);

        // Ensure table exists if auto.create is enabled
        if (config.isAutoCreate()) {
            ensureTableExists(tableName, records.get(0));
        }

        // Evolve table schema if auto.evolve is enabled
        if (config.isAutoEvolve()) {
            evolveTable(tableName, records.get(0));
        }

        // Group by operation type
        List<ProcessedRecord> inserts = new ArrayList<>();
        List<ProcessedRecord> updates = new ArrayList<>();
        List<ProcessedRecord> upserts = new ArrayList<>();
        List<ProcessedRecord> deletes = new ArrayList<>();

        for (ProcessedRecord record : records) {
            switch (record.getOperation()) {
                case INSERT:
                    inserts.add(record);
                    break;
                case UPDATE:
                    updates.add(record);
                    break;
                case UPSERT:
                    upserts.add(record);
                    break;
                case DELETE:
                    deletes.add(record);
                    break;
            }
        }

        // Process each operation type
        if (!inserts.isEmpty()) {
            executeInserts(tableName, inserts);
        }
        if (!updates.isEmpty()) {
            executeUpdates(tableName, updates);
        }
        if (!upserts.isEmpty()) {
            executeUpserts(tableName, upserts);
        }
        if (!deletes.isEmpty()) {
            executeDeletes(tableName, deletes);
        }
    }

    private void executeInserts(String tableName, List<ProcessedRecord> records)
            throws SQLException {
        ProcessedRecord sample = records.get(0);
        List<String> columns = extractColumnNames(sample);

        String sql = dialect.buildInsertSql(tableName, columns);
        log.fine("INSERT SQL: " + sql);

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (ProcessedRecord record : records) {
                setParameters(ps, record, columns);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void executeUpdates(String tableName, List<ProcessedRecord> records)
            throws SQLException {
        ProcessedRecord sample = records.get(0);
        List<String> columns = extractColumnNames(sample);
        List<String> pkColumns = config.getPkFields();

        if (pkColumns.isEmpty()) {
            log.warning("No primary key fields configured, falling back to upsert for UPDATE operations");
            executeUpserts(tableName, records);
            return;
        }

        String sql = dialect.buildUpsertSql(tableName, columns, pkColumns);
        log.fine("UPDATE SQL: " + sql);

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (ProcessedRecord record : records) {
                setUpdateParameters(ps, record, columns, pkColumns);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void executeUpserts(String tableName, List<ProcessedRecord> records)
            throws SQLException {
        ProcessedRecord sample = records.get(0);
        List<String> columns = extractColumnNames(sample);
        List<String> pkColumns = config.getPkFields();

        String sql = dialect.buildUpsertSql(tableName, columns, pkColumns);
        log.fine("UPSERT SQL: " + sql);

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (ProcessedRecord record : records) {
                setParameters(ps, record, columns);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void executeDeletes(String tableName, List<ProcessedRecord> records)
            throws SQLException {
        List<String> pkColumns = config.getPkFields();
        if (pkColumns.isEmpty()) {
            log.warning("No primary key fields configured, cannot execute DELETE operations");
            return;
        }

        String sql = dialect.buildDeleteSql(tableName, pkColumns);
        log.fine("DELETE SQL: " + sql);

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (ProcessedRecord record : records) {
                setDeleteParameters(ps, record, pkColumns);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    // Parameter Setting Methods

    private void setParameters(PreparedStatement ps, ProcessedRecord record,
                               List<String> columns) throws SQLException {
        Map<String, Object> values = extractValueMap(record);
        int idx = 1;
        for (String col : columns) {
            ps.setObject(idx++, values.get(col));
        }
    }

    private void setUpdateParameters(PreparedStatement ps, ProcessedRecord record,
                                     List<String> columns, List<String> pkColumns) throws SQLException {
        Map<String, Object> values = extractValueMap(record);
        Map<String, Object> keyValues = extractKeyMap(record);

        // First set non-PK columns for SET clause
        List<String> nonPkColumns = new ArrayList<>(columns);
        nonPkColumns.removeAll(pkColumns);

        int idx = 1;
        for (String col : nonPkColumns) {
            ps.setObject(idx++, values.get(col));
        }

        // Then set PK columns for WHERE clause
        for (String col : pkColumns) {
            ps.setObject(idx++, keyValues.getOrDefault(col, values.get(col)));
        }
    }

    private void setDeleteParameters(PreparedStatement ps, ProcessedRecord record,
                                     List<String> pkColumns) throws SQLException {
        Map<String, Object> keyValues = extractKeyMap(record);
        Map<String, Object> values = extractValueMap(record);

        int idx = 1;
        for (String col : pkColumns) {
            Object value = keyValues.get(col);
            if (value == null) {
                value = values.get(col);
            }
            ps.setObject(idx++, value);
        }
    }

    // Helper Methods

    private List<String> extractColumnNames(ProcessedRecord record) {
        List<String> columns = new ArrayList<>();

        Object value = record.getValue();
        if (value instanceof Struct) {
            Struct struct = (Struct) value;
            for (Field field : struct.schema().fields()) {
                columns.add(field.name());
            }
        } else if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, ?> map = (Map<String, ?>) value;
            columns.addAll(map.keySet());
        }

        return columns;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> extractValueMap(ProcessedRecord record) {
        Object value = record.getValue();

        if (value instanceof Struct) {
            Struct struct = (Struct) value;
            Map<String, Object> map = new LinkedHashMap<>();
            for (Field field : struct.schema().fields()) {
                map.put(field.name(), struct.get(field));
            }
            return map;
        } else if (value instanceof Map) {
            return new LinkedHashMap<>((Map<String, Object>) value);
        }

        return Collections.emptyMap();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> extractKeyMap(ProcessedRecord record) {
        Object key = record.getKey();

        if (key instanceof Struct) {
            Struct struct = (Struct) key;
            Map<String, Object> map = new LinkedHashMap<>();
            for (Field field : struct.schema().fields()) {
                map.put(field.name(), struct.get(field));
            }
            return map;
        } else if (key instanceof Map) {
            return new LinkedHashMap<>((Map<String, Object>) key);
        }

        return Collections.emptyMap();
    }

    private void ensureTableExists(String tableName, ProcessedRecord sample) throws SQLException {
        DatabaseMetaData meta = connection.getMetaData();
        String normalizedTableName = dialect.normalizeIdentifierForMetadata(tableName);
        try (ResultSet rs = meta.getTables(null, null, normalizedTableName, new String[]{"TABLE"})) {
            if (!rs.next()) {
                log.info("Auto-creating table: " + tableName);
                createTable(tableName, sample);
            }
        }
    }

    private void createTable(String tableName, ProcessedRecord sample) throws SQLException {
        List<String> pkColumns = config.getPkFields();
        String ddl = dialect.buildCreateTableSql(tableName, sample, pkColumns);
        log.info("Creating table with DDL: " + ddl);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(ddl);
        }
    }

    private void evolveTable(String tableName, ProcessedRecord sample) throws SQLException {
        DatabaseMetaData meta = connection.getMetaData();
        String normalizedTableName = dialect.normalizeIdentifierForMetadata(tableName);
        Set<String> existingColumns = new HashSet<>();
        try (ResultSet rs = meta.getColumns(null, null, normalizedTableName, null)) {
            while (rs.next()) {
                existingColumns.add(rs.getString("COLUMN_NAME").toUpperCase());
            }
        }

        List<String> recordColumns = extractColumnNames(sample);
        List<String> missingColumns = new ArrayList<>();
        for (String col : recordColumns) {
            if (!existingColumns.contains(col.toUpperCase())) {
                missingColumns.add(col);
            }
        }

        if (!missingColumns.isEmpty()) {
            log.info("Evolving table " + tableName + " with missing columns: " + missingColumns);
            String ddl = dialect.buildAlterTableSql(tableName, missingColumns, sample);
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(ddl);
            }
        }
    }

    @Override
    public void close() throws SQLException {
        for (PreparedStatement ps : statementCache.values()) {
            ps.close();
        }
        statementCache.clear();
    }

    /**
     * Represents a processed CDC record ready for database operations.
     */
    public static class ProcessedRecord {
        private final String targetTable;
        private final CdcOperation operation;
        private final Object key;
        private final Object value;
        private final Schema keySchema;
        private final Schema valueSchema;
        private final String isoTimestamp;

        public ProcessedRecord(String targetTable, CdcOperation operation, Object key, Object value,
                               Schema keySchema, Schema valueSchema, String isoTimestamp) {
            this.targetTable = targetTable;
            this.operation = operation;
            this.key = key;
            this.value = value;
            this.keySchema = keySchema;
            this.valueSchema = valueSchema;
            this.isoTimestamp = isoTimestamp;
        }

        public String getTargetTable() {
            return targetTable;
        }

        public CdcOperation getOperation() {
            return operation;
        }

        public Object getKey() {
            return key;
        }

        public Object getValue() {
            return value;
        }

        public Schema getKeySchema() {
            return keySchema;
        }

        public Schema getValueSchema() {
            return valueSchema;
        }

        public String getIsoTimestamp() {
            return isoTimestamp;
        }
    }
}
