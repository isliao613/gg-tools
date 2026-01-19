package com.example.kafka.connect.iidr.writer;

import com.example.kafka.connect.iidr.util.HeaderExtractor;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Writes corrupt/invalid events to the streaming_corrupt_events table.
 *
 * Table schema:
 * - id: BIGINT AUTO_INCREMENT
 * - topic: VARCHAR(255) - Source Kafka topic
 * - kafka_partition: INT - Kafka partition
 * - kafka_offset: BIGINT - Kafka offset
 * - record_key: TEXT - JSON serialized key
 * - record_value: LONGTEXT - JSON serialized value
 * - headers: TEXT - JSON serialized headers
 * - error_reason: VARCHAR(1000) - Why the event was marked corrupt
 * - table_name: VARCHAR(255) - TableName header value if present
 * - entry_type: VARCHAR(10) - A_ENTTYP header value if present
 * - created_at: TIMESTAMP - When the record was inserted
 */
public class CorruptEventWriter implements AutoCloseable {

    private static final Logger log = Logger.getLogger(CorruptEventWriter.class.getName());

    private final Connection connection;
    private final String tableName;
    private PreparedStatement insertStatement;

    private static final String INSERT_SQL =
            "INSERT INTO %s (topic, kafka_partition, kafka_offset, record_key, record_value, headers, " +
                    "error_reason, table_name, entry_type, created_at) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS %s (" +
                    "  id BIGINT AUTO_INCREMENT PRIMARY KEY," +
                    "  topic VARCHAR(255) NOT NULL," +
                    "  kafka_partition INT NOT NULL," +
                    "  kafka_offset BIGINT NOT NULL," +
                    "  record_key TEXT," +
                    "  record_value LONGTEXT," +
                    "  headers TEXT," +
                    "  error_reason VARCHAR(1000) NOT NULL," +
                    "  table_name VARCHAR(255)," +
                    "  entry_type VARCHAR(10)," +
                    "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
                    "  INDEX idx_topic_partition_offset (topic, kafka_partition, kafka_offset)," +
                    "  INDEX idx_table_name (table_name)," +
                    "  INDEX idx_created_at (created_at)" +
                    ")";

    public CorruptEventWriter(Connection connection, String tableName, boolean autoCreate)
            throws SQLException {
        this.connection = connection;
        this.tableName = tableName;

        if (autoCreate) {
            ensureTableExists();
        }

        this.insertStatement = connection.prepareStatement(
                String.format(INSERT_SQL, tableName)
        );
    }

    private void ensureTableExists() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(String.format(CREATE_TABLE_SQL, tableName));
            log.info("Ensured corrupt events table exists: " + tableName);
        }
    }

    /**
     * Write corrupt records to the error table.
     */
    public void write(List<CorruptRecord> records) throws SQLException {
        if (records.isEmpty()) {
            return;
        }

        log.info("Writing " + records.size() + " corrupt records to " + tableName);

        for (CorruptRecord corrupt : records) {
            SinkRecord record = corrupt.getRecord();

            try {
                insertStatement.setString(1, record.topic());
                insertStatement.setInt(2, record.kafkaPartition());
                insertStatement.setLong(3, record.kafkaOffset());
                insertStatement.setString(4, serializeToJson(record.key()));
                insertStatement.setString(5, serializeToJson(record.value()));
                insertStatement.setString(6, serializeHeaders(record));
                insertStatement.setString(7, truncate(corrupt.getReason(), 1000));
                insertStatement.setString(8, HeaderExtractor.extractTableName(record));
                insertStatement.setString(9, HeaderExtractor.extractEntryType(record));
                insertStatement.setTimestamp(10, Timestamp.from(Instant.now()));

                insertStatement.addBatch();

            } catch (Exception e) {
                log.severe("Failed to prepare corrupt record for insertion: " + e.getMessage());
            }
        }

        insertStatement.executeBatch();
    }

    private String serializeToJson(Object obj) {
        if (obj == null) {
            return null;
        }
        // Simple JSON serialization for basic types
        if (obj instanceof Map) {
            return mapToJson((Map<?, ?>) obj);
        }
        return obj.toString();
    }

    @SuppressWarnings("unchecked")
    private String mapToJson(Map<?, ?> map) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!first) {
                sb.append(",");
            }
            first = false;
            sb.append("\"").append(entry.getKey()).append("\":");
            Object value = entry.getValue();
            if (value == null) {
                sb.append("null");
            } else if (value instanceof String) {
                sb.append("\"").append(escapeJson((String) value)).append("\"");
            } else if (value instanceof Number || value instanceof Boolean) {
                sb.append(value);
            } else {
                sb.append("\"").append(escapeJson(value.toString())).append("\"");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    private String escapeJson(String str) {
        return str.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    private String serializeHeaders(SinkRecord record) {
        if (record.headers() == null || record.headers().isEmpty()) {
            return "{}";
        }

        Map<String, String> headerMap = new HashMap<>();
        record.headers().forEach(h -> {
            String value = h.value() != null ? h.value().toString() : null;
            headerMap.put(h.key(), value);
        });
        return mapToJson(headerMap);
    }

    private String truncate(String str, int maxLength) {
        if (str == null || str.length() <= maxLength) {
            return str;
        }
        return str.substring(0, maxLength - 3) + "...";
    }

    @Override
    public void close() throws SQLException {
        if (insertStatement != null) {
            insertStatement.close();
        }
    }

    /**
     * Represents a corrupt record with its reason.
     */
    public static class CorruptRecord {
        private final SinkRecord record;
        private final String reason;

        public CorruptRecord(SinkRecord record, String reason) {
            this.record = record;
            this.reason = reason;
        }

        public SinkRecord getRecord() {
            return record;
        }

        public String getReason() {
            return reason;
        }
    }
}
