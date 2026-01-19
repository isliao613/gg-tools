package com.example.kafka.connect.iidr;

import com.example.kafka.connect.iidr.dialect.Dialect;
import com.example.kafka.connect.iidr.dialect.DialectFactory;
import com.example.kafka.connect.iidr.operation.CdcOperation;
import com.example.kafka.connect.iidr.operation.EntryTypeMapper;
import com.example.kafka.connect.iidr.util.HeaderExtractor;
import com.example.kafka.connect.iidr.util.TimestampConverter;
import com.example.kafka.connect.iidr.writer.CorruptEventWriter;
import com.example.kafka.connect.iidr.writer.CorruptEventWriter.CorruptRecord;
import com.example.kafka.connect.iidr.writer.JdbcWriter;
import com.example.kafka.connect.iidr.writer.JdbcWriter.ProcessedRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Kafka Connect Sink Task for processing IIDR CDC events.
 *
 * This task reads CDC events, maps A_ENTTYP headers to operations,
 * converts timestamps, and writes to the target JDBC database.
 * Invalid events are routed to the corrupt events table.
 */
public class IidrCdcSinkTask extends SinkTask {

    private static final Logger log = Logger.getLogger(IidrCdcSinkTask.class.getName());

    private IidrCdcSinkConfig config;
    private Connection connection;
    private JdbcWriter jdbcWriter;
    private CorruptEventWriter corruptEventWriter;
    private TimestampConverter timestampConverter;

    @Override
    public String version() {
        return IidrCdcSinkConnector.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting IidrCdcSinkTask");

        this.config = new IidrCdcSinkConfig(props);
        this.timestampConverter = new TimestampConverter(config.getDefaultTimezone());

        // Initialize JDBC connection
        try {
            this.connection = DriverManager.getConnection(
                    config.getConnectionUrl(),
                    config.getConnectionUser(),
                    config.getConnectionPassword()
            );
            connection.setAutoCommit(false);

            Dialect dialect = DialectFactory.create(connection);
            this.jdbcWriter = new JdbcWriter(connection, config, dialect);
            this.corruptEventWriter = new CorruptEventWriter(
                    connection,
                    config.getCorruptEventsTable(),
                    config.isAutoCreate()
            );

            log.info("IidrCdcSinkTask started successfully");

        } catch (SQLException e) {
            throw new RuntimeException("Failed to establish JDBC connection", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        log.fine("Processing " + records.size() + " records");

        // Group records by table and validity
        Map<String, List<ProcessedRecord>> validRecordsByTable = new HashMap<>();
        List<CorruptRecord> corruptRecords = new ArrayList<>();

        for (SinkRecord record : records) {
            try {
                ProcessingResult result = processRecord(record);

                if (result.isCorrupt()) {
                    corruptRecords.add(new CorruptRecord(record, result.getCorruptReason()));
                } else {
                    ProcessedRecord processed = result.getProcessedRecord();
                    validRecordsByTable
                            .computeIfAbsent(processed.getTargetTable(), k -> new ArrayList<>())
                            .add(processed);
                }
            } catch (Exception e) {
                log.log(Level.SEVERE, "Unexpected error processing record: " + e.getMessage(), e);
                corruptRecords.add(new CorruptRecord(record, "Processing error: " + e.getMessage()));
            }
        }

        // Write valid records by table
        try {
            for (Map.Entry<String, List<ProcessedRecord>> entry : validRecordsByTable.entrySet()) {
                jdbcWriter.write(entry.getKey(), entry.getValue());
            }

            // Write corrupt records
            if (!corruptRecords.isEmpty()) {
                corruptEventWriter.write(corruptRecords);
            }

            // Commit transaction
            connection.commit();

        } catch (SQLException e) {
            log.log(Level.SEVERE, "Failed to write records to database", e);
            try {
                connection.rollback();
            } catch (SQLException rollbackEx) {
                log.log(Level.SEVERE, "Failed to rollback transaction", rollbackEx);
            }
            throw new RuntimeException("Failed to write records", e);
        }
    }

    /**
     * Process a single SinkRecord into a ProcessingResult.
     * Validates headers, maps operation, and extracts data.
     */
    private ProcessingResult processRecord(SinkRecord record) {
        // 1. Validate required headers
        String headerError = HeaderExtractor.validateRequiredHeaders(record);
        if (headerError != null) {
            return ProcessingResult.corrupt(headerError);
        }

        // 2. Extract headers
        String tableName = HeaderExtractor.extractTableName(record);
        String entryType = HeaderExtractor.extractEntryType(record);
        String timestamp = HeaderExtractor.extractTimestamp(record);

        // 3. Map entry type to operation
        CdcOperation operation = EntryTypeMapper.mapEntryType(entryType);
        if (operation == null) {
            return ProcessingResult.corrupt("Unrecognized A_ENTTYP code: " + entryType);
        }

        // 4. Validate operation-specific requirements
        if (operation == CdcOperation.DELETE) {
            if (record.key() == null) {
                return ProcessingResult.corrupt("DELETE operation requires a Kafka key");
            }
        } else {
            // INSERT, UPDATE, UPSERT require a value
            if (record.value() == null) {
                return ProcessingResult.corrupt(operation + " operation requires a non-null value");
            }
        }

        // 5. Convert timestamp if present
        String isoTimestamp = null;
        if (timestamp != null) {
            isoTimestamp = timestampConverter.convertToIso8601(timestamp);
        }

        // 6. Build target table name
        String targetTable = resolveTargetTable(tableName, record.topic());

        ProcessedRecord processed = new ProcessedRecord(
                targetTable,
                operation,
                record.key(),
                record.value(),
                record.keySchema(),
                record.valueSchema(),
                isoTimestamp
        );

        return ProcessingResult.success(processed);
    }

    /**
     * Resolve target table name from format string.
     */
    private String resolveTargetTable(String tableName, String topic) {
        String format = config.getTableNameFormat();
        return format
                .replace("${TableName}", tableName != null ? tableName : "")
                .replace("${topic}", topic != null ? topic : "");
    }

    @Override
    public void stop() {
        log.info("Stopping IidrCdcSinkTask");

        try {
            if (jdbcWriter != null) {
                jdbcWriter.close();
            }
            if (corruptEventWriter != null) {
                corruptEventWriter.close();
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            log.log(Level.SEVERE, "Error closing resources", e);
        }
    }

    /**
     * Result of processing a SinkRecord.
     */
    private static class ProcessingResult {
        private final ProcessedRecord processedRecord;
        private final String corruptReason;

        private ProcessingResult(ProcessedRecord processedRecord, String corruptReason) {
            this.processedRecord = processedRecord;
            this.corruptReason = corruptReason;
        }

        static ProcessingResult success(ProcessedRecord record) {
            return new ProcessingResult(record, null);
        }

        static ProcessingResult corrupt(String reason) {
            return new ProcessingResult(null, reason);
        }

        boolean isCorrupt() {
            return corruptReason != null;
        }

        String getCorruptReason() {
            return corruptReason;
        }

        ProcessedRecord getProcessedRecord() {
            return processedRecord;
        }
    }
}
