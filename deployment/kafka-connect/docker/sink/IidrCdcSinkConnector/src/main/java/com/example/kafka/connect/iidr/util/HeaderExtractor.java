package com.example.kafka.connect.iidr.util;

import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;

import java.nio.charset.StandardCharsets;

/**
 * Extracts required headers from Kafka records for IIDR CDC processing.
 *
 * Required headers:
 * - TableName: Source table name
 * - A_ENTTYP: IBM Journal Entry Type code
 * - A_TIMSTAMP: Source change timestamp
 */
public class HeaderExtractor {

    public static final String HEADER_TABLE_NAME = "TableName";
    public static final String HEADER_ENTRY_TYPE = "A_ENTTYP";
    public static final String HEADER_TIMESTAMP = "A_TIMSTAMP";

    /**
     * Extract a header value as String.
     */
    public static String extractString(SinkRecord record, String headerKey) {
        Headers headers = record.headers();
        if (headers == null) {
            return null;
        }

        Header header = headers.lastWithName(headerKey);
        if (header == null) {
            return null;
        }

        Object value = header.value();
        if (value == null) {
            return null;
        }

        // Handle byte arrays (common for headers)
        if (value instanceof byte[]) {
            return new String((byte[]) value, StandardCharsets.UTF_8);
        }

        return value.toString();
    }

    /**
     * Extract the TableName header.
     */
    public static String extractTableName(SinkRecord record) {
        return extractString(record, HEADER_TABLE_NAME);
    }

    /**
     * Extract the A_ENTTYP header.
     */
    public static String extractEntryType(SinkRecord record) {
        return extractString(record, HEADER_ENTRY_TYPE);
    }

    /**
     * Extract the A_TIMSTAMP header.
     */
    public static String extractTimestamp(SinkRecord record) {
        return extractString(record, HEADER_TIMESTAMP);
    }

    /**
     * Validate that all required headers are present.
     *
     * @return null if valid, otherwise a description of what's missing
     */
    public static String validateRequiredHeaders(SinkRecord record) {
        StringBuilder errors = new StringBuilder();

        if (extractTableName(record) == null) {
            errors.append("Missing header: TableName. ");
        }
        if (extractEntryType(record) == null) {
            errors.append("Missing header: A_ENTTYP. ");
        }
        // A_TIMSTAMP is recommended but not strictly required

        return errors.length() > 0 ? errors.toString().trim() : null;
    }
}
