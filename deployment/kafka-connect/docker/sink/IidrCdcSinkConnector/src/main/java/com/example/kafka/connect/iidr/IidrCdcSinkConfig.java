package com.example.kafka.connect.iidr;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Configuration for the IIDR CDC Sink Connector.
 */
public class IidrCdcSinkConfig extends AbstractConfig {

    // JDBC Connection Settings
    public static final String CONNECTION_URL_CONFIG = "connection.url";
    private static final String CONNECTION_URL_DOC = "JDBC connection URL";

    public static final String CONNECTION_USER_CONFIG = "connection.user";
    private static final String CONNECTION_USER_DOC = "JDBC connection username";

    public static final String CONNECTION_PASSWORD_CONFIG = "connection.password";
    private static final String CONNECTION_PASSWORD_DOC = "JDBC connection password";

    // Table Mapping Settings
    public static final String TABLE_NAME_FORMAT_CONFIG = "table.name.format";
    private static final String TABLE_NAME_FORMAT_DOC = "Format string for target table name. " +
            "Use ${topic} for topic name, ${TableName} for header value";
    public static final String TABLE_NAME_FORMAT_DEFAULT = "${TableName}";

    // Corrupt Events Table
    public static final String CORRUPT_EVENTS_TABLE_CONFIG = "corrupt.events.table";
    private static final String CORRUPT_EVENTS_TABLE_DOC = "Table name for corrupt/invalid events";
    public static final String CORRUPT_EVENTS_TABLE_DEFAULT = "streaming_corrupt_events";

    // Timezone Settings
    public static final String DEFAULT_TIMEZONE_CONFIG = "default.timezone";
    private static final String DEFAULT_TIMEZONE_DOC = "Default timezone for A_TIMSTAMP interpretation " +
            "(e.g., Asia/Taipei, +08:00, UTC)";
    public static final String DEFAULT_TIMEZONE_DEFAULT = "UTC";

    // Primary Key Settings
    public static final String PK_MODE_CONFIG = "pk.mode";
    private static final String PK_MODE_DOC = "Primary key mode: 'record_key' (from Kafka key), " +
            "'record_value' (from value fields), 'none'";
    public static final String PK_MODE_DEFAULT = "record_key";

    public static final String PK_FIELDS_CONFIG = "pk.fields";
    private static final String PK_FIELDS_DOC = "Comma-separated list of primary key field names";
    public static final String PK_FIELDS_DEFAULT = "";

    // DDL Settings
    public static final String AUTO_CREATE_CONFIG = "auto.create";
    private static final String AUTO_CREATE_DOC = "Automatically create target tables if they don't exist";
    public static final boolean AUTO_CREATE_DEFAULT = false;

    public static final String AUTO_EVOLVE_CONFIG = "auto.evolve";
    private static final String AUTO_EVOLVE_DOC = "Automatically add columns to existing tables";
    public static final boolean AUTO_EVOLVE_DEFAULT = false;

    // Batch Settings
    public static final String BATCH_SIZE_CONFIG = "batch.size";
    private static final String BATCH_SIZE_DOC = "Maximum number of records in a single JDBC batch";
    public static final int BATCH_SIZE_DEFAULT = 3000;

    // Error Handling
    public static final String MAX_RETRIES_CONFIG = "max.retries";
    private static final String MAX_RETRIES_DOC = "Maximum number of retries on transient errors";
    public static final int MAX_RETRIES_DEFAULT = 10;

    public static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
    private static final String RETRY_BACKOFF_MS_DOC = "Backoff time in milliseconds between retries";
    public static final int RETRY_BACKOFF_MS_DEFAULT = 3000;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            // JDBC Connection
            .define(CONNECTION_URL_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    Importance.HIGH, CONNECTION_URL_DOC)
            .define(CONNECTION_USER_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    Importance.HIGH, CONNECTION_USER_DOC)
            .define(CONNECTION_PASSWORD_CONFIG, Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE,
                    Importance.HIGH, CONNECTION_PASSWORD_DOC)
            // Table Mapping
            .define(TABLE_NAME_FORMAT_CONFIG, Type.STRING, TABLE_NAME_FORMAT_DEFAULT,
                    Importance.MEDIUM, TABLE_NAME_FORMAT_DOC)
            .define(CORRUPT_EVENTS_TABLE_CONFIG, Type.STRING, CORRUPT_EVENTS_TABLE_DEFAULT,
                    Importance.MEDIUM, CORRUPT_EVENTS_TABLE_DOC)
            // Timezone
            .define(DEFAULT_TIMEZONE_CONFIG, Type.STRING, DEFAULT_TIMEZONE_DEFAULT,
                    Importance.MEDIUM, DEFAULT_TIMEZONE_DOC)
            // Primary Key
            .define(PK_MODE_CONFIG, Type.STRING, PK_MODE_DEFAULT,
                    Importance.HIGH, PK_MODE_DOC)
            .define(PK_FIELDS_CONFIG, Type.STRING, PK_FIELDS_DEFAULT,
                    Importance.HIGH, PK_FIELDS_DOC)
            // DDL
            .define(AUTO_CREATE_CONFIG, Type.BOOLEAN, AUTO_CREATE_DEFAULT,
                    Importance.MEDIUM, AUTO_CREATE_DOC)
            .define(AUTO_EVOLVE_CONFIG, Type.BOOLEAN, AUTO_EVOLVE_DEFAULT,
                    Importance.MEDIUM, AUTO_EVOLVE_DOC)
            // Batch
            .define(BATCH_SIZE_CONFIG, Type.INT, BATCH_SIZE_DEFAULT,
                    Importance.LOW, BATCH_SIZE_DOC)
            // Error Handling
            .define(MAX_RETRIES_CONFIG, Type.INT, MAX_RETRIES_DEFAULT,
                    Importance.MEDIUM, MAX_RETRIES_DOC)
            .define(RETRY_BACKOFF_MS_CONFIG, Type.INT, RETRY_BACKOFF_MS_DEFAULT,
                    Importance.LOW, RETRY_BACKOFF_MS_DOC);

    public IidrCdcSinkConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
    }

    public String getConnectionUrl() {
        return getString(CONNECTION_URL_CONFIG);
    }

    public String getConnectionUser() {
        return getString(CONNECTION_USER_CONFIG);
    }

    public String getConnectionPassword() {
        return getPassword(CONNECTION_PASSWORD_CONFIG).value();
    }

    public String getTableNameFormat() {
        return getString(TABLE_NAME_FORMAT_CONFIG);
    }

    public String getCorruptEventsTable() {
        return getString(CORRUPT_EVENTS_TABLE_CONFIG);
    }

    public String getDefaultTimezone() {
        return getString(DEFAULT_TIMEZONE_CONFIG);
    }

    public String getPkMode() {
        return getString(PK_MODE_CONFIG);
    }

    public List<String> getPkFields() {
        String fields = getString(PK_FIELDS_CONFIG);
        if (fields == null || fields.trim().isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.asList(fields.split(","));
    }

    public boolean isAutoCreate() {
        return getBoolean(AUTO_CREATE_CONFIG);
    }

    public boolean isAutoEvolve() {
        return getBoolean(AUTO_EVOLVE_CONFIG);
    }

    public int getBatchSize() {
        return getInt(BATCH_SIZE_CONFIG);
    }

    public int getMaxRetries() {
        return getInt(MAX_RETRIES_CONFIG);
    }

    public int getRetryBackoffMs() {
        return getInt(RETRY_BACKOFF_MS_CONFIG);
    }
}
