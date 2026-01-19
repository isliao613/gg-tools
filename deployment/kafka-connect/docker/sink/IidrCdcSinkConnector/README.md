# IBM InfoSphere Data Replication (IIDR) CDC Sink Connector

A Kafka Connect Sink Connector for processing IIDR CDC events based on the IBM Journal Entry Type codes (A_ENTTYP).

## Overview

This connector consumes CDC events from Kafka topics with IIDR-specific headers and writes them to a target JDBC database. It handles:

- **A_ENTTYP Header Mapping**: Maps IBM Journal Entry Type codes to database operations
- **Idempotent Replay**: All INSERT and UPDATE operations use UPSERT for safe replay
- **Timezone Conversion**: Converts A_TIMSTAMP to ISO8601 with configurable timezone
- **Configurable Error Handling**: Choose to fail, log, or silently skip corrupt events
- **Optional Corrupt Event Routing**: Invalid events can be routed to a dedicated error table

## Event Structure Overview
Each Kafka event consists of three primary components. All components are delivered in JSON format or as Kafka Metadata Headers.

| Component | Format | Requirement | Description |
|-----------|--------|-------------|-----------------------------------|
| Key       | JSON   | Required for DELETE | Contains the unique identifier/primary key columns. |
| Value     | JSON   | Required for INSERT/UPDATE | Contains the full data payload (Full Row Image). |
| Headers   | Metadata | Required for ALL | Contains CDC metadata (Journal Control Fields). |

## Component Details
### A. Kafka Key
- **Purpose**: Identifies the specific row being modified.
- **Constraint**: Must be a JSON object containing the primary key column(s).

### B. Kafka Value
- **Purpose**: Contains the actual data content of the record.
- **Constraint**: For UPDATE events, the full record data must be provided (not just changed columns). Both INSERT and UPDATE trigger a `FULL_UPSERT` operation in the target.

### C. Kafka Headers (IIDR Journal Fields)
The following headers must be set via the Kafka Client (typically via IIDR KCOP configuration):
| Header Key | Description | Requirements |
|---|---|---|
| TableName | Source table name | Must match sourceTableName in jobSettings.yaml (Case Sensitive). |
| A_ENTTYP | IIDR Journal Entry Code | Determines the operation type (See Section 3). |
| A_TIMSTAMP | Source change timestamp | Format: yyyy-MM-dd HH:mm:ss.SSSSSSSSSSSS. |

## A_ENTTYP Operation Mapping

All INSERT and UPDATE codes are mapped to UPSERT for idempotent replay:

| Target Operation | A_ENTTYP Codes | Description |
|------------------|----------------|-------------|
| UPSERT | PT, RR, PX | New record insertion (mapped to UPSERT) |
| UPSERT | UP, FI, FP | Record modification (mapped to UPSERT) |
| UPSERT | UR | Appears in both INSERT and UPDATE |
| DELETE | DL, DR | Record deletion |

## Event Examples
### Example A: INSERT / UPDATE (Full Upsert)
Note: Both use the same structure. A_ENTTYP distinguishes the intent. Value must be a full data set.
```json
{
  "key": {
    "TRANSACTION_ID": "T1001"
  },
  "value": {
    "TRANSACTION_ID": "T1001",
    "USER_NAME": "John Doe",
    "TRANSACTION_DATE": "2026-01-15T14:00:00.111222",
    "TRANSACTION_AMOUNT": 150.00,
    "STATUS": "Pending"
  },
  "headers": {
    "TableName": "DB2_TRANSACTIONS",
    "A_ENTTYP": "PT",
    "A_TIMSTAMP": "2026-01-15 11:17:14.000000000000"
  }
}
```

### Example B: DELETE
Note: Value is null; Key and Headers are mandatory for identification.
```json
{
  "key": {
    "TRANSACTION_ID": "T1001"
  },
  "value": null,
  "headers": {
    "TableName": "DB2_TRANSACTIONS",
    "A_ENTTYP": "DL",
    "A_TIMSTAMP": "2026-01-15 11:17:14.000000000000"
  }
}
```

## Timezone Handling
The A_TIMSTAMP field is not ISO8601 compliant. The ingestion job interprets this string based on the defaultTransformTimeZone defined in tableSettings.yaml.
Example: If Config TZ is Asia/Taipei (+08:00), then 2026-01-15 11:17:14.000000 is processed as 2026-01-15T11:17:14.000000+08:00.

## Error Handling

The connector provides flexible error handling via the `errors.tolerance` configuration:

| Mode | Behavior |
|------|----------|
| `none` | Fail the task immediately when encountering a corrupt event |
| `log` | Log a warning and skip the corrupt event (default) |
| `all` | Silently skip corrupt events |

Events are considered corrupt if:
- `A_ENTTYP` is missing or contains an unrecognized code
- `TableName` header is missing
- A `DELETE` event is received without a Kafka Key
- An `INSERT`/`UPDATE` event is received with a null Value

Optionally, corrupt events can also be written to a database table by configuring `corrupt.events.table`.

## Configuration Properties

### JDBC Connection

| Property | Description | Required | Default |
|----------|-------------|----------|---------|
| `connection.url` | JDBC connection URL | Yes | - |
| `connection.user` | JDBC username | Yes | - |
| `connection.password` | JDBC password | Yes | - |

### Table Mapping

| Property | Description | Required | Default |
|----------|-------------|----------|---------|
| `table.name.format` | Target table name. Use `${TableName}` for header value, `${topic}` for topic name, or a static name | No | `${TableName}` |

### Error Handling

| Property | Description | Required | Default |
|----------|-------------|----------|---------|
| `errors.tolerance` | Behavior for corrupt events: `none` (fail), `log` (warn and skip), `all` (silent skip) | No | `log` |
| `corrupt.events.table` | Table name for corrupt/invalid events. Leave empty to disable. | No | (empty/disabled) |

### Timezone

| Property | Description | Required | Default |
|----------|-------------|----------|---------|
| `default.timezone` | Timezone for A_TIMSTAMP interpretation (e.g., `Asia/Taipei`, `+08:00`, `UTC`) | No | `UTC` |

### Primary Key

| Property | Description | Required | Default |
|----------|-------------|----------|---------|
| `pk.mode` | Primary key mode: `record_key`, `record_value`, or `none` | No | `record_key` |
| `pk.fields` | Comma-separated list of primary key field names | No | - |

### DDL

| Property | Description | Required | Default |
|----------|-------------|----------|---------|
| `auto.create` | Automatically create target tables if they don't exist | No | `false` |
| `auto.evolve` | Automatically add columns to existing tables | No | `false` |

### Performance

| Property | Description | Required | Default |
|----------|-------------|----------|---------|
| `batch.size` | Maximum number of records in a single JDBC batch | No | `3000` |
| `max.retries` | Maximum number of retries on transient errors | No | `10` |
| `retry.backoff.ms` | Backoff time in milliseconds between retries | No | `3000` |

## Example Configurations

### Single Connector (Multiple Tables)

A single connector handling multiple tables via the `${TableName}` header:

```json
{
    "name": "iidr_cdc_sink_connector",
    "config": {
        "connector.class": "com.example.kafka.connect.iidr.IidrCdcSinkConnector",
        "tasks.max": "1",
        "topics": "iidr.CDC.ORDERS,iidr.CDC.PRODUCTS",

        "connection.url": "jdbc:mariadb://localhost:3306/target_db",
        "connection.user": "root",
        "connection.password": "password",

        "table.name.format": "IIDR_${TableName}",
        "errors.tolerance": "log",
        "default.timezone": "Asia/Taipei",

        "pk.mode": "record_key",
        "pk.fields": "ID",
        "auto.create": "true",

        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false"
    }
}
```

### Multi-Connector Setup (One Connector Per Table)

For better isolation and independent scaling, use separate connectors for each table:

**Orders Connector:**
```json
{
    "name": "iidr_cdc_sink_orders",
    "config": {
        "connector.class": "com.example.kafka.connect.iidr.IidrCdcSinkConnector",
        "tasks.max": "1",
        "topics": "iidr.CDC.ORDERS",

        "connection.url": "jdbc:mariadb://localhost:3306/target_db",
        "connection.user": "root",
        "connection.password": "password",

        "table.name.format": "IIDR_ORDERS",
        "errors.tolerance": "log",
        "default.timezone": "Asia/Taipei",

        "pk.mode": "record_key",
        "pk.fields": "ORDER_ID",
        "auto.create": "true",

        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false"
    }
}
```

**Products Connector:**
```json
{
    "name": "iidr_cdc_sink_products",
    "config": {
        "connector.class": "com.example.kafka.connect.iidr.IidrCdcSinkConnector",
        "tasks.max": "1",
        "topics": "iidr.CDC.PRODUCTS",

        "connection.url": "jdbc:mariadb://localhost:3306/target_db",
        "connection.user": "root",
        "connection.password": "password",

        "table.name.format": "IIDR_PRODUCTS",
        "errors.tolerance": "log",
        "default.timezone": "Asia/Taipei",

        "pk.mode": "record_key",
        "pk.fields": "PRODUCT_ID",
        "auto.create": "true",

        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false"
    }
}
```

### With Corrupt Events Table

Enable corrupt event logging to a database table:

```json
{
    "name": "iidr_cdc_sink_with_error_table",
    "config": {
        "connector.class": "com.example.kafka.connect.iidr.IidrCdcSinkConnector",
        "tasks.max": "1",
        "topics": "iidr.CDC.TRANSACTIONS",

        "connection.url": "jdbc:mariadb://localhost:3306/target_db",
        "connection.user": "root",
        "connection.password": "password",

        "table.name.format": "IIDR_TRANSACTIONS",
        "errors.tolerance": "log",
        "corrupt.events.table": "streaming_corrupt_events",
        "default.timezone": "Asia/Taipei",

        "pk.mode": "record_key",
        "pk.fields": "TRANSACTION_ID",
        "auto.create": "true",

        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false"
    }
}
```

## Corrupt Events Table Schema

When `corrupt.events.table` is configured, corrupt events are written to a table with this schema:

```sql
CREATE TABLE streaming_corrupt_events (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset BIGINT NOT NULL,
    record_key TEXT,
    record_value LONGTEXT,
    headers TEXT,
    error_reason VARCHAR(1000) NOT NULL,
    table_name VARCHAR(255),
    entry_type VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_topic_partition_offset (topic, kafka_partition, kafka_offset),
    INDEX idx_table_name (table_name),
    INDEX idx_created_at (created_at)
);
```

## Building

The connector is built as part of the Kafka Connect Docker image:

```bash
make build-v2  # For Debezium 2.x
make build-v3  # For Debezium 3.x
```

## Deployment

Register the connector via Kafka Connect REST API:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @hack/sink-jdbc/iidr_cdc_sink-test.json
```

Check connector status:

```bash
curl http://localhost:8083/connectors/iidr_cdc_sink_test/status
```

## Testing

Run the E2E test suite:

```bash
# Single table test with Debezium 2.x
make iidr-all-v2

# Single table test with Debezium 3.x
make iidr-all-v3

# Test with both versions
make iidr-all-dual
```

## Compatibility

- **Java**: 11 (Debezium 2.x) or 17 (Debezium 3.x)
- **Kafka Connect**: Confluent Platform 7.6.1+ or 7.8.0+
- **Target Databases**: MySQL, MariaDB, PostgreSQL, and other JDBC-compatible databases

## References
- [IBM Docs: IIDR Journal Codes](https://www.ibm.com/docs/en/idr/11.4?topic=tables-journal-control-field-header-format)
- [IBM Docs: Adding Headers to Kafka Records](https://www.ibm.com/support/pages/node/6252611)
