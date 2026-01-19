# IBM InfoSphere Data Replication (IIDR) CDC Sink Connector

A Kafka Connect Sink Connector for processing IIDR CDC events based on the IBM Journal Entry Type codes (A_ENTTYP).

## Overview

This connector consumes CDC events from Kafka topics with IIDR-specific headers and writes them to a target JDBC database. It handles:

- **A_ENTTYP Header Mapping**: Maps IBM Journal Entry Type codes to database operations
- **Timezone Conversion**: Converts A_TIMSTAMP to ISO8601 with configurable timezone
- **Corrupt Event Routing**: Invalid events are routed to a dedicated error table

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

| Target Operation | A_ENTTYP Codes | Description |
|------------------|----------------|-------------|
| INSERT | PT, RR, PX, UR | New record insertion |
| UPDATE | UP, FI, FP | Record modification |
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
Events will be moved to `streaming_corrupt_events` if:
- `A_ENTTYP` is missing or contains an unrecognized code.
- `TableName` does not match the configured mapping.
- A `DELETE` event is received without a Kafka Key.
- An `INSERT`/`UPDATE` event is received with a null Value.

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
| `table.name.format` | Target table name format. Use `${TableName}` for header value, `${topic}` for topic name | No | `${TableName}` |
| `corrupt.events.table` | Table name for corrupt/invalid events | No | `streaming_corrupt_events` |

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

## Example Configuration

```json
{
    "name": "iidr_cdc_sink_connector",
    "config": {
        "connector.class": "com.example.kafka.connect.iidr.IidrCdcSinkConnector",
        "tasks.max": "1",
        "topics": "iir.CDC.TRANSACTIONS",

        "connection.url": "jdbc:mariadb://localhost:3306/target_db",
        "connection.user": "root",
        "connection.password": "password",

        "table.name.format": "${TableName}",
        "corrupt.events.table": "streaming_corrupt_events",
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

## Corrupt Events Table Schema

The table schema:

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
cd deployment/kafka-connect/docker
./build.sh
```

## Deployment

Register the connector via Kafka Connect REST API:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @hack/sink-jdbc/iidr_cdc_sink.json
```

Check connector status:

```bash
curl http://localhost:8083/connectors/iidr_cdc_sink_connector/status
```

## Compatibility

- **Java**: 11 (Debezium 2.x) or 17 (Debezium 3.x)
- **Kafka Connect**: Confluent Platform 7.6.1+ or 7.8.0+
- **Target Databases**: MySQL, MariaDB, PostgreSQL, and other JDBC-compatible databases

## References
- [IBM Docs: IIDR Journal Codes](https://www.ibm.com/docs/en/idr/11.4?topic=tables-journal-control-field-header-format)
- [IBM Docs: Adding Headers to Kafka Records](https://www.ibm.com/support/pages/node/6252611)
