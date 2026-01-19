# Debezium 2.x vs 3.x Comparison Report

This report compares Debezium 2.x and 3.x configurations and data type handling based on the kafka-dbsync project setup.

## 1. Infrastructure Differences

| Component | Debezium 2.x | Debezium 3.x |
|-----------|-------------|--------------|
| **Java Version** | Java 11 | Java 17 (required) |
| **Kafka Connect Base** | cp-kafka-connect:7.6.1 | cp-kafka-connect:7.8.0 |
| **Kafka Compatibility** | Kafka 3.x | Kafka 4.x |
| **Container Registry** | Docker Hub | Quay.io (official) |

### Dockerfile Comparison

**Debezium 2.x** (`Dockerfile`):
```dockerfile
FROM maven:3.9-eclipse-temurin-11 AS smt-builder
FROM confluentinc/cp-kafka-connect:7.6.1
# Debezium versions: 2.6.1 - 2.7.4
```

**Debezium 3.x** (`Dockerfile.debezium-3.4`):
```dockerfile
FROM maven:3.9-eclipse-temurin-17 AS smt-builder
FROM confluentinc/cp-kafka-connect:7.8.0
# Debezium version: 3.4.0 (unified across all connectors)
```

## 2. Connector Version Mapping

| Connector | Debezium 2.x Version | Debezium 3.x Version |
|-----------|---------------------|---------------------|
| Oracle | 2.6.1.Final | 3.4.0.Final |
| MySQL | 2.6.1.Final | 3.4.0.Final |
| SQL Server | 2.7.4.Final | 3.4.0.Final |
| MongoDB | 2.6.2.Final | 3.4.0.Final |
| JDBC Sink | 2.7.4.Final | 3.4.0.Final |

> Note: Debezium 3.x uses unified versioning across all connectors, simplifying dependency management.

## 3. Connector Configuration Comparison

### Source Connector (Oracle)

**Common Configuration** (identical in both versions):
```json
{
    "connector.class": "io.debezium.connector.oracle.OracleConnector",
    "tasks.max": "1",
    "database.hostname": "dbrep-oracle-oracle-db.dev.svc.cluster.local",
    "database.port": "1521",
    "database.user": "c##dbzcdc",
    "database.password": "dbz",
    "database.dbname": "XE",
    "database.pdb.name": "XEPDB1",
    "table.include.list": "DEMO.SOURCE_ORDERS",
    "log.mining.strategy": "online_catalog",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false"
}
```

**Differences**:

| Setting | 2.x Value | 3.x Value |
|---------|-----------|-----------|
| `name` | `source_cdc_oracle_demo_v2` | `source_cdc_oracle_demo_v3` |
| `topic.prefix` | `cdc_oracle_v2` | `cdc_oracle_v3` |
| `schema.history.internal.kafka.topic` | `schema-changes.oracle.v2` | `schema-changes.oracle.v3` |

### Sink Connector (JDBC to MariaDB)

**Configuration Differences**:

| Setting | 2.x Value | 3.x Value |
|---------|-----------|-----------|
| `name` | `sink_cdc_oracle_to_mariadb_v2` | `sink_cdc_oracle_to_mariadb_v3` |
| `topics` | `cdc_oracle_v2.DEMO.SOURCE_ORDERS` | `cdc_oracle_v3.DEMO.SOURCE_ORDERS` |
| `table.name.format` | `target_orders_v2` | `target_orders_v3` |

## 4. Data Type Mapping (Oracle to MariaDB)

### Source Table Schema (Oracle)

```sql
CREATE TABLE DEMO.SOURCE_ORDERS (
    ID NUMBER(10) NOT NULL PRIMARY KEY,
    ORDER_NO VARCHAR2(50) NOT NULL,
    CUSTOMER_NAME VARCHAR2(100) NOT NULL,
    AMOUNT NUMBER(15,2) NOT NULL,
    STATUS VARCHAR2(20) DEFAULT 'PENDING' NOT NULL,
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    MODIFIED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);
```

### Data Type Conversion (Both Versions)

| Oracle Type | Kafka Connect Schema | MariaDB Target |
|-------------|---------------------|----------------|
| `NUMBER(10)` | `int32` / `int64` | `INT` / `BIGINT` |
| `NUMBER(15,2)` | `org.apache.kafka.connect.data.Decimal` | `DECIMAL(15,2)` |
| `VARCHAR2(n)` | `string` | `VARCHAR(n)` |
| `TIMESTAMP` | `io.debezium.time.Timestamp` (microseconds) | `DATETIME(6)` |

### Key Data Type Changes in Debezium 3.x

1. **Timestamp Precision**: Both versions handle Oracle `TIMESTAMP` with microsecond precision, but 3.x has improved handling for edge cases.

2. **Decimal Handling**: Behavior varies based on `decimal.handling.mode` setting (see below).

3. **LOB Columns**: Debezium 3.x changed LOB column behavior when using column re-selector - LOB columns are now emitted regardless of configuration.

4. **Vector Data Types** (if applicable):
   - 2.x: PostgreSQL-specific `io.debezium.data.SparseVector`
   - 3.x: Generic shared type across PostgreSQL, MySQL, and Oracle

### decimal.handling.mode Options

The `decimal.handling.mode` connector configuration controls how Oracle `NUMBER` types are handled:

| Mode | Description | Use Case |
|------|-------------|----------|
| `precise` | Uses `org.apache.kafka.connect.data.Decimal` (Kafka Connect default) | Maximum precision, but may cause JDBC Sink issues with unbounded NUMBER |
| `double` | Converts all decimals to `float64` | Simple, but loses precision for large numbers |
| `string` | Converts all decimals to string | Safe for all values, requires downstream parsing |

**Recommendation**: Use `decimal.handling.mode=string` for Oracle to avoid `VariableScaleDecimal` errors with the JDBC Sink connector. This is the configuration used in the datatype comparison tests.

**Known Issue**: Both Debezium 2.x and 3.x fail with JDBC Sink when Oracle `NUMBER` (without precision) or `FLOAT` is used without `decimal.handling.mode` configuration:
```
io.debezium.data.VariableScaleDecimal (STRUCT) type doesn't have a mapping to the SQL database column type
```

## 5. Breaking Changes (2.x to 3.x Migration)

### Configuration Removals

| Deprecated (2.x) | Replacement (3.x) |
|-----------------|-------------------|
| `snapshot.mode=schema_only` | `snapshot.mode=no_data` |
| `snapshot.mode=schema_only_recovery` | `snapshot.mode=recovery` |
| Signal `additional-condition` | Signal `additional-conditions` |

### Behavioral Changes

1. **Signal Reprocessing**: Connectors no longer replay signals on restart in 3.x. Signals must be manually re-sent after connector stops.

2. **Schema History DDL**: The `schema.history.internal.store.only.captured.databases.ddl` default value documentation was corrected - verify your deployment's configuration.

3. **OpenLineage Headers** (3.2+): New headers are automatically added to events. Disable with `extended.headers.enabled=false` if needed.

## 6. Kafka Service Configuration

### Dual Deployment Architecture

```
Debezium 2.x Service: dbrep-kafka-connect-cp-kafka-connect:8083
Debezium 3.x Service: dbrep-kafka-connect-v3-cp-kafka-connect:8083
```

### Topic Separation

| Component | 2.x Topic | 3.x Topic |
|-----------|-----------|-----------|
| CDC Data | `cdc_oracle_v2.DEMO.SOURCE_ORDERS` | `cdc_oracle_v3.DEMO.SOURCE_ORDERS` |
| Schema History | `schema-changes.oracle.v2` | `schema-changes.oracle.v3` |
| Target Table | `target_orders_v2` | `target_orders_v3` |

## 7. Migration Recommendations

### Pre-Migration Checklist

- [ ] Verify Java 17 runtime availability
- [ ] Update `snapshot.mode` if using deprecated values
- [ ] Review LOB column handling if using column re-selector
- [ ] Test signal behavior changes in non-production environment
- [ ] Update container image sources from Docker Hub to Quay.io

### Upgrade Path

1. Deploy Debezium 3.x as a separate Kafka Connect instance (dual mode)
2. Configure 3.x connectors with separate topic prefixes and target tables
3. Compare data output between 2.x and 3.x during parallel operation
4. Validate data type mappings match expectations
5. Switch production traffic to 3.x after validation
6. Decommission 2.x instance

### Command Reference

```bash
# Deploy dual mode
make all-dual

# Register connectors on both versions
make register-v2
make register-v3

# Verify data replication
make verify-v2  # Check target_orders_v2
make verify-v3  # Check target_orders_v3

# Clean up
make test-clean
```

### Data Type Comparison Testing

For comprehensive data type testing between Debezium 2.x and 3.x:

```bash
# Run full datatype comparison E2E test
make datatype-all-v2
make datatype-all-v3

# Or run individual steps:
make datatype-setup       # Create DATATYPE_TEST table with 30+ column types
make datatype-register-v2 # Register connectors on Debezium 2.x
make datatype-register-v3 # Register connectors on Debezium 3.x
make datatype-verify      # View data in both MariaDB tables
make datatype-clean       # Delete datatype connectors and tables
```

The comparison report is generated at `docs/debezium-datatype-comparison-results.md`.

## 8. References

- [Debezium 3.0 Release Notes](https://debezium.io/releases/3.0/release-notes)
- [Debezium 3.4.0.Final Release](https://debezium.io/blog/2025/12/16/debezium-3-4-final-released/)
- [Debezium Release Series 3.0](https://debezium.io/releases/3.0/)

## 9. Actual Test Results: Data Type Mapping Differences

### Test Configuration
- Oracle XE 21c with comprehensive DATATYPE_TEST table (30+ column types)
- Both versions tested with `decimal.handling.mode=string` (recommended)
- Previous tests with `decimal.handling.mode=double` shown below for comparison
- Both connectors using JDBC sink to MariaDB with auto-create/auto-evolve

To run your own comparison:
```bash
make datatype-all-v2
make datatype-all-v3
```

### Historical Test Results (with decimal.handling.mode=double)

### Schema Comparison (auto-created by JDBC Sink)

| Oracle Column | Oracle Type | Debezium 2.x → MariaDB | Debezium 3.x → MariaDB | **Difference** |
|---------------|-------------|------------------------|------------------------|----------------|
| ID | NUMBER(10) | `bigint(20)` | `double` | **Yes** |
| COL_NUMBER_5 | NUMBER(5) | `int(11)` | `double` | **Yes** |
| COL_NUMBER_10 | NUMBER(10) | `bigint(20)` | `double` | **Yes** |
| COL_NUMBER_19 | NUMBER(19) | `double` | `double` | No |
| COL_NUMBER_10_2 | NUMBER(10,2) | `double` | `double` | No |
| COL_NUMBER_15_5 | NUMBER(15,5) | `double` | `double` | No |
| COL_FLOAT | FLOAT | `double` | `double` | No |
| COL_BINARY_FLOAT | BINARY_FLOAT | `float` | `float` | No |
| COL_BINARY_DOUBLE | BINARY_DOUBLE | `double` | `double` | No |
| COL_CHAR | CHAR(10) | `text` | `text` | No |
| COL_CHAR_50 | CHAR(50) | `text` | `text` | No |
| COL_VARCHAR2_50 | VARCHAR2(50) | `text` | `text` | No |
| COL_VARCHAR2_500 | VARCHAR2(500) | `text` | `text` | No |
| COL_NCHAR | NCHAR(10) | `text` | `text` | No |
| COL_NVARCHAR2 | NVARCHAR2(100) | `text` | `text` | No |
| COL_DATE | DATE | `bigint(20)` | `bigint(20)` | No |
| COL_TIMESTAMP | TIMESTAMP | `bigint(20)` | `bigint(20)` | No |
| COL_TIMESTAMP_3 | TIMESTAMP(3) | `bigint(20)` | `bigint(20)` | No |
| COL_TIMESTAMP_6 | TIMESTAMP(6) | `bigint(20)` | `bigint(20)` | No |
| COL_TIMESTAMP_TZ | TIMESTAMP WITH TIME ZONE | `text` | `text` | No |
| COL_TIMESTAMP_LTZ | TIMESTAMP WITH LOCAL TIME ZONE | `text` | `text` | No |
| COL_INTERVAL_YM | INTERVAL YEAR TO MONTH | `bigint(20)` | `bigint(20)` | No |
| COL_INTERVAL_DS | INTERVAL DAY TO SECOND | `bigint(20)` | `bigint(20)` | No |
| COL_RAW | RAW(100) | `varbinary(1024)` | `varbinary(1024)` | No |
| COL_BOOLEAN_SIM | NUMBER(1) | `tinyint(4)` | `double` | **Yes** |

### Key Finding: Integer Type Handling with decimal.handling.mode=double

When using `decimal.handling.mode=double`:

**Debezium 2.x** preserves integer types:
- `NUMBER(5)` → `int(11)`
- `NUMBER(10)` → `bigint(20)`
- `NUMBER(1)` → `tinyint(4)`

**Debezium 3.x** converts all numeric to double:
- `NUMBER(5)` → `double`
- `NUMBER(10)` → `double`
- `NUMBER(1)` → `double`

> **Note**: This behavioral difference is specific to `decimal.handling.mode=double`. With `decimal.handling.mode=string` (recommended), both versions produce consistent `text` types for all numeric columns.

### Data Values Comparison

Data values are **identical** between both versions for all rows tested:
- Row 1: Positive values ✓
- Row 2: Negative values ✓
- Row 3: NULL values ✓
- Row 4: Zero values ✓

### Known Issue: VariableScaleDecimal

Both Debezium 2.x and 3.x encounter the same JDBC Sink error when:
- Oracle `NUMBER` without precision (unbounded) is used
- Oracle `FLOAT` without `decimal.handling.mode` configuration

Error: `io.debezium.data.VariableScaleDecimal (STRUCT) type doesn't have a mapping to the SQL database column type`

**Solution**: Use `decimal.handling.mode=double` or `decimal.handling.mode=string` in source connector config.

---

*Report generated: 2026-01-12*
*Project: kafka-dbsync*
*Branch: feature/debezium-3.4*
