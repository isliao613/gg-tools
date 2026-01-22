# Testing LegacyCharsetTransform SMT

This guide walks through testing the LegacyCharsetTransform SMT with the Oracle -> Kafka -> MariaDB pipeline.

## Prerequisites

- Docker and Docker Compose
- At least 8GB of available RAM (Oracle requires 4GB)

## Quick Start

```bash
cd deployment/kafka-connect/docker

# Start all services
docker compose up -d

# Wait for services to be healthy (Oracle takes ~3 minutes)
docker compose ps
```

## Step 1: Wait for Services

Oracle takes the longest to start. Wait until all services show "healthy":

```bash
# Check status
docker compose ps

# Expected output:
# docker-kafka-1            ... (healthy)
# docker-kafka-connect-1    ... (healthy)
# docker-mariadb-1          ... (healthy)
# docker-oracle-1           ... (healthy)
```

## Step 2: Enable Oracle ARCHIVELOG Mode

Oracle Free doesn't enable ARCHIVELOG by default. Enable it for CDC:

```bash
docker compose exec oracle bash -c "sqlplus -S / as sysdba << 'EOF'
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
SELECT LOG_MODE FROM V\$DATABASE;
EXIT;
EOF"
```

Expected output: `ARCHIVELOG`

## Step 3: Grant Oracle CDC Privileges

```bash
docker compose exec oracle bash -c "sqlplus -S / as sysdba << 'EOF'
ALTER USER c##dbzuser QUOTA UNLIMITED ON USERS;
GRANT SELECT ON V_\$DATABASE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_\$LOG TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_\$LOG_HISTORY TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_\$LOGMNR_LOGS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_\$LOGMNR_CONTENTS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_\$LOGFILE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_\$ARCHIVED_LOG TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_\$ARCHIVE_DEST_STATUS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_\$TRANSACTION TO c##dbzuser CONTAINER=ALL;
EXIT;
EOF"
```

## Step 4: Insert Big-5 Test Data

Insert test data that simulates halfwidth-garbled Big-5 characters:

```bash
docker compose exec oracle bash -c "sqlplus -S testuser/testpwd@//localhost:1521/FREEPDB1 << 'EOF'
DELETE FROM customers;

-- Record 1: name=測試 address=台北市 description=測試中文
INSERT INTO customers (id, name, address, description) VALUES (
    1,
    UNISTR('\FFB4\FFFA\FFB8\FFD5'),
    UNISTR('\FFA5\0078\FFA5\005F\FFA5\FFAB'),
    UNISTR('\FFB4\FFFA\FFB8\FFD5\FFA4\FFA4\FFA4\FFE5')
);

-- Record 2: name=你好 address=世界 description=你好世界
INSERT INTO customers (id, name, address, description) VALUES (
    2,
    UNISTR('\FFA7\0041\FFA6\006E'),
    UNISTR('\FFA5\0040\FFAC\FFC9'),
    UNISTR('\FFA7\0041\FFA6\006E\FFA5\0040\FFAC\FFC9')
);

-- Record 3: Mixed ASCII and Big-5
INSERT INTO customers (id, name, address, description) VALUES (
    3,
    'Customer-' || UNISTR('\FFA5\0078\FFA5\005F'),
    UNISTR('\FFA5\0078\FFA5\005F\FFA5\FFAB') || '-District',
    'Mixed content with ' || UNISTR('\FFA4\FFA4\FFA4\FFE5')
);

COMMIT;
SELECT id, name, address FROM customers;
EOF"
```

The data will display as garbled halfwidth characters (e.g., `ﾴ￺ﾸￕ`).

## Step 5: Deploy Oracle Source Connector (with SMT)

```bash
docker compose exec kafka-connect curl -sf -X POST \
  -H "Content-Type: application/json" \
  -d @/connectors/oracle-source-with-smt.json \
  http://localhost:8083/connectors | jq .
```

Check connector status:

```bash
docker compose exec kafka-connect curl -s \
  http://localhost:8083/connectors/oracle-smt-sink/status | jq '.tasks[0].state'
```

Expected: `"RUNNING"`

## Step 6: Verify SMT Transformation in Kafka

Check that the SMT decoded the Big-5 characters:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic oracle-sink.TESTUSER.CUSTOMERS \
  --from-beginning \
  --timeout-ms 10000 2>/dev/null | jq '.payload.after'
```

Expected output (properly decoded UTF-8):

```json
{"ID": "1", "NAME": "測試", "ADDRESS": "台北市", "DESCRIPTION": "測試中文"}
{"ID": "2", "NAME": "你好", "ADDRESS": "世界", "DESCRIPTION": "你好世界"}
{"ID": "3", "NAME": "Customer-台北", "ADDRESS": "台北市-District", "DESCRIPTION": "Mixed content with 中文"}
```

## Step 7: Deploy MariaDB Sink Connector

```bash
docker compose exec kafka-connect curl -sf -X POST \
  -H "Content-Type: application/json" \
  -d @/connectors/mariadb-sink.json \
  http://localhost:8083/connectors | jq .
```

Check connector status:

```bash
docker compose exec kafka-connect curl -s \
  http://localhost:8083/connectors/mariadb-sink/status | jq '.tasks[0].state'
```

Expected: `"RUNNING"`

## Step 8: Verify Data in MariaDB

```bash
docker compose exec mariadb mariadb -u testuser -ptestpwd testdb \
  -e "SELECT * FROM customers;"
```

Expected output:

```
+----+----------------+---------------------+---------------------------+
| ID | NAME           | ADDRESS             | DESCRIPTION               |
+----+----------------+---------------------+---------------------------+
| 1  | 測試           | 台北市              | 測試中文                   |
| 2  | 你好           | 世界                | 你好世界                   |
| 3  | Customer-台北  | 台北市-District     | Mixed content with 中文    |
+----+----------------+---------------------+---------------------------+
```

## Step 9: Test CDC (Insert New Record)

Insert a new record to test real-time CDC:

```bash
docker compose exec oracle bash -c "sqlplus -S testuser/testpwd@//localhost:1521/FREEPDB1 << 'EOF'
INSERT INTO customers (id, name, address, description) VALUES (
    4,
    UNISTR('\FFB4\FFFA\FFB8\FFD5'),
    UNISTR('\FFA5\0078\FFA5\005F\FFA5\FFAB'),
    'CDC test record'
);
COMMIT;
EOF"
```

Wait a few seconds, then verify in MariaDB:

```bash
docker compose exec mariadb mariadb -u testuser -ptestpwd testdb \
  -e "SELECT * FROM customers WHERE ID='4';"
```

## Cleanup

```bash
# Stop all services
docker compose down

# Remove volumes (deletes all data)
docker compose down -v
```

## Troubleshooting

### Check Connector Status

```bash
# List all connectors
docker compose exec kafka-connect curl -s http://localhost:8083/connectors | jq .

# Check specific connector status
docker compose exec kafka-connect curl -s \
  http://localhost:8083/connectors/<connector-name>/status | jq .
```

### View Kafka Connect Logs

```bash
docker compose logs kafka-connect --tail 100

# Filter for SMT logs
docker compose logs kafka-connect 2>&1 | grep LegacyCharsetTransform
```

### List Kafka Topics

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

### Reset Connector (Delete and Recreate)

```bash
docker compose exec kafka-connect curl -sf -X DELETE \
  http://localhost:8083/connectors/<connector-name>

# Then redeploy
```

## Understanding the Test Data

The test uses Unicode halfwidth characters to simulate what Oracle JDBC returns from a US7ASCII database with Big-5 data:

| Big-5 Character | Big-5 Bytes | Halfwidth Unicode | Display |
|-----------------|-------------|-------------------|---------|
| 測 | B4 FA | U+FFB4 U+FFFA | ﾴ￺ |
| 試 | B8 D5 | U+FFB8 U+FFD5 | ﾸￕ |
| 台 | A5 78 | U+FFA5 U+0078 | ﾥx |
| 北 | A5 5F | U+FFA5 U+005F | ﾥ_ |
| 市 | A5 AB | U+FFA5 U+FFAB | ﾥﾫ |

The SMT reverses this conversion: `byte = codepoint - 0xFF00`, then decodes using Big-5 charset.
