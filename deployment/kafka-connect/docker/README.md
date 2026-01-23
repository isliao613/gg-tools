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

## Step 3: Grant Quota to CDC User

Most CDC privileges are already granted by the startup script. We only need to grant quota for the schema history table:

```bash
docker compose exec oracle bash -c "sqlplus -S / as sysdba << 'EOF'
ALTER USER c##dbzuser QUOTA UNLIMITED ON USERS;
EXIT;
EOF"
```

## Step 4: Insert Big-5 Test Data

Insert test data that simulates halfwidth-garbled Big-5 characters:

```bash
docker compose exec oracle /container-entrypoint-startdb.d/insert_big5_data.sh
```

The script inserts 3 records and displays the character mappings. The data will show as garbled halfwidth characters (e.g., `ﾴ￺ﾸￕ`).

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
  http://localhost:8083/connectors/oracle-source-smt/status | jq '.tasks[0].state'
```

Expected: `"RUNNING"`

## Step 6: Verify SMT Transformation in Kafka

Check that the SMT decoded the Big-5 characters:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic oracle.TESTUSER.CUSTOMERS \
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
docker compose exec oracle bash -c "sqlplus -S c##dbzuser/dbz@//localhost:1521/XEPDB1 << 'EOF'
INSERT INTO big5_test (id, name, address, description) VALUES (
    4,
    CONVERT('新增', 'ZHT16MSWIN950', 'AL32UTF8'),
    CONVERT('測試', 'ZHT16MSWIN950', 'AL32UTF8'),
    'CDC test record'
);
COMMIT;
EOF"
```

Wait a few seconds, then verify in Kafka:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic oracle.C__DBZUSER.BIG5_TEST \
  --from-beginning \
  --timeout-ms 10000 2>/dev/null | grep '"ID":"4"' | jq '.payload.after'
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
