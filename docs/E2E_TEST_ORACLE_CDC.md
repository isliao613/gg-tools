# E2E Test: Oracle CDC to MariaDB

This guide walks through testing the Debezium Oracle CDC pipeline that replicates data from Oracle XE 21c to MariaDB via Kafka.

## Architecture

```
Oracle XE 21c (XEPDB1.DEMO.SOURCE_ORDERS)
    │
    ▼ Debezium Oracle Connector (LogMiner)
    │
Kafka (cdc_oracle.DEMO.SOURCE_ORDERS)
    │
    ▼ JDBC Sink Connector
    │
MariaDB (target_database.target_orders)
```

## Prerequisites

- Kubernetes cluster with kubectl configured
- Helm 3.x installed
- All services deployed via `make all-dual` (or `make all-v2` / `make all-v3`)

## Quick Start (Automated)

The easiest way to run the E2E test is using Make targets:

```bash
# Deploy everything and run E2E test
make all-dual

# Or step by step:
make base-infra-up    # Create cluster and deploy services
make build-all        # Build Kafka Connect images
make test-setup       # Setup Oracle and MariaDB
make register-v2      # Register Debezium 2.x connectors
make register-v3      # Register Debezium 3.x connectors
make test-verify      # Verify replication
```

## Manual Steps

### Step 1: Start Port Forwarding

```bash
make port-forward
```

This exposes:
- Kafka Connect 2.x REST API: http://localhost:8083
- Kafka Connect 3.x REST API: http://localhost:8084
- Kafka Connect UI: http://localhost:8000
- Redpanda Console: http://localhost:8080

### Step 2: Setup Oracle Database

The `make test-setup` target automatically configures Oracle XE 21c. For manual setup:

```bash
# Get Oracle pod name
ORACLE_POD=$(kubectl get pods -n dev -l app=oracle-db -o jsonpath='{.items[0].metadata.name}')

# Connect to Oracle as SYSDBA (CDB)
kubectl exec -it $ORACLE_POD -n dev -- sqlplus sys/oracle@localhost:1521/XE as sysdba
```

Or run the automated setup scripts:

```bash
# Enable ARCHIVELOG mode (required for CDC)
kubectl cp hack/sql/oracle-xe-archivelog.sql $ORACLE_POD:/tmp/archivelog.sql -n dev
kubectl exec $ORACLE_POD -n dev -- sqlplus -S / as sysdba @/tmp/archivelog.sql

# Create CDC user in CDB
kubectl cp hack/sql/oracle-xe-cdc-user.sql $ORACLE_POD:/tmp/cdc-user.sql -n dev
kubectl exec $ORACLE_POD -n dev -- sqlplus -S sys/oracle@localhost:1521/XE as sysdba @/tmp/cdc-user.sql

# Create demo schema in PDB
kubectl cp hack/sql/oracle-xe-demo-schema.sql $ORACLE_POD:/tmp/demo-schema.sql -n dev
kubectl exec $ORACLE_POD -n dev -- sqlplus -S sys/oracle@localhost:1521/XEPDB1 as sysdba @/tmp/demo-schema.sql
```

The scripts will:
- Enable ARCHIVELOG mode and supplemental logging
- Create `c##dbzcdc` user in CDB with LogMiner privileges
- Create `DEMO` schema in PDB (XEPDB1)
- Create `DEMO.SOURCE_ORDERS` table with test data

### Step 3: Setup MariaDB Database

Connect to MariaDB:

```bash
kubectl exec -it dbrep-mariadb-0 -n dev -- mysql -h dbrep-mariadb.dev.svc.cluster.local -uroot -proot_password
```

Create the target database:

```sql
CREATE DATABASE IF NOT EXISTS `target_database` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

Note: The `target_orders` table will be auto-created by the sink connector.

### Step 4: Register Source Connector (Debezium Oracle)

For Debezium 2.x:
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @hack/source-debezium/oracle-free-demo-2x.json
```

For Debezium 3.x:
```bash
curl -X POST http://localhost:8084/connectors \
  -H "Content-Type: application/json" \
  -d @hack/source-debezium/oracle-free-demo-3x.json
```

Verify connector status:

```bash
curl http://localhost:8083/connectors/source_cdc_oracle_demo/status | jq
curl http://localhost:8084/connectors/source_cdc_oracle_demo_v3/status | jq
```

### Step 5: Register Sink Connector (JDBC MariaDB)

Wait for the source connector to complete initial snapshot (check status), then:

For Debezium 2.x:
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @hack/sink-jdbc/cdc_oracle_mariadb-demo-2x.json
```

For Debezium 3.x:
```bash
curl -X POST http://localhost:8084/connectors \
  -H "Content-Type: application/json" \
  -d @hack/sink-jdbc/cdc_oracle_mariadb-demo-3x.json
```

Verify connector status:

```bash
curl http://localhost:8083/connectors/sink_cdc_oracle_to_mariadb/status | jq
curl http://localhost:8084/connectors/sink_cdc_oracle_to_mariadb_v3/status | jq
```

### Step 6: Verify Initial Sync

Check MariaDB for replicated data:

```bash
# Debezium 2.x target table
kubectl exec -it dbrep-mariadb-0 -n dev -- mysql -h dbrep-mariadb.dev.svc.cluster.local -uroot -proot_password -e "SELECT * FROM target_database.target_orders;"

# Debezium 3.x target table
kubectl exec -it dbrep-mariadb-0 -n dev -- mysql -h dbrep-mariadb.dev.svc.cluster.local -uroot -proot_password -e "SELECT * FROM target_database.target_orders_v3;"
```

Expected output: 4 rows matching Oracle source data.

### Step 7: Test CDC (Change Data Capture)

Connect to Oracle PDB:

```bash
ORACLE_POD=$(kubectl get pods -n dev -l app=oracle-db -o jsonpath='{.items[0].metadata.name}')
kubectl exec -it $ORACLE_POD -n dev -- sqlplus sys/oracle@localhost:1521/XEPDB1 as sysdba
```

#### Test INSERT

```sql
INSERT INTO DEMO.SOURCE_ORDERS(ID, ORDER_NO, CUSTOMER_NAME, AMOUNT, STATUS) VALUES(5, 'ORD-005', 'Eve', 3100.00, 'PENDING');
COMMIT;
```

#### Test UPDATE

```sql
UPDATE DEMO.SOURCE_ORDERS SET STATUS='COMPLETED', MODIFIED_AT=CURRENT_TIMESTAMP WHERE ID=1;
COMMIT;
```

#### Test DELETE

```sql
DELETE FROM DEMO.SOURCE_ORDERS WHERE ID=3;
COMMIT;
```

#### Verify Changes in MariaDB

```bash
kubectl exec -it dbrep-mariadb-0 -n dev -- mysql -h dbrep-mariadb.dev.svc.cluster.local -uroot -proot_password -e "SELECT * FROM target_database.target_orders ORDER BY ID;"
```

Expected:
- Row ID=5 should appear (INSERT)
- Row ID=1 should have STATUS='COMPLETED' (UPDATE)
- Row ID=3 should be deleted (DELETE)

## Troubleshooting

### Check Connector Logs

```bash
# Debezium 2.x logs
make logs-v2
# or
kubectl logs -f deployment/dbrep-kafka-connect-cp-kafka-connect -n dev --tail=100

# Debezium 3.x logs
make logs-v3
# or
kubectl logs -f deployment/dbrep-kafka-connect-v3-cp-kafka-connect -n dev --tail=100
```

### Check Kafka Topics

```bash
kubectl exec -it dbrep-kafka-0 -n dev -- kafka-topics.sh --bootstrap-server localhost:9092 --list | grep cdc_oracle
```

### Consume Messages from Topic

```bash
kubectl exec -it dbrep-kafka-0 -n dev -- kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic cdc_oracle.DEMO.SOURCE_ORDERS \
  --from-beginning \
  --max-messages 5
```

### Delete Connectors (Reset)

```bash
# Debezium 2.x
curl -X DELETE http://localhost:8083/connectors/source_cdc_oracle_demo
curl -X DELETE http://localhost:8083/connectors/sink_cdc_oracle_to_mariadb

# Debezium 3.x
curl -X DELETE http://localhost:8084/connectors/source_cdc_oracle_demo_v3
curl -X DELETE http://localhost:8084/connectors/sink_cdc_oracle_to_mariadb_v3
```

Or use Make targets:
```bash
make test-clean-v2    # Clean Debezium 2.x connectors
make test-clean-v3    # Clean Debezium 3.x connectors
make test-clean       # Clean all connectors and tables
```

### Common Issues

| Issue | Solution |
|-------|----------|
| ORA-01031: insufficient privileges | Re-run GRANT statements as SYSDBA |
| Connector FAILED state | Check logs, verify database connectivity |
| No data in MariaDB | Verify topic exists, check sink connector status |
| Schema mismatch errors | Drop target table, let auto.create recreate it |

## Connector Configuration Reference

### Source: `hack/source-debezium/oracle-free-demo-2x.json`

| Property | Value | Description |
|----------|-------|-------------|
| connector.class | io.debezium.connector.oracle.OracleConnector | Debezium Oracle connector |
| database.pdb.name | XEPDB1 | Pluggable database name |
| table.include.list | DEMO.SOURCE_ORDERS | Tables to capture |
| topic.prefix | cdc_oracle | Kafka topic prefix |
| log.mining.strategy | online_catalog | Use online catalog for mining |

### Sink: `hack/sink-jdbc/cdc_oracle_mariadb-demo-2x.json`

| Property | Value | Description |
|----------|-------|-------------|
| connector.class | io.confluent.connect.jdbc.JdbcSinkConnector | Confluent JDBC sink |
| topics | cdc_oracle.DEMO.SOURCE_ORDERS | Source Kafka topic |
| insert.mode | upsert | Handle duplicates via upsert |
| pk.mode | record_key | Use message key as primary key |
| auto.create | true | Auto-create target table |
| delete.enabled | true | Process delete events |
