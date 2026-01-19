# Kafka-DBSync: Database Replication & CDC Toolkit

A comprehensive Kubernetes-based toolkit for Change Data Capture (CDC) and database replication using Kafka, Debezium, and JDBC connectors.

## Overview

Kafka-DBSync provides infrastructure and automation for building multi-database CDC pipelines that stream changes from source databases to target databases in real-time. It supports Oracle, MariaDB, and MSSQL with flexible deployment options for development and testing.

### Key Features

- **Multi-Database CDC**: Oracle, MariaDB, MSSQL support
- **Real-time Streaming**: Kafka 4.0 with KRaft mode (no ZooKeeper)
- **Debezium Integration**: Log-based CDC connectors (2.x and 3.x versions)
- **Dual Debezium Support**: Run Debezium 2.x and 3.x side-by-side for migration testing
- **JDBC Sink Connectors**: Write to any JDBC-compatible database
- **Web UIs**: Redpanda Console and Kafka Connect UI for monitoring
- **E2E Testing**: Automated setup and verification scripts
- **Kubernetes-Native**: Helm charts for all components
- **Oracle XE 21c**: Fast startup for development and testing

### Architecture

```
                         ┌──────────────────────────────────┐
                         │         Kafka Cluster            │
┌─────────────┐          │  ┌────────────────────────────┐  │          ┌─────────────┐
│   Source    │          │  │    Kafka Connect 2.x       │  │          │   Target    │
│  Database   │──────────│──│    (Debezium 2.6.x)        │──│──────────│  Database   │
│ (Oracle/    │          │  └────────────────────────────┘  │          │ (MariaDB/   │
│  MSSQL)     │          │  ┌────────────────────────────┐  │          │  MSSQL)     │
│             │──────────│──│    Kafka Connect 3.x       │──│──────────│             │
│             │          │  │    (Debezium 3.4.x)        │  │          │             │
└─────────────┘          │  └────────────────────────────┘  │          └─────────────┘
                         └──────────────────────────────────┘
```

**Data Flow:**
1. Debezium source connector captures changes from source database transaction logs
2. Changes are published to Kafka topics
3. JDBC sink connector consumes from Kafka and writes to target database

**Dual Debezium Mode:** Both Debezium 2.x and 3.x can run simultaneously, allowing:
- Side-by-side comparison of CDC behavior
- Safe migration testing between versions
- Independent connector management per version

## Quick Start

### Prerequisites

- Docker Desktop or similar container runtime
- 8GB+ RAM recommended
- kubectl, helm, and kind (or use `make tools` to install)

### 1. Install Tools (Optional)

```bash
make .tools
```

Installs kubectl, helm, and kind if not already present.

### 2. Create Cluster and Deploy Services

```bash
# Debezium 2.x only
make all-v2

# Debezium 3.x only
make all-v3

# BOTH Debezium 2.x and 3.x (recommended)
make all-dual
```

This will:
- Create a kind Kubernetes cluster
- Deploy Kafka, Kafka Connect, Oracle, MariaDB, and supporting services
- Wait for all pods to be ready (~3-5 minutes)

**Dual Mode Services:**
- Debezium 2.x: `dbrep-kafka-connect-cp-kafka-connect:8083`
- Debezium 3.x: `dbrep-kafka-connect-v3-cp-kafka-connect:8083`

### 3. Verify Data Replication

```bash
# Start port forwarding (in separate terminal)
make port-forward

# Verify data in target tables
make verify-v2          # Check Debezium 2.x target table
make verify-v3          # Check Debezium 3.x target table
```

**Replication Results:**
- Debezium 2.x replicates to `target_orders_v2` table
- Debezium 3.x replicates to `target_orders_v3` table

### 4. Test CDC Operations

```bash
# Run CDC tests (INSERT, UPDATE, DELETE)
make test-run

# Verify results
make test-verify
```

### 5. Cleanup

```bash
# Remove connectors only
make test-clean-v2      # Remove Debezium 2.x connectors
make test-clean-v3      # Remove Debezium 3.x connectors
make test-clean         # Remove connectors and tables (both versions)

# Remove entire cluster and services
make clean              # Delete Kind cluster and all services
```

## Project Structure

```
kafka-dbsync/
├── Makefile             # Main Makefile
├── Makefile.param       # Makefile for parameters
├── Makefile.docker      # Makefile for Docker images
├── Makefile.e2e         # Makefile for end-to-end tests
├── deployment/          # Helm wrapper charts for deployment
│   ├── kafka/          # Kafka cluster (Bitnami)
│   ├── kafka-connect/  # Confluent Kafka Connect
│   ├── kafka-connect-ui/  # Kafka Connect UI (Landoop)
│   ├── redpanda-console/  # Redpanda Console (Kafka Web UI)
│   ├── oracle/         # Oracle Database (XE 21c)
│   ├── mariadb/        # MariaDB
│   ├── mssql/          # Microsoft SQL Server
│   └── curl-client/    # Curl client for verification
├── helm-chart/         # Source Helm charts
│   └── */              # Actual chart implementations
├── hack/               # E2E testing and development utilities
│   ├── E2E_TEST_ORACLE_CDC.md  # Detailed E2E guide
│   ├── source-debezium/  # Debezium connector configs
│   ├── sink-jdbc/        # JDBC sink connector configs
│   └── sql/              # Database setup scripts
├── tools/              # Tool installers (kubectl, helm, kind, etc.)
└── docs/               # Documentation and architecture diagrams
```

### Deployment Architecture

The project uses a **wrapper chart pattern**:
- `/deployment/*` - Thin wrapper charts that customize base charts for specific deployments
- `/helm-chart/*` - Source charts with full configurations and templates

This allows environment-specific customization without modifying base charts.

## Available Make Targets

### Main Workflows

```bash
make all-v2             # Full E2E pipeline with Debezium 2.x (base-infra-up, build-v2, test-setup, register-v2, verify-v2)
make all-v3             # Full E2E pipeline with Debezium 3.x (base-infra-up, build-v3, test-setup, register-v3, verify-v3)
make all-dual           # Full E2E pipeline with BOTH Debezium 2.x and 3.x
make clean              # Delete Kind cluster and all services
```

### Infrastructure Management

```bash
make .tools             # Install kubectl, helm, kind
make .check-tools       # Verify tools are installed
make base-infra-up      # Create Kind cluster and deploy base services (Kafka, Oracle, MariaDB, etc.)
make clean              # Delete Kind cluster and all services
```

### Build Images

```bash
make build-v2           # Build and deploy Kafka Connect with Debezium 2.x
make build-v3           # Build and deploy Kafka Connect with Debezium 3.x
make build-all          # Build both Debezium 2.x and 3.x images
```

### E2E Testing

```bash
make test-setup         # Set up databases for testing (setup-oracle + setup-mariadb)
make test-run           # Run CDC tests (INSERT/UPDATE/DELETE)
make test-verify        # Verify data in both v2 and v3 target tables
make test-clean         # Clean up connectors and tables (both versions)
make test-clean-v2      # Clean up Debezium 2.x connectors only
make test-clean-v3      # Clean up Debezium 3.x connectors only
make register-v2        # Register connectors on Debezium 2.x
make register-v3        # Register connectors on Debezium 3.x
make verify-v2          # Verify data from Debezium 2.x (target_orders_v2)
make verify-v3          # Verify data from Debezium 3.x (target_orders_v3)
```

### Data Type Comparison Testing

Data type comparison testing is now integrated into the main `Makefile`:

```bash
make datatype-all-v2    # Full datatype test with Debezium 2.x
make datatype-all-v3    # Full datatype test with Debezium 3.x
make datatype-all-dual  # Full datatype test with both 2.x and 3.x
make datatype-setup     # Set up Oracle and MariaDB for datatype testing
make datatype-register-v2 # Register Debezium 2.x connectors for datatype testing
make datatype-register-v3 # Register Debezium 3.x connectors for datatype testing
make datatype-verify    # Verify data in MariaDB for datatype testing
make datatype-clean     # Clean up connectors and tables for datatype testing
```

### Utilities

```bash
make port-forward       # Forward all services (UI:8000, Console:8080, 2.x:8083, 3.x:8084)
make status-v2          # Check connector status on Debezium 2.x
make status-v3          # Check connector status on Debezium 3.x
make logs-v2            # View Debezium 2.x Kafka Connect logs
make logs-v3            # View Debezium 3.x Kafka Connect logs
```

## Web UIs

Two web interfaces are available for monitoring and managing Kafka:

### Redpanda Console (Port 8080)

Modern Kafka web UI for viewing topics, messages, consumer groups, and connectors.

```bash
kubectl port-forward svc/dbrep-redpanda-console 8080:8080 -n dev
# Open http://localhost:8080
```

### Kafka Connect UI (Port 8000)

Landoop UI for managing Kafka Connect connectors.

```bash
kubectl port-forward svc/dbrep-kafka-connect-ui 8000:8000 -n dev
# Open http://localhost:8000
```

## Dual Debezium Deployment

Run both Debezium 2.x and 3.x simultaneously for migration testing and version comparison.

### Setup

```bash
# Deploy with both Debezium versions (recommended)
make all-dual

# Or step by step:
make base-infra-up      # Create cluster and deploy base services
make build-all          # Build both Debezium images
make test-setup         # Set up Oracle and MariaDB
make register-v2        # Register connectors on 2.x
make register-v3        # Register connectors on 3.x
```

This deploys two Kafka Connect instances:
- **Debezium 2.x**: `dbrep-kafka-connect-cp-kafka-connect:8083` (Java 11, Debezium 2.6.x)
- **Debezium 3.x**: `dbrep-kafka-connect-v3-cp-kafka-connect:8083` (Java 17, Debezium 3.4.x)

### Connector Configuration

| Version | Source Connector | Sink Connector | Target Table |
|---------|-----------------|----------------|--------------|
| 2.x | `source_cdc_oracle_demo_v2` | `sink_cdc_oracle_to_mariadb_v2` | `target_orders_v2` |
| 3.x | `source_cdc_oracle_demo_v3` | `sink_cdc_oracle_to_mariadb_v3` | `target_orders_v3` |

### Verify Results

```bash
# Check data from both versions
make verify-v2
make verify-v3

# Or verify both at once
make test-verify
```

### Data Type Comparison Testing

Compare how Debezium 2.x and 3.x handle Oracle data types differently:

```bash
# Run complete datatype comparison test for both versions at once
make datatype-all-dual

# Or run individually
make datatype-all-v2
make datatype-all-v3
```

This will:
1. Create a `DATATYPE_TEST` table in Oracle with various data types
2. Register datatype-specific connectors on both 2.x and 3.x
3. Replicate data to `target_database.datatype_test_v2` and `target_database.datatype_test_v3` tables
4. You can verify the data using `make datatype-verify`

**Individual datatype commands:**
```bash
make datatype-setup       # Create test table in Oracle
make datatype-register-v2 # Register connectors on 2.x
make datatype-register-v3 # Register connectors on 3.x
make datatype-verify      # Show data from both tables
make datatype-clean       # Delete datatype connectors and tables
```

### Use Cases

- **Version Migration**: Test Debezium 3.x before upgrading from 2.x
- **Behavior Comparison**: Compare CDC behavior between versions
- **Data Type Analysis**: Understand schema differences between versions
- **Parallel Processing**: Run different workloads on different versions
- **Rollback Safety**: Keep 2.x running while testing 3.x

## Supported CDC Scenarios

### Source Connectors (Debezium)

- **Oracle**: LogMiner-based CDC
- **MSSQL**: SQL Server CDC

### Sink Connectors (JDBC)

- **MariaDB**: JDBC sink with upsert
- Any JDBC-compatible database

### Example Pipelines

1. **Oracle → Kafka → MariaDB** (E2E tested)
2. **MSSQL → Kafka → MariaDB**
3. Custom combinations via connector configs in `hack/`

## Documentation

- [E2E Test Guide](hack/E2E_TEST_ORACLE_CDC.md) - Detailed Oracle CDC to MariaDB walkthrough
- [Makefile Reference](Makefile) - All automation targets and configuration
- Connector Configurations:
  - [Source Debezium Connectors](hack/source-debezium/)
  - [JDBC Sink Connectors](hack/sink-jdbc/)
  - [Database Setup Scripts](hack/sql/)

## Troubleshooting

### Check Pod Status

```bash
kubectl get pods -n dev
```

### View Connector Logs

```bash
make logs-v2            # Debezium 2.x logs
make logs-v3            # Debezium 3.x logs
# or
kubectl logs -f deployment/dbrep-kafka-connect-cp-kafka-connect -n dev
kubectl logs -f deployment/dbrep-kafka-connect-v3-cp-kafka-connect -n dev
```

### Check Connector Status

```bash
make status-v2          # Debezium 2.x connectors
make status-v3          # Debezium 3.x connectors
# or
curl http://localhost:8083/connectors/<connector-name>/status | jq
```

### Common Issues

| Issue | Solution |
|-------|----------|
| Pods not starting | Check resources: `docker stats`, increase Docker memory |
| Oracle startup timeout | Increase timeout in values or check Oracle pod logs |
| Connector FAILED state | Check logs: `make logs-v2` or `make logs-v3`, verify database connectivity |
| No data replicating | Check connector status: `make status-v2` or `make status-v3` |
| Port forwarding fails | Ensure pods are ready: `kubectl get pods -n dev` |

### Reset Everything

```bash
# Delete cluster and start fresh
make clean
make all-v2             # Debezium 2.x only
# or
make all-dual           # Both Debezium 2.x and 3.x
```

## Development

### Adding New Connectors

1. Create connector config in `hack/source-debezium/` or `hack/sink-jdbc/`
2. Add SQL setup script in `hack/sql/`
3. Add Makefile targets for setup and registration
4. Test with `make e2e`

### Customizing Deployments

Edit values in `deployment/*/values.yaml`:
- Resource limits
- Storage sizes
- Database passwords
- Connector configurations

### Running Manual Tests

```bash
# Start port forwarding
make port-forward

# Register connector manually
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @hack/source-debezium/oracle-free-demo-2x.json

# Check status
curl http://localhost:8083/connectors/source_cdc_oracle_demo_v2/status | jq
```

## Contributing

When adding features:
1. Update relevant documentation
2. Add Makefile targets for automation
3. Test with Oracle XE 21c
4. Follow existing naming conventions

## License

[Add your license here]

## References

- [Apache Kafka](https://kafka.apache.org/)
- [Debezium](https://debezium.io/)
- [Confluent Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html)
- [Kubernetes](https://kubernetes.io/)
- [Helm](https://helm.sh/)
