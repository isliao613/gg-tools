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
- **Lightweight Mode**: Fast Oracle XE for development

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
make tools
```

Installs kubectl, helm, and kind if not already present.

### 2. Create Cluster and Deploy Services

```bash
# Standard mode with Debezium 2.x (default)
make all

# Dual mode with BOTH Debezium 2.x and 3.x
make all-dual

# Debezium 3.4 only
make all-debezium-3.4

# Enterprise Oracle mode (Oracle EE 19c, full features)
make all LIGHTWEIGHT=0
```

This will:
- Create a kind Kubernetes cluster
- Deploy Kafka, Kafka Connect, Oracle, MariaDB, and supporting services
- Wait for all pods to be ready (~3-5 minutes)

**Dual Mode Services:**
- Debezium 2.x: `dbrep-kafka-connect-cp-kafka-connect:8083`
- Debezium 3.x: `dbrep-kafka-connect-v3-cp-kafka-connect:8083`

### 3. Run E2E Test

```bash
# Start port forwarding (in separate terminal)
make port-forward

# Run E2E test (Debezium 2.x)
make e2e

# Or test BOTH Debezium 2.x and 3.x (requires dual deployment)
make e2e-dual
```

The E2E test will:
- Setup Oracle and MariaDB databases
- Register Debezium source and JDBC sink connectors
- Verify data replication

**Dual E2E Test Results:**
- Debezium 2.x replicates to `target_orders` table
- Debezium 3.x replicates to `target_orders_3x` table

### 4. Test CDC Operations

```bash
# Test INSERT, UPDATE, DELETE replication
make test
```

### 5. Cleanup

```bash
# Remove connectors only
make clean

# Remove entire cluster
make cluster-delete
```

## Project Structure

```
kafka-dbsync/
├── deployment/          # Helm wrapper charts for deployment
│   ├── kafka/          # Kafka cluster (Bitnami)
│   ├── kafka-connect/  # Confluent Kafka Connect
│   ├── kafka-connect-ui/  # Kafka Connect UI (Landoop)
│   ├── redpanda-console/  # Redpanda Console (Kafka Web UI)
│   ├── oracle/         # Oracle Database (XE/EE)
│   ├── mariadb/        # MariaDB
│   ├── mssql/          # Microsoft SQL Server
│   └── curl-client/    # Curl client for verification
├── helm-chart/         # Source Helm charts
│   └── */              # Actual chart implementations
├── hack/               # E2E testing and development utilities
│   ├── Makefile        # Automated E2E test orchestration
│   ├── E2E_TEST_ORACLE_CDC.md  # Detailed E2E guide
│   ├── source-debezium/  # Debezium connector configs
│   ├── sink-jdbc/        # JDBC sink connector configs
│   └── sql/              # Database setup scripts
├── tools/              # Tool installers (kubectl, helm, kind, etc.)
└── doc/                # Architecture diagrams (PlantUML)
```

### Deployment Architecture

The project uses a **wrapper chart pattern**:
- `/deployment/*` - Thin wrapper charts that customize base charts for specific deployments
- `/helm-chart/*` - Source charts with full configurations and templates

This allows environment-specific customization without modifying base charts.

## Available Make Targets

### Infrastructure Management

```bash
make tools              # Install kubectl, helm, kind
make cluster            # Create kind cluster
make cluster-delete     # Delete kind cluster
make deploy             # Deploy all services (Debezium 2.x)
make deploy-dual        # Deploy with BOTH Debezium 2.x and 3.x
make deploy-debezium-3.4  # Deploy with Debezium 3.4 only
make deploy-delete      # Remove all services
make wait-ready         # Wait for pods to be ready
make all                # Full setup: cluster + deploy + wait + e2e
make all-dual           # Full setup with BOTH Debezium 2.x and 3.x
make all-debezium-3.4   # Full setup with Debezium 3.4 only
```

### Build Images

```bash
make build-debezium-2.x-image  # Build Kafka Connect with Debezium 2.x
make build-debezium-3.4-image  # Build Kafka Connect with Debezium 3.4
make build-all-images          # Build both images
```

### E2E Testing

```bash
make e2e                # Run full E2E test (Debezium 2.x)
make e2e-2x             # Run E2E test using Debezium 2.x
make e2e-3x             # Run E2E test using Debezium 3.x
make e2e-dual           # Run E2E test on BOTH versions
make setup              # Setup databases only
make register           # Register connectors (2.x)
make register-2x        # Register connectors on Debezium 2.x
make register-3x        # Register connectors on Debezium 3.x
make verify             # Verify data replication
make verify-2x          # Verify data from Debezium 2.x
make verify-3x          # Verify data from Debezium 3.x
make test               # Test CDC operations (INSERT/UPDATE/DELETE)
make clean              # Delete connectors (2.x)
make clean-dual         # Delete connectors (both versions)
make reset              # Full reset (connectors + tables)
```

### Utilities

```bash
make port-forward       # Forward Kafka Connect (8083) and Kafka Connect UI (8000)
make status             # Check connector status
make logs               # Tail Kafka Connect logs
make topics             # List Kafka CDC topics
make pods               # Show pod status
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

## Configuration Modes

### Lightweight Mode (Default)

```bash
make all LIGHTWEIGHT=1
```

- Oracle XE 21c Free (gvenzl/oracle-xe)
- Fast startup (~2-3 minutes)
- Lower resource requirements
- Ideal for development and testing

### Enterprise Mode

```bash
make all LIGHTWEIGHT=0
```

- Oracle EE 19c
- Slower startup (~10 minutes)
- Full Oracle features
- Production-like testing

## Dual Debezium Deployment

Run both Debezium 2.x and 3.x simultaneously for migration testing and version comparison.

### Setup

```bash
# Deploy with both Debezium versions
make all-dual
```

This deploys two Kafka Connect instances:
- **Debezium 2.x**: `dbrep-kafka-connect-cp-kafka-connect:8083` (Java 11, Debezium 2.6.x)
- **Debezium 3.x**: `dbrep-kafka-connect-v3-cp-kafka-connect:8083` (Java 17, Debezium 3.4.x)

### Testing Both Versions

```bash
# Run E2E test on both versions
make e2e-dual
```

This registers connectors on both instances:
| Version | Source Connector | Sink Connector | Target Table |
|---------|-----------------|----------------|--------------|
| 2.x | `source_cdc_oracle_demo` | `sink_cdc_oracle_to_mariadb` | `target_orders` |
| 3.x | `source_cdc_oracle_demo_3x` | `sink_cdc_oracle_to_mariadb_3x` | `target_orders_3x` |

### Verify Results

```bash
# Check data from both versions
make verify-2x
make verify-3x
```

### Use Cases

- **Version Migration**: Test Debezium 3.x before upgrading from 2.x
- **Behavior Comparison**: Compare CDC behavior between versions
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
- [Makefile Reference](hack/Makefile) - All automation targets and configuration
- Connector Configurations:
  - [Source Debezium Connectors](hack/source-debezium/)
  - [JDBC Sink Connectors](hack/sink-jdbc/)
  - [Database Setup Scripts](hack/sql/)

## Troubleshooting

### Check Pod Status

```bash
make pods
# or
kubectl get pods -n dev
```

### View Connector Logs

```bash
make logs
# or
kubectl logs -f deployment/dbrep-kafka-connect-cp-kafka-connect -n dev
```

### Check Connector Status

```bash
make status
# or
curl http://localhost:8083/connectors/<connector-name>/status | jq
```

### Common Issues

| Issue | Solution |
|-------|----------|
| Pods not starting | Check resources: `docker stats`, increase Docker memory |
| Oracle startup timeout | Use `LIGHTWEIGHT=1` or increase timeout in values |
| Connector FAILED state | Check logs: `make logs`, verify database connectivity |
| No data replicating | Verify topic exists: `make topics`, check connector status |
| Port forwarding fails | Ensure pods are ready: `make pods` |

### Reset Everything

```bash
# Delete cluster and start fresh
make cluster-delete
make all
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
  -d @hack/source-debezium/oracle-demo.json

# Check status
curl http://localhost:8083/connectors/source_cdc_oracle_demo/status | jq
```

## Contributing

When adding features:
1. Update relevant documentation
2. Add Makefile targets for automation
3. Test in both LIGHTWEIGHT and Enterprise modes
4. Follow existing naming conventions

## License

[Add your license here]

## References

- [Apache Kafka](https://kafka.apache.org/)
- [Debezium](https://debezium.io/)
- [Confluent Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html)
- [Kubernetes](https://kubernetes.io/)
- [Helm](https://helm.sh/)
