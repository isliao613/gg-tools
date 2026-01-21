# Kafka-DBSync

Kubernetes-based CDC toolkit for database replication using Kafka, Debezium, and JDBC connectors.

## Features

- **Multi-Database CDC**: Oracle, MariaDB, PostgreSQL, MSSQL via Debezium
- **Dual Debezium Support**: Run 2.x and 3.x side-by-side for migration testing
- **IIDR Sink Connector**: Custom connector for IBM InfoSphere Data Replication events
- **Kafka 4.0**: KRaft mode (no ZooKeeper)
- **Web UIs**: Redpanda Console and Kafka Connect UI
- **Modular Makefiles**: Independent test suites that can run separately

## Architecture

```
┌─────────────┐     ┌─────────────────────────────────┐     ┌─────────────┐
│   Source    │     │          Kafka Cluster          │     │   Target    │
│  (Oracle/   │────▶│  ┌─────────────────────────┐    │────▶│ (MariaDB/   │
│   MSSQL)    │     │  │ Kafka Connect 2.x / 3.x │    │     │ PostgreSQL) │
└─────────────┘     │  └─────────────────────────┘    │     └─────────────┘
                    └─────────────────────────────────┘
```

## Quick Start

```bash
# 1. Install tools (kubectl, helm, kind)
make .tools

# 2. Create infrastructure (Kind cluster, Kafka, Oracle, MariaDB, PostgreSQL)
make base-infra-up

# 3. Build Kafka Connect images with Debezium
make build-all

# 4. Run tests (choose one)
make e2e-all-dual       # E2E: Oracle → MariaDB
make datatype-all-dual  # Datatype comparison
make iidr-all-dual      # IIDR sink: Kafka → MariaDB/PostgreSQL

# 5. Cleanup
make clean
```

> **Note**: Steps 1-3 are prerequisites. All test targets (`e2e-*`, `datatype-*`, `iidr-*`) require infrastructure and images to be ready first.

## Modular Makefiles

Each test suite has its own independent Makefile:

| Makefile | Purpose | Example |
|----------|---------|---------|
| `Makefile.common` | Infrastructure & utilities | `make -f Makefile.common base-infra-up` |
| `Makefile.docker` | Docker image builds | `make -f Makefile.docker build-v3` |
| `Makefile.e2e` | E2E Oracle→MariaDB testing | `make -f Makefile.e2e all-dual` |
| `Makefile.datatype` | Data type comparison | `make -f Makefile.datatype all-dual` |
| `Makefile.iidr` | IIDR CDC sink testing | `make -f Makefile.iidr all-dual` |

Run `make -f <Makefile> help` to see available targets for each module.

## Make Targets

### Infrastructure

| Target | Description |
|--------|-------------|
| `base-infra-up` | Create Kind cluster, deploy Kafka/Oracle/MariaDB/PostgreSQL |
| `clean` | Delete cluster and all services |
| `.tools` | Install kubectl, helm, kind |
| `build-v2` / `build-v3` | Build Kafka Connect images |
| `build-all` | Build both Debezium 2.x and 3.x images |
| `port-forward` | Forward all service ports |

### E2E Testing (Oracle → MariaDB)

| Target | Description |
|--------|-------------|
| `e2e-all-v2` / `e2e-all-v3` | Full E2E pipeline |
| `e2e-all-dual` | Full E2E with both versions |
| `e2e-setup` | Set up databases |
| `e2e-run` | Run CDC tests (INSERT/UPDATE/DELETE) |
| `e2e-verify` | Verify target tables |
| `e2e-clean` | Clean up connectors and tables |
| `e2e-register-v2` / `e2e-register-v3` | Register connectors |

### Data Type Testing

| Target | Description |
|--------|-------------|
| `datatype-all-v2` / `datatype-all-v3` | Full datatype test |
| `datatype-all-dual` | Compare 2.x vs 3.x data type handling |
| `datatype-setup` | Set up Oracle and MariaDB |
| `datatype-verify` | Show data from both versions |
| `datatype-clean` | Clean up |

### IIDR Sink Connector (MariaDB & PostgreSQL)

| Target | Description |
|--------|-------------|
| `iidr-all-v2` / `iidr-all-v3` | Full IIDR sink test |
| `iidr-all-dual` | Test with both versions |
| `iidr-setup` | Set up Kafka topic and databases |
| `iidr-register-v2` / `iidr-register-v3` | Register MariaDB sink |
| `iidr-register-pg-v2` / `iidr-register-pg-v3` | Register PostgreSQL sink |
| `iidr-run` | Produce test IIDR events |
| `iidr-verify` | Verify MariaDB and PostgreSQL results |
| `iidr-clean` | Clean up |

### Utilities

| Target | Description |
|--------|-------------|
| `logs-v2` / `logs-v3` | View Kafka Connect logs |
| `status-v2` / `status-v3` | Check connector status |
| `setup-oracle` | Set up Oracle for CDC |
| `setup-mariadb` | Set up MariaDB target |
| `setup-postgres` | Set up PostgreSQL target |

## Web UIs

```bash
make port-forward
```

| UI | URL | Description |
|----|-----|-------------|
| Redpanda Console | http://localhost:8080 | Topics, messages, consumer groups |
| Kafka Connect UI | http://localhost:8000 | Connector management |
| Kafka Connect 2.x | http://localhost:8083 | REST API |
| Kafka Connect 3.x | http://localhost:8084 | REST API |

## Project Structure

```
kafka-dbsync/
├── Makefile             # Main aggregator
├── Makefile.param       # Shared parameters
├── Makefile.common      # Infrastructure & utilities
├── Makefile.docker      # Docker image builds
├── Makefile.e2e         # E2E testing
├── Makefile.datatype    # Datatype testing
├── Makefile.iidr        # IIDR sink testing
├── deployment/          # Helm wrapper charts
│   ├── kafka/           # Kafka (Bitnami)
│   ├── kafka-connect/   # Confluent Kafka Connect + Debezium
│   │   └── docker/      # Custom connector source code
│   ├── oracle/          # Oracle XE 21c
│   ├── mariadb/         # MariaDB
│   └── postgres/        # PostgreSQL
├── hack/                # Test configs and scripts
│   ├── source-debezium/ # Debezium connector configs
│   ├── sink-jdbc/       # JDBC sink configs
│   ├── scripts/         # Test producer scripts
│   └── sql/             # Database setup scripts
└── docs/                # Documentation
```

## Dual Debezium Mode

Run both versions for migration testing:

| Version | Service | Java | Debezium |
|---------|---------|------|----------|
| 2.x | `dbrep-kafka-connect-cp-kafka-connect:8083` | 11 | 2.6.x |
| 3.x | `dbrep-kafka-connect-v3-cp-kafka-connect:8083` | 17 | 3.4.x |

Each version writes to separate target tables (`*_v2`, `*_v3`) for comparison.

## IIDR Sink Connector

Custom sink connector for IBM IIDR CDC events with A_ENTTYP header mapping:

| A_ENTTYP | Operation |
|----------|-----------|
| PT, RR, PX, UR | UPSERT |
| UP, FI, FP | UPSERT |
| DL, DR | DELETE |

Supports both MariaDB and PostgreSQL targets with auto-create and auto-evolve.

See [IIDR Connector README](deployment/kafka-connect/docker/sink/IidrCdcSinkConnector/README.md) for details.

## Troubleshooting

```bash
# Check pods
kubectl get pods -n dev

# View logs
make logs-v2
make logs-v3

# Check connector status
make status-v2
make status-v3

# Reset everything
make clean && make base-infra-up && make build-all
```

| Issue | Solution |
|-------|----------|
| Pods not starting | Increase Docker memory (8GB+ recommended) |
| Connector FAILED | Check logs, verify database connectivity |
| No data replicating | Check connector status, verify topic exists |
| PostgreSQL type errors | Ensure latest connector image is deployed |
| VariableScaleDecimal mapping error | Use `decimal.handling.mode: "double"` or add `"legacy.decimal.handling.strategy": "true"` for Debezium 3.x |

> **Note**: Debezium 3.x changed decimal handling behavior. If you encounter `VariableScaleDecimal` type mapping errors with JDBC sink, either:
> - Set `"decimal.handling.mode": "double"` (may lose precision for large numbers)
> - Set `"decimal.handling.mode": "string"` with `"legacy.decimal.handling.strategy": "true"` (Debezium 3.x only)

## Documentation

- [E2E Test Guide](hack/E2E_TEST_ORACLE_CDC.md)
- [IIDR Sink Connector](deployment/kafka-connect/docker/sink/IidrCdcSinkConnector/README.md)
- [Debezium 2.x vs 3.x Comparison](docs/debezium-2x-vs-3x-comparison.md)

## References

- [Debezium](https://debezium.io/)
- [Confluent Kafka Connect](https://docs.confluent.io/platform/current/connect/)
- [Apache Kafka](https://kafka.apache.org/)
