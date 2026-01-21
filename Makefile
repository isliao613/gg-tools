# Makefile
#
# Main aggregator Makefile that delegates to sub-Makefiles.
# This provides a unified interface for all operations.
#
# Each sub-Makefile can also be run independently:
#   make -f Makefile.common <target>   # Infrastructure and utilities
#   make -f Makefile.docker <target>   # Docker image builds
#   make -f Makefile.e2e <target>      # E2E testing
#   make -f Makefile.datatype <target> # Datatype testing
#   make -f Makefile.iidr <target>     # IIDR CDC sink testing
#
# ==============================================================================

.DEFAULT_GOAL := help

# Include shared parameters for display in help
include Makefile.param

# =============================================================================
# Help
# =============================================================================

.PHONY: help
help:
	@echo "Kafka DBSync - CDC Testing Framework"
	@echo "====================================="
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "You can also run each Makefile independently:"
	@echo "  make -f Makefile.common help    # Infrastructure and utilities"
	@echo "  make -f Makefile.docker help    # Docker image builds"
	@echo "  make -f Makefile.e2e help       # E2E testing"
	@echo "  make -f Makefile.datatype help  # Datatype testing"
	@echo "  make -f Makefile.iidr help      # IIDR CDC sink testing"
	@echo ""
	@echo "=== Infrastructure (Makefile.common) ==="
	@echo "  base-infra-up        - Create Kind cluster and deploy base services"
	@echo "  clean                - Delete Kind cluster and all services"
	@echo "  .tools               - Install required tools (kubectl, helm, kind)"
	@echo "  setup-oracle         - Set up Oracle XE 21c for CDC"
	@echo "  setup-mariadb        - Set up MariaDB target database"
	@echo "  setup-postgres       - Set up PostgreSQL target database"
	@echo "  logs-v2/logs-v3      - View Kafka Connect logs"
	@echo "  status-v2/status-v3  - Check connector status"
	@echo "  port-forward         - Set up port forwarding"
	@echo ""
	@echo "=== Docker Builds (Makefile.docker) ==="
	@echo "  build-v2             - Build Kafka Connect with Debezium 2.x"
	@echo "  build-v3             - Build Kafka Connect with Debezium 3.x"
	@echo "  build-all            - Build both Debezium 2.x and 3.x"
	@echo ""
	@echo "=== E2E Testing (Makefile.e2e) ==="
	@echo "  e2e-all-v2           - Full E2E pipeline with Debezium 2.x"
	@echo "  e2e-all-v3           - Full E2E pipeline with Debezium 3.x"
	@echo "  e2e-all-dual         - Full E2E pipeline with both versions"
	@echo "  e2e-setup            - Set up databases for testing"
	@echo "  e2e-run              - Run CDC tests (insert, update, delete)"
	@echo "  e2e-verify           - Verify data in target tables"
	@echo "  e2e-clean            - Clean up connectors and tables"
	@echo "  e2e-register-v2/v3   - Register connectors"
	@echo ""
	@echo "=== Datatype Testing (Makefile.datatype) ==="
	@echo "  datatype-all-v2      - Full datatype test with Debezium 2.x"
	@echo "  datatype-all-v3      - Full datatype test with Debezium 3.x"
	@echo "  datatype-all-dual    - Full datatype test with both versions"
	@echo "  datatype-setup       - Set up Oracle and MariaDB for datatype testing"
	@echo "  datatype-register-v2 - Register Debezium 2.x connectors"
	@echo "  datatype-register-v3 - Register Debezium 3.x connectors"
	@echo "  datatype-verify      - Verify data in MariaDB"
	@echo "  datatype-clean       - Clean up connectors and tables"
	@echo ""
	@echo "=== IIDR CDC Sink Testing (Makefile.iidr) ==="
	@echo "  iidr-all-v2          - Full IIDR test with Debezium 2.x"
	@echo "  iidr-all-v3          - Full IIDR test with Debezium 3.x"
	@echo "  iidr-all-dual        - Full IIDR test with both versions"
	@echo "  iidr-setup           - Set up Kafka topic and databases"
	@echo "  iidr-register-v2/v3  - Register IIDR MariaDB sink connectors"
	@echo "  iidr-register-pg-v2  - Register IIDR PostgreSQL sink on 2.x"
	@echo "  iidr-register-pg-v3  - Register IIDR PostgreSQL sink on 3.x"
	@echo "  iidr-run             - Produce test IIDR CDC events"
	@echo "  iidr-verify          - Verify data in MariaDB and PostgreSQL"
	@echo "  iidr-status-v2/v3    - Check IIDR connector status"
	@echo "  iidr-clean           - Clean up IIDR test resources"
	@echo ""
	@echo "Dual Kafka Connect Services:"
	@echo "  Debezium 2.x: $(KAFKA_CONNECT_2X_SVC):8083"
	@echo "  Debezium 3.x: $(KAFKA_CONNECT_3X_SVC):8083"

# =============================================================================
# Infrastructure (Makefile.common)
# =============================================================================

.PHONY: base-infra-up clean .tools .check-tools setup-oracle setup-mariadb setup-postgres \
	logs-v2 logs-v3 status-v2 status-v3 port-forward

base-infra-up:
	@$(MAKE) -f Makefile.common base-infra-up

clean:
	@$(MAKE) -f Makefile.common clean

.tools:
	@$(MAKE) -f Makefile.common .tools

.check-tools:
	@$(MAKE) -f Makefile.common .check-tools

setup-oracle:
	@$(MAKE) -f Makefile.common setup-oracle

setup-mariadb:
	@$(MAKE) -f Makefile.common setup-mariadb

setup-postgres:
	@$(MAKE) -f Makefile.common setup-postgres

logs-v2:
	@$(MAKE) -f Makefile.common logs-v2

logs-v3:
	@$(MAKE) -f Makefile.common logs-v3

status-v2:
	@$(MAKE) -f Makefile.common status-v2

status-v3:
	@$(MAKE) -f Makefile.common status-v3

port-forward:
	@$(MAKE) -f Makefile.common port-forward

# =============================================================================
# Docker Builds (Makefile.docker)
# =============================================================================

.PHONY: build-v2 build-v3 build-all

build-v2:
	@$(MAKE) -f Makefile.docker build-v2

build-v3:
	@$(MAKE) -f Makefile.docker build-v3

build-all:
	@$(MAKE) -f Makefile.docker build-all

# =============================================================================
# E2E Testing (Makefile.e2e)
# =============================================================================

.PHONY: e2e-all-v2 e2e-all-v3 e2e-all-dual \
	e2e-setup e2e-run e2e-verify e2e-clean e2e-clean-v2 e2e-clean-v3 \
	e2e-register-v2 e2e-register-v3

e2e-all-v2:
	@$(MAKE) -f Makefile.e2e all-v2

e2e-all-v3:
	@$(MAKE) -f Makefile.e2e all-v3

e2e-all-dual:
	@$(MAKE) -f Makefile.e2e all-dual

e2e-setup:
	@$(MAKE) -f Makefile.e2e test-setup

e2e-run:
	@$(MAKE) -f Makefile.e2e test-run

e2e-verify:
	@$(MAKE) -f Makefile.e2e test-verify

e2e-clean:
	@$(MAKE) -f Makefile.e2e test-clean

e2e-clean-v2:
	@$(MAKE) -f Makefile.e2e test-clean-v2

e2e-clean-v3:
	@$(MAKE) -f Makefile.e2e test-clean-v3

e2e-register-v2:
	@$(MAKE) -f Makefile.e2e register-v2

e2e-register-v3:
	@$(MAKE) -f Makefile.e2e register-v3

# =============================================================================
# Datatype Testing (Makefile.datatype)
# =============================================================================

.PHONY: datatype-all-v2 datatype-all-v3 datatype-all-dual \
	datatype-setup datatype-register-v2 datatype-register-v3 datatype-verify datatype-clean

datatype-all-v2:
	@$(MAKE) -f Makefile.datatype all-v2

datatype-all-v3:
	@$(MAKE) -f Makefile.datatype all-v3

datatype-all-dual:
	@$(MAKE) -f Makefile.datatype all-dual

datatype-setup:
	@$(MAKE) -f Makefile.datatype setup

datatype-register-v2:
	@$(MAKE) -f Makefile.datatype register-v2

datatype-register-v3:
	@$(MAKE) -f Makefile.datatype register-v3

datatype-verify:
	@$(MAKE) -f Makefile.datatype verify

datatype-clean:
	@$(MAKE) -f Makefile.datatype clean

# =============================================================================
# IIDR CDC Sink Testing (Makefile.iidr)
# =============================================================================

.PHONY: iidr-all-v2 iidr-all-v3 iidr-all-dual \
	iidr-setup iidr-register-v2 iidr-register-v3 iidr-register-pg-v2 iidr-register-pg-v3 \
	iidr-run iidr-verify iidr-status-v2 iidr-status-v3 iidr-clean

iidr-all-v2:
	@$(MAKE) -f Makefile.iidr all-v2

iidr-all-v3:
	@$(MAKE) -f Makefile.iidr all-v3

iidr-all-dual:
	@$(MAKE) -f Makefile.iidr all-dual

iidr-setup:
	@$(MAKE) -f Makefile.iidr setup

iidr-register-v2:
	@$(MAKE) -f Makefile.iidr register-v2

iidr-register-v3:
	@$(MAKE) -f Makefile.iidr register-v3

iidr-register-pg-v2:
	@$(MAKE) -f Makefile.iidr register-pg-v2

iidr-register-pg-v3:
	@$(MAKE) -f Makefile.iidr register-pg-v3

iidr-run:
	@$(MAKE) -f Makefile.iidr run

iidr-verify:
	@$(MAKE) -f Makefile.iidr verify

iidr-status-v2:
	@$(MAKE) -f Makefile.iidr status-v2

iidr-status-v3:
	@$(MAKE) -f Makefile.iidr status-v3

iidr-clean:
	@$(MAKE) -f Makefile.iidr clean
