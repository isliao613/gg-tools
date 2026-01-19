# Makefile

.DEFAULT_GOAL := help

include Makefile.param
include Makefile.docker
include Makefile.e2e

# =============================================================================
# Phony Targets
# =============================================================================

.PHONY: help all-v2 all-v3 all-dual clean \
	base-infra-up .check-tools .tools \
	build-v2 build-v3 build-all \
	test test-setup test-run test-verify test-clean test-clean-v2 test-clean-v3 \
	setup-oracle setup-mariadb \
	register-v2 register-v3 verify-v2 verify-v3 \
	logs-v2 logs-v3 status-v2 status-v3 port-forward

help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Main Workflows:"
	@echo "  all-v2               - Full E2E pipeline with Debezium 2.x (base-infra-up, build-v2, test-setup, register-v2, verify-v2)"
	@echo "  all-v3               - Full E2E pipeline with Debezium 3.x (base-infra-up, build-v3, test-setup, register-v3, verify-v3)"
	@echo "  all-dual             - Full E2E pipeline with BOTH Debezium 2.x and 3.x"
	@echo ""
	@echo "Infrastructure Management:"
	@echo "  base-infra-up        - Create Kind cluster and deploy base services"
	@echo "  clean                - Delete Kind cluster and all services"
	@echo ""
	@echo "E2E Testing:"
	@echo "  test-setup           - Set up the databases for testing"
	@echo "  test-run             - Run the CDC tests (insert, update, delete)"
	@echo "  test-verify          - Verify data in both v2 and v3 target tables"
	@echo "  test-clean           - Clean up connectors and tables (both versions)"
	@echo "  test-clean-v2        - Clean up Debezium 2.x connectors only"
	@echo "  test-clean-v3        - Clean up Debezium 3.x connectors only"
	@echo ""
	@echo "Datatype Testing:"
	@echo "  datatype-all-v2      - Full datatype test with Debezium 2.x"
	@echo "  datatype-all-v3      - Full datatype test with Debezium 3.x"
	@echo "  datatype-all-dual    - Full datatype test with both 2.x and 3.x"
	@echo "  datatype-setup       - Set up Oracle and MariaDB for datatype testing"
	@echo "  datatype-register-v2 - Register Debezium 2.x connectors for datatype testing"
	@echo "  datatype-register-v3 - Register Debezium 3.x connectors for datatype testing"
	@echo "  datatype-verify      - Verify data in MariaDB for datatype testing"
	@echo "  datatype-clean       - Clean up connectors and tables for datatype testing"
	@echo ""
	@echo "Utilities:"
	@echo "  logs-v2              - View logs of Debezium 2.x Kafka Connect"
	@echo "  logs-v3              - View logs of Debezium 3.x Kafka Connect"
	@echo "  status-v2            - Check connector status on Debezium 2.x"
	@echo "  status-v3            - Check connector status on Debezium 3.x"
	@echo "  port-forward         - Set up port forwarding"
	@echo ""
	@echo "Dual Kafka Connect Services:"
	@echo "  Debezium 2.x: $(KAFKA_CONNECT_2X_SVC):8083"
	@echo "  Debezium 3.x: $(KAFKA_CONNECT_3X_SVC):8083"

# =============================================================================
# Main Workflows
# =============================================================================

all-v2: base-infra-up build-v2 test-setup register-v2 verify-v2
	@echo "[OK] Full E2E pipeline with Debezium 2.x completed!"

all-v3: base-infra-up build-v3 test-setup register-v3 verify-v3
	@echo "[OK] Full E2E pipeline with Debezium 3.x completed!"

all-dual: base-infra-up build-all test-setup register-v2 verify-v2 register-v3 verify-v3
	@echo "[OK] Full E2E pipeline with Debezium 2.x and 3.x completed!"

# =============================================================================
# Infrastructure Management
# =============================================================================

base-infra-up: .check-tools
	@echo "Creating Kind cluster and deploying services (dual Kafka Connect)..."
	@if kind get clusters 2>/dev/null | grep -q "^$(CLUSTER_NAME)$$"; then \
		echo "Cluster '$(CLUSTER_NAME)' already exists"; \
	else \
		kind create cluster --name $(CLUSTER_NAME) --config hack/kind-config.yaml; \
	fi
	@kubectl cluster-info --context kind-$(CLUSTER_NAME)
	@echo "[OK] Cluster created"
	@echo "[INFO] Creating namespace '$(NAMESPACE)'..."
	-kubectl create ns $(NAMESPACE)
	kubectl config set-context --current --namespace $(NAMESPACE)
	@echo "[INFO] Deploying services..."
	docker pull public.ecr.aws/bitnami/kafka:3.9.0-debian-12-r4
	kind load docker-image public.ecr.aws/bitnami/kafka:3.9.0-debian-12-r4 --name $(CLUSTER_NAME)
	helm repo add bitnami https://charts.bitnami.com/bitnami
	helm dependency build deployment/kafka
	helm upgrade --install dbrep-kafka deployment/kafka -f deployment/kafka/values-kraft.yaml -n $(NAMESPACE)
	helm dependency build deployment/redpanda-console
	helm upgrade --install dbrep-redpanda-console deployment/redpanda-console -f deployment/redpanda-console/values.yaml -n $(NAMESPACE)
	helm dependency build deployment/mariadb
	helm upgrade --install dbrep-mariadb deployment/mariadb -f deployment/mariadb/values.yaml -n $(NAMESPACE)
	-kubectl delete -f helm-chart/minio/minio-dev.yaml -n $(NAMESPACE) 2>/dev/null || true
	kubectl apply -f helm-chart/minio/minio-dev.yaml -n $(NAMESPACE)
	helm dependency build deployment/mssql
	helm upgrade --install dbrep-mssql deployment/mssql -f deployment/mssql/values.yaml -n $(NAMESPACE)
	@echo "[INFO] Deploying Oracle XE 21c..."
	kubectl apply -f deployment/oracle/oracle-free.yaml -n $(NAMESPACE)
	helm dependency build deployment/curl-client
	helm upgrade --install dbrep-curl-client deployment/curl-client -f deployment/curl-client/values.yaml -n $(NAMESPACE)
	@echo "[OK] All services deployed (Debezium 2.x + 3.x)"
	@echo "[INFO] Waiting for pods to be ready..."
	kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=kafka --timeout=300s -n $(NAMESPACE)
	kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=mariadb --timeout=300s -n $(NAMESPACE)
	kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=curl-client --timeout=120s -n $(NAMESPACE)
	kubectl wait --for=condition=ready pod -l app=oracle-db --timeout=300s -n $(NAMESPACE)
	@echo "[OK] All pods ready"

clean:
	@echo "Deleting Kind cluster and services..."
	-kind delete cluster --name $(CLUSTER_NAME)
	@echo "[INFO] Waiting 10s for cluster to be deleted..."
	sleep 10
	-kubectl delete -f deployment/oracle/oracle-free.yaml -n $(NAMESPACE) || true
	-helm uninstall dbrep-mssql -n $(NAMESPACE) || true
	-kubectl delete -f helm-chart/minio/minio-dev.yaml -n $(NAMESPACE) || true
	-helm uninstall dbrep-mariadb -n $(NAMESPACE) || true
	-helm uninstall dbrep-redpanda-console -n $(NAMESPACE) || true
	-helm uninstall dbrep-kafka-connect-ui -n $(NAMESPACE) || true
	-helm uninstall dbrep-kafka-connect -n $(NAMESPACE) || true
	-helm uninstall dbrep-kafka-connect-v3 -n $(NAMESPACE) || true
	-helm uninstall dbrep-kafka -n $(NAMESPACE) || true
	-helm uninstall dbrep-curl-client -n $(NAMESPACE) || true
	-kubectl delete pvc --all -n $(NAMESPACE) || true
	@echo "[OK] All services and PVCs deleted"

.check-tools:
	@command -v kubectl >/dev/null 2>&1 || { echo "[ERROR] kubectl not found. Run 'make .tools' first."; exit 1; }
	@command -v helm >/dev/null 2>&1 || { echo "[ERROR] helm not found. Run 'make .tools' first."; exit 1; }
	@command -v kind >/dev/null 2>&1 || { echo "[ERROR] kind not found. Run 'make .tools' first."; exit 1; }

.tools:
	@echo "[INFO] Installing kubectl..."
	@if ! command -v kubectl >/dev/null 2>&1; then \
		curl -sLO "https://dl.k8s.io/release/v1.28.0/bin/linux/amd64/kubectl" && \
		chmod +x kubectl && \
		sudo mv kubectl /usr/local/bin/; \
	else \
		echo "kubectl already installed"; \
	fi
	@echo "[INFO] Installing helm..."
	@if ! command -v helm >/dev/null 2>&1; then \
		curl -sLo helm.tar.gz "https://get.helm.sh/helm-v3.13.0-linux-amd64.tar.gz" && \
		tar -zxf helm.tar.gz linux-amd64/helm --strip-components 1 && \
		sudo mv helm /usr/local/bin/ && \
		rm helm.tar.gz; \
	else \
		echo "helm already installed"; \
	fi
	@echo "[INFO] Installing kind..."
	@if ! command -v kind >/dev/null 2>&1; then \
		curl -sLo kind "https://kind.sigs.k8s.io/dl/v0.20.0/kind-linux-amd64" && \
		chmod +x kind && \
		sudo mv kind /usr/local/bin/; \
	else \
		echo "kind already installed"; \
	fi
	@echo "[OK] Tools installed"

# =============================================================================
# Datatype Testing
# =============================================================================

datatype-all-v2: base-infra-up build-v2 datatype-setup datatype-register-v2 datatype-verify
	@echo "[OK] Full datatype test with Debezium 2.x completed!"

datatype-all-v3: base-infra-up build-v3 datatype-setup datatype-register-v3 datatype-verify
	@echo "[OK] Full datatype test with Debezium 3.x completed!"

datatype-all-dual: base-infra-up build-v2 build-v3 datatype-setup datatype-register-v2 datatype-register-v3 datatype-verify
	@echo "[OK] Full datatype test with Debezium 2.x and 3.x completed!"

datatype-setup:
	@echo "[INFO] Setting up Oracle and MariaDB for datatype testing..."
	@echo "[INFO] Setting up Oracle..."
	kubectl cp hack/sql/oracle-datatype-test.sql $(ORACLE_POD):/tmp/oracle-datatype-test.sql -n $(NAMESPACE)
	kubectl exec $(ORACLE_POD) -n $(NAMESPACE) -- sqlplus -S sys/oracle@localhost:1521/$(ORACLE_PDB) as sysdba @/tmp/oracle-datatype-test.sql
	@echo "[INFO] Setting up MariaDB..."
	kubectl exec $(MARIADB_POD) -- mysql -h $(MARIADB_HOST) -u$(MARIADB_USER) -p$(MARIADB_PASS) -e \
		"DROP TABLE IF EXISTS target_database.datatype_test_v2; DROP TABLE IF EXISTS target_database.datatype_test_v3;"
	@echo "[OK] Datatype setup completed"

datatype-register-v2:
	@echo "[INFO] Registering Debezium 2.x connectors for datatype testing..."
	kubectl cp hack/source-debezium/oracle-datatype-test-2x.json $(CURL_POD):/tmp/source-connector-datatype-2x.json -n $(NAMESPACE)
	kubectl exec $(CURL_POD) -n $(NAMESPACE) -- curl -sf --max-time 10 -X POST \
		http://$(KAFKA_CONNECT_2X_SVC):8083/connectors \
		-H "Content-Type: application/json" \
		-d @/tmp/source-connector-datatype-2x.json || echo "[ERROR] Failed to register source connector"
	@echo "[INFO] Waiting 30s for initial snapshot..."
	@sleep 30
	kubectl cp hack/sink-jdbc/cdc_oracle_mariadb-datatype-2x.json $(CURL_POD):/tmp/sink-connector-datatype-2x.json -n $(NAMESPACE)
	kubectl exec $(CURL_POD) -n $(NAMESPACE) -- curl -sf --max-time 10 -X POST \
		http://$(KAFKA_CONNECT_2X_SVC):8083/connectors \
		-H "Content-Type: application/json" \
		-d @/tmp/sink-connector-datatype-2x.json || echo "[ERROR] Failed to register sink connector"
	@echo "[INFO] Waiting 10s for sync..."
	@sleep 10
	@echo "[OK] Debezium 2.x connectors registered"

datatype-register-v3:
	@echo "[INFO] Registering Debezium 3.x connectors for datatype testing..."
	kubectl cp hack/source-debezium/oracle-datatype-test-3x.json $(CURL_POD):/tmp/source-connector-datatype-3x.json -n $(NAMESPACE)
	kubectl exec $(CURL_POD) -n $(NAMESPACE) -- curl -sf --max-time 10 -X POST \
		http://$(KAFKA_CONNECT_3X_SVC):8083/connectors \
		-H "Content-Type: application/json" \
		-d @/tmp/source-connector-datatype-3x.json || echo "[ERROR] Failed to register source connector"
	@echo "[INFO] Waiting 30s for initial snapshot..."
	@sleep 30
	kubectl cp hack/sink-jdbc/cdc_oracle_mariadb-datatype-3x.json $(CURL_POD):/tmp/sink-connector-datatype-3x.json -n $(NAMESPACE)
	kubectl exec $(CURL_POD) -n $(NAMESPACE) -- curl -sf --max-time 10 -X POST \
		http://$(KAFKA_CONNECT_3X_SVC):8083/connectors \
		-H "Content-Type: application/json" \
		-d @/tmp/sink-connector-datatype-3x.json || echo "[ERROR] Failed to register sink connector"
	@echo "[INFO] Waiting 10s for sync..."
	@sleep 10
	@echo "[OK] Debezium 3.x connectors registered"

datatype-verify:
	@echo "[INFO] Verifying data in MariaDB for datatype testing..."
	@echo "[INFO] Debezium 2.x data:"
	@kubectl exec $(MARIADB_POD) -- mysql -h $(MARIADB_HOST) -u$(MARIADB_USER) -p$(MARIADB_PASS) -e \
		"SELECT * FROM target_database.datatype_test_v2 ORDER BY ID;" 2>/dev/null || echo "Table not found or empty"
	@echo "[INFO] Debezium 3.x data:"
	@kubectl exec $(MARIADB_POD) -- mysql -h $(MARIADB_HOST) -u$(MARIADB_USER) -p$(MARIADB_PASS) -e \
		"SELECT * FROM target_database.datatype_test_v3 ORDER BY ID;" 2>/dev/null || echo "Table not found or empty"

datatype-clean:
	@echo "[INFO] Cleaning up datatype testing connectors and tables..."
	-kubectl exec $(CURL_POD) -n $(NAMESPACE) -- curl -s -X DELETE \
		http://$(KAFKA_CONNECT_2X_SVC):8083/connectors/source_cdc_oracle_datatype_v2
	-kubectl exec $(CURL_POD) -n $(NAMESPACE) -- curl -s -X DELETE \
		http://$(KAFKA_CONNECT_2X_SVC):8083/connectors/sink_cdc_oracle_datatype_v2
	-kubectl exec $(CURL_POD) -n $(NAMESPACE) -- curl -s -X DELETE \
		http://$(KAFKA_CONNECT_3X_SVC):8083/connectors/source_cdc_oracle_datatype_v3
	-kubectl exec $(CURL_POD) -n $(NAMESPACE) -- curl -s -X DELETE \
		http://$(KAFKA_CONNECT_3X_SVC):8083/connectors/sink_cdc_oracle_datatype_v3
	@echo "DROP TABLE DEMO.DATATYPE_TEST; EXIT;" \
		| kubectl exec -i $(ORACLE_POD) -n $(NAMESPACE) -- sqlplus -S sys/oracle@localhost:1521/$(ORACLE_PDB) as sysdba
	-kubectl exec $(MARIADB_POD) -n $(NAMESPACE) -- mysql -h $(MARIADB_HOST) -u$(MARIADB_USER) -p$(MARIADB_PASS) \
		-e "DROP TABLE IF EXISTS target_database.datatype_test_v2; DROP TABLE IF EXISTS target_database.datatype_test_v3;"
	@echo "[OK] Datatype cleanup completed"

# =============================================================================
# Utilities
# =============================================================================

logs-v2:
	@echo "Viewing Kafka Connect logs (Debezium 2.x)..."
	kubectl logs -f deployment/$(KAFKA_CONNECT_2X_SVC) --tail=100 -n $(NAMESPACE)

logs-v3:
	@echo "Viewing Kafka Connect logs (Debezium 3.x)..."
	kubectl logs -f deployment/$(KAFKA_CONNECT_3X_SVC) --tail=100 -n $(NAMESPACE)

status-v2:
	@echo "[INFO] Checking connector status on Debezium 2.x..."
	@kubectl exec $(CURL_POD) -n $(NAMESPACE) -- curl -s http://$(KAFKA_CONNECT_2X_SVC):8083/connectors | jq .
	@kubectl exec $(CURL_POD) -n $(NAMESPACE) -- curl -s "http://$(KAFKA_CONNECT_2X_SVC):8083/connectors?expand=status" | jq .

status-v3:
	@echo "[INFO] Checking connector status on Debezium 3.x..."
	@kubectl exec $(CURL_POD) -n $(NAMESPACE) -- curl -s http://$(KAFKA_CONNECT_3X_SVC):8083/connectors | jq .
	@kubectl exec $(CURL_POD) -n $(NAMESPACE) -- curl -s "http://$(KAFKA_CONNECT_3X_SVC):8083/connectors?expand=status" | jq .

port-forward:
	@echo "Setting up port forwarding..."
	@echo "Kafka Connect UI: http://localhost:8000"
	@echo "Redpanda Console: http://localhost:8080"
	@echo "Kafka Connect 2.x API: http://localhost:8083"
	@echo "Kafka Connect 3.x API: http://localhost:8084"
	kubectl port-forward svc/dbrep-kafka-connect-ui 8000:80 -n $(NAMESPACE) &
	kubectl port-forward svc/dbrep-redpanda-console 8080:8080 -n $(NAMESPACE) &
	kubectl port-forward svc/$(KAFKA_CONNECT_2X_SVC) 8083:8083 -n $(NAMESPACE) &
	kubectl port-forward svc/$(KAFKA_CONNECT_3X_SVC) 8084:8083 -n $(NAMESPACE) &
	@echo "[INFO] Port forwarding started in background. Press Ctrl+C to stop all."
	wait
