#!/bin/bash
# Big-5 Charset Transformation E2E Test Suite
#
# Tests the LegacyCharsetTransform SMT for converting Big-5 encoded data
# from Oracle US7ASCII databases to proper UTF-8 in Kafka.
#
# Prerequisites:
#   - Docker Compose services running (docker compose up -d)
#   - Oracle and Kafka Connect healthy
#
# Usage:
#   ./tests/big5-tests.sh           # Run all tests
#   ./tests/big5-tests.sh setup     # Setup only (table + data)
#   ./tests/big5-tests.sh cleanup   # Cleanup only

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"

cd "$DOCKER_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_test() { echo -e "${BLUE}[TEST]${NC} $1"; }

CONNECT_URL="http://localhost:8083"

# Test counters
TESTS_PASSED=0
TESTS_FAILED=0

# ============================================================================
# Helper Functions
# ============================================================================

# Run sqlplus command inside the Oracle container
run_sqlplus() {
    docker compose exec -T oracle sqlplus -S "$@"
}

# Run curl command inside the Kafka Connect container
run_curl() {
    docker compose exec -T kafka-connect curl "$@"
}

wait_for_oracle() {
    log_info "Waiting for Oracle to be ready..."
    local max_attempts=30
    local attempt=0
    until run_sqlplus c##dbzuser/dbz@//localhost:1521/XEPDB1 <<< "SELECT 1 FROM DUAL;" > /dev/null 2>&1; do
        attempt=$((attempt + 1))
        if [ $attempt -ge $max_attempts ]; then
            log_error "Oracle not ready after $max_attempts attempts"
            return 1
        fi
        echo -n "."
        sleep 2
    done
    echo ""
    log_info "Oracle is ready"
}

wait_for_connect() {
    log_info "Waiting for Kafka Connect to be ready..."
    local max_attempts=30
    local attempt=0
    until run_curl -sf "$CONNECT_URL/connectors" > /dev/null 2>&1; do
        attempt=$((attempt + 1))
        if [ $attempt -ge $max_attempts ]; then
            log_error "Kafka Connect not ready after $max_attempts attempts"
            return 1
        fi
        echo -n "."
        sleep 2
    done
    echo ""
    log_info "Kafka Connect is ready"
}

delete_connector() {
    local name=$1
    if run_curl -sf "$CONNECT_URL/connectors/$name" > /dev/null 2>&1; then
        log_info "Deleting existing connector: $name"
        run_curl -sf -X DELETE "$CONNECT_URL/connectors/$name" > /dev/null
        sleep 2
    fi
}

deploy_connector() {
    local config_file=$1
    local connector_name
    connector_name=$(jq -r '.name' "$config_file")

    delete_connector "$connector_name"

    # Convert host path to container path (connectors/ -> /connectors/)
    local container_path="/connectors/$(basename "$config_file")"

    log_info "Deploying connector: $connector_name"
    local response
    response=$(run_curl -sf -X POST \
        -H "Content-Type: application/json" \
        -d @"$container_path" \
        "$CONNECT_URL/connectors")

    if [ $? -ne 0 ]; then
        log_error "Failed to deploy connector: $connector_name"
        return 1
    fi

    # Wait for connector to start
    sleep 5

    # Check connector status
    local status
    status=$(run_curl -sf "$CONNECT_URL/connectors/$connector_name/status" | jq -r '.connector.state')
    if [ "$status" != "RUNNING" ]; then
        log_error "Connector $connector_name is not running (state: $status)"
        run_curl -sf "$CONNECT_URL/connectors/$connector_name/status" | jq .
        return 1
    fi

    log_info "Connector $connector_name is running"
}

consume_messages() {
    local topic=$1
    local count=${2:-10}
    local timeout=${3:-30000}

    docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 \
        --topic "$topic" \
        --from-beginning \
        --max-messages "$count" \
        --timeout-ms "$timeout" 2>/dev/null
}

assert_contains() {
    local haystack=$1
    local needle=$2
    local test_name=$3

    if echo "$haystack" | grep -q "$needle"; then
        log_test "PASS: $test_name"
        TESTS_PASSED=$((TESTS_PASSED + 1))
        return 0
    else
        log_test "FAIL: $test_name (expected to find: $needle)"
        TESTS_FAILED=$((TESTS_FAILED + 1))
        return 1
    fi
}

assert_not_contains() {
    local haystack=$1
    local needle=$2
    local test_name=$3

    if ! echo "$haystack" | grep -q "$needle"; then
        log_test "PASS: $test_name"
        TESTS_PASSED=$((TESTS_PASSED + 1))
        return 0
    else
        log_test "FAIL: $test_name (expected NOT to find: $needle)"
        TESTS_FAILED=$((TESTS_FAILED + 1))
        return 1
    fi
}

# ============================================================================
# Setup Functions
# ============================================================================

create_big5_table() {
    log_info "Creating big5_test table..."

    run_sqlplus c##dbzuser/dbz@//localhost:1521/XEPDB1 << 'EOF'
-- Drop table if exists
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE big5_test';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
/

-- Create test table
CREATE TABLE big5_test (
    id NUMBER PRIMARY KEY,
    name VARCHAR2(100),
    address VARCHAR2(200),
    description VARCHAR2(500)
);

-- Enable supplemental logging for CDC
ALTER TABLE big5_test ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

EXIT;
EOF

    log_info "big5_test table created"
}

insert_big5_data() {
    log_info "Inserting Big-5 test data..."

    # Big-5 byte mappings used:
    # 測 = 0xB4 0xFA = CHR(180)||CHR(250)
    # 試 = 0xB8 0xD5 = CHR(184)||CHR(213)
    # 台 = 0xA5 0x78 = CHR(165)||CHR(120)
    # 北 = 0xA5 0x5F = CHR(165)||CHR(95)
    # 市 = 0xA5 0xAB = CHR(165)||CHR(171)
    # 中 = 0xA4 0xA4 = CHR(164)||CHR(164)
    # 文 = 0xA4 0xE5 = CHR(164)||CHR(229)

    run_sqlplus c##dbzuser/dbz@//localhost:1521/XEPDB1 << 'EOF'
SET SERVEROUTPUT ON

-- Clear existing data
DELETE FROM big5_test;

-- Record 1: name=測試 address=台北市 description=測試中文
-- Using CHR() with raw Big-5 byte values (decimal)
INSERT INTO big5_test (id, name, address, description) VALUES (
    1,
    CHR(180)||CHR(250)||CHR(184)||CHR(213),
    CHR(165)||CHR(120)||CHR(165)||CHR(95)||CHR(165)||CHR(171),
    CHR(180)||CHR(250)||CHR(184)||CHR(213)||CHR(164)||CHR(164)||CHR(164)||CHR(229)
);

-- Record 2: name=你好 address=世界 description=你好世界
-- Using CONVERT() to convert UTF-8 to Big-5 bytes
INSERT INTO big5_test (id, name, address, description) VALUES (
    2,
    CONVERT('你好', 'ZHT16MSWIN950', 'AL32UTF8'),
    CONVERT('世界', 'ZHT16MSWIN950', 'AL32UTF8'),
    CONVERT('你好世界', 'ZHT16MSWIN950', 'AL32UTF8')
);

-- Record 3: CJK Punctuation (U+3000-U+303F) - Big-5 supported
-- 、= 0xA1 0x42  。= 0xA1 0x43
INSERT INTO big5_test (id, name, address, description) VALUES (
    3,
    CONVERT('標點、符號。', 'ZHT16MSWIN950', 'AL32UTF8'),
    CONVERT('「引號」', 'ZHT16MSWIN950', 'AL32UTF8'),
    CONVERT('【括號】', 'ZHT16MSWIN950', 'AL32UTF8')
);

-- Record 4: Bopomofo (U+3100-U+312F) - Big-5 supported
INSERT INTO big5_test (id, name, address, description) VALUES (
    4,
    CONVERT('ㄅㄆㄇㄈ', 'ZHT16MSWIN950', 'AL32UTF8'),
    CONVERT('ㄉㄊㄋㄌ', 'ZHT16MSWIN950', 'AL32UTF8'),
    CONVERT('注音符號', 'ZHT16MSWIN950', 'AL32UTF8')
);

-- Record 5: Full-width ASCII (U+FF00-U+FFEF) - Big-5 supported
INSERT INTO big5_test (id, name, address, description) VALUES (
    5,
    CONVERT('ＡＢＣＤ', 'ZHT16MSWIN950', 'AL32UTF8'),
    CONVERT('１２３４', 'ZHT16MSWIN950', 'AL32UTF8'),
    CONVERT('全形字母', 'ZHT16MSWIN950', 'AL32UTF8')
);

-- Record 6: Mixed ASCII and Big-5
INSERT INTO big5_test (id, name, address, description) VALUES (
    6,
    'Customer-' || CHR(165)||CHR(120)||CHR(165)||CHR(95),
    CONVERT('台北市', 'ZHT16MSWIN950', 'AL32UTF8') || '-District',
    'Mixed content with ' || CHR(164)||CHR(164)||CHR(164)||CHR(229)
);

COMMIT;

SELECT 'Inserted ' || COUNT(*) || ' records' AS result FROM big5_test;
EXIT;
EOF

    log_info "Big-5 test data inserted"
}

# ============================================================================
# Cleanup Functions
# ============================================================================

cleanup_connectors() {
    log_info "Cleaning up connectors..."
    delete_connector "oracle-source-smt"
    delete_connector "oracle-source"
}

cleanup_table() {
    log_info "Cleaning up big5_test table..."

    run_sqlplus c##dbzuser/dbz@//localhost:1521/XEPDB1 << 'EOF' 2>/dev/null || true
DROP TABLE big5_test;
EXIT;
EOF

    log_info "big5_test table dropped"
}

cleanup_topics() {
    log_info "Cleaning up Kafka topics..."

    # Delete the CDC topic
    docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --delete \
        --topic "oracle.C__DBZUSER.BIG5_TEST" 2>/dev/null || true

    log_info "Kafka topics cleaned"
}

full_cleanup() {
    cleanup_connectors
    cleanup_topics
    cleanup_table
}

# ============================================================================
# Test Functions
# ============================================================================

test_smt_transforms_big5() {
    log_info "=== Test: SMT transforms Big-5 to UTF-8 ==="

    # Clean up any existing connector and topic
    delete_connector "oracle-source-smt"
    delete_connector "oracle-source"
    cleanup_topics

    # Deploy connector with SMT
    deploy_connector "connectors/oracle-source-with-smt.json"

    # Wait for data to be captured
    log_info "Waiting for CDC to capture data..."
    sleep 10

    # Consume messages (we inserted 6 records)
    local messages
    messages=$(consume_messages "oracle.C__DBZUSER.BIG5_TEST" 6)

    # Test assertions - check for properly decoded UTF-8
    assert_contains "$messages" "測試" "SMT decodes 測試 correctly"
    assert_contains "$messages" "台北市" "SMT decodes 台北市 correctly"
    assert_contains "$messages" "中文" "SMT decodes 中文 correctly"
    assert_contains "$messages" "你好" "SMT decodes 你好 correctly"
    assert_contains "$messages" "世界" "SMT decodes 世界 correctly"
    assert_contains "$messages" "ㄅㄆㄇㄈ" "SMT decodes Bopomofo correctly"
    assert_contains "$messages" "ＡＢＣＤ" "SMT decodes Full-width ASCII correctly"

    # Should NOT contain halfwidth characters (raw/garbled)
    assert_not_contains "$messages" "ﾴ￺" "No halfwidth characters in output"
}

test_without_smt_shows_halfwidth() {
    log_info "=== Test: Without SMT shows halfwidth characters ==="

    # Clean up previous connector and topic
    delete_connector "oracle-source-smt"
    delete_connector "oracle-source"
    cleanup_topics

    # Deploy connector without SMT
    deploy_connector "connectors/oracle-source.json"

    # Wait for data to be captured
    log_info "Waiting for CDC to capture data..."
    sleep 10

    # Consume messages (we inserted 6 records)
    local messages
    messages=$(consume_messages "oracle.C__DBZUSER.BIG5_TEST" 6)

    # Without SMT, data should contain halfwidth characters
    # Big-5 測試 (0xB4 0xFA 0xB8 0xD5) becomes ﾴ￺ﾸￕ
    assert_contains "$messages" "ﾴ" "Without SMT shows halfwidth character ﾴ"

    # Should NOT contain properly decoded Chinese
    assert_not_contains "$messages" "測試" "Without SMT does not decode properly"
}

# ============================================================================
# Main
# ============================================================================

print_summary() {
    echo ""
    echo "============================================"
    echo "Test Summary"
    echo "============================================"
    echo -e "Passed: ${GREEN}$TESTS_PASSED${NC}"
    echo -e "Failed: ${RED}$TESTS_FAILED${NC}"
    echo "============================================"

    if [ $TESTS_FAILED -gt 0 ]; then
        return 1
    fi
    return 0
}

run_setup() {
    wait_for_oracle
    wait_for_connect
    create_big5_table
    insert_big5_data
}

run_all_tests() {
    log_info "Starting Big-5 E2E Test Suite"
    echo ""

    # Setup
    run_setup

    echo ""

    # Run tests
    test_smt_transforms_big5

    echo ""

    test_without_smt_shows_halfwidth

    echo ""

    # Cleanup
    full_cleanup

    # Summary
    print_summary
}

case "${1:-all}" in
    setup)
        run_setup
        ;;
    cleanup)
        full_cleanup
        ;;
    test-smt)
        wait_for_connect
        test_smt_transforms_big5
        print_summary
        ;;
    test-no-smt)
        wait_for_connect
        test_without_smt_shows_halfwidth
        print_summary
        ;;
    all)
        run_all_tests
        ;;
    *)
        echo "Usage: $0 [setup|cleanup|test-smt|test-no-smt|all]"
        echo ""
        echo "Commands:"
        echo "  setup       - Create table and insert test data"
        echo "  cleanup     - Remove connectors, topics, and table"
        echo "  test-smt    - Test SMT transformation only"
        echo "  test-no-smt - Test without SMT only"
        echo "  all         - Run complete test suite (default)"
        exit 1
        ;;
esac
