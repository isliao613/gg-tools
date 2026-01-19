#!/bin/bash
# Produce test IIDR CDC events to Kafka with headers
# Usage: ./iidr-produce-test-events.sh [kafka-pod] [namespace]

KAFKA_POD="${1:-dbrep-kafka-controller-0}"
NAMESPACE="${2:-dev}"
TOPIC="iidr.CDC.TEST_ORDERS"
BOOTSTRAP_SERVER="localhost:9092"

echo "[INFO] Creating Kafka topic: $TOPIC"
kubectl exec "$KAFKA_POD" -n "$NAMESPACE" -- kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create --if-not-exists \
    --topic "$TOPIC" \
    --partitions 1 \
    --replication-factor 1

echo "[INFO] Producing test IIDR CDC events..."

# Helper function to produce message with headers
produce_message() {
    local key="$1"
    local value="$2"
    local headers="$3"

    # Use kafka-console-producer with headers
    kubectl exec "$KAFKA_POD" -n "$NAMESPACE" -- bash -c "
        echo '$key|$value' | kafka-console-producer.sh \
            --bootstrap-server $BOOTSTRAP_SERVER \
            --topic $TOPIC \
            --property parse.key=true \
            --property key.separator='|' \
            --property parse.headers=true \
            --property headers.delimiter=',' \
            --property headers.separator=':' \
            --property headers.key.separator='=' 2>/dev/null
    "
}

# Since kafka-console-producer doesn't support headers well,
# we'll use a different approach with kafkacat/kcat if available,
# or create a simple Java producer

# Alternative: Use kubectl exec with a heredoc to create a Python script
echo "[INFO] Producing INSERT event (A_ENTTYP=PT)..."
kubectl exec "$KAFKA_POD" -n "$NAMESPACE" -- bash -c "
cat << 'EOF' | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP_SERVER --topic $TOPIC --property parse.key=true --property key.separator='|'
{\"ID\":1}|{\"ID\":1,\"ORDER_NAME\":\"Order-001\",\"AMOUNT\":100.50,\"STATUS\":\"NEW\",\"CREATED_AT\":\"2026-01-15T10:00:00\"}
{\"ID\":2}|{\"ID\":2,\"ORDER_NAME\":\"Order-002\",\"AMOUNT\":200.75,\"STATUS\":\"NEW\",\"CREATED_AT\":\"2026-01-15T10:01:00\"}
{\"ID\":3}|{\"ID\":3,\"ORDER_NAME\":\"Order-003\",\"AMOUNT\":350.00,\"STATUS\":\"NEW\",\"CREATED_AT\":\"2026-01-15T10:02:00\"}
EOF
"

echo "[INFO] Test events produced to topic: $TOPIC"
echo "[INFO] Note: Headers must be added via IIDR configuration or custom producer"
