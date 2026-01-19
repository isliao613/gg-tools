#!/usr/bin/env python3
"""
IIDR CDC Test Event Producer

Produces test CDC events with IIDR-specific headers to Kafka.
Requires: pip install kafka-python

Usage:
    python iidr-test-producer.py --bootstrap-server localhost:9092 --topic iidr.CDC.TEST_ORDERS
"""

import argparse
import json
import time
from datetime import datetime

try:
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
except ImportError:
    print("ERROR: kafka-python not installed. Run: pip install kafka-python")
    exit(1)


def create_topic(bootstrap_server, topic_name):
    """Create Kafka topic if it doesn't exist."""
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_server)
        existing_topics = admin.list_topics()
        if topic_name not in existing_topics:
            topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
            admin.create_topics([topic])
            print(f"[INFO] Created topic: {topic_name}")
        else:
            print(f"[INFO] Topic already exists: {topic_name}")
        admin.close()
    except Exception as e:
        print(f"[WARN] Could not create topic: {e}")


def produce_iidr_events(bootstrap_server, topic):
    """Produce test IIDR CDC events with headers."""

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_server,
        key_serializer=lambda k: json.dumps(k).encode('utf-8') if k else None,
        value_serializer=lambda v: json.dumps(v).encode('utf-8') if v else None,
    )

    timestamp_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.000000000000")

    # Test events: INSERT, UPDATE, DELETE
    test_events = [
        # INSERT events (A_ENTTYP = PT)
        {
            "key": {"ID": 1},
            "value": {"ID": 1, "ORDER_NAME": "Order-001", "AMOUNT": 100.50, "STATUS": "NEW", "CREATED_AT": "2026-01-15T10:00:00"},
            "headers": [
                ("TableName", b"TEST_ORDERS"),
                ("A_ENTTYP", b"PT"),
                ("A_TIMSTAMP", timestamp_now.encode('utf-8'))
            ]
        },
        {
            "key": {"ID": 2},
            "value": {"ID": 2, "ORDER_NAME": "Order-002", "AMOUNT": 200.75, "STATUS": "NEW", "CREATED_AT": "2026-01-15T10:01:00"},
            "headers": [
                ("TableName", b"TEST_ORDERS"),
                ("A_ENTTYP", b"PT"),
                ("A_TIMSTAMP", timestamp_now.encode('utf-8'))
            ]
        },
        {
            "key": {"ID": 3},
            "value": {"ID": 3, "ORDER_NAME": "Order-003", "AMOUNT": 350.00, "STATUS": "PENDING", "CREATED_AT": "2026-01-15T10:02:00"},
            "headers": [
                ("TableName", b"TEST_ORDERS"),
                ("A_ENTTYP", b"PT"),
                ("A_TIMSTAMP", timestamp_now.encode('utf-8'))
            ]
        },
        # UPDATE event (A_ENTTYP = UP)
        {
            "key": {"ID": 2},
            "value": {"ID": 2, "ORDER_NAME": "Order-002-Updated", "AMOUNT": 250.00, "STATUS": "PROCESSING", "CREATED_AT": "2026-01-15T10:01:00"},
            "headers": [
                ("TableName", b"TEST_ORDERS"),
                ("A_ENTTYP", b"UP"),
                ("A_TIMSTAMP", timestamp_now.encode('utf-8'))
            ]
        },
        # DELETE event (A_ENTTYP = DL)
        {
            "key": {"ID": 3},
            "value": None,
            "headers": [
                ("TableName", b"TEST_ORDERS"),
                ("A_ENTTYP", b"DL"),
                ("A_TIMSTAMP", timestamp_now.encode('utf-8'))
            ]
        },
        # Corrupt event - missing A_ENTTYP header
        {
            "key": {"ID": 99},
            "value": {"ID": 99, "ORDER_NAME": "Corrupt-Order", "AMOUNT": 999.99, "STATUS": "BAD", "CREATED_AT": "2026-01-15T10:03:00"},
            "headers": [
                ("TableName", b"TEST_ORDERS"),
                # Missing A_ENTTYP - should go to corrupt events table
                ("A_TIMSTAMP", timestamp_now.encode('utf-8'))
            ]
        },
    ]

    print(f"[INFO] Producing {len(test_events)} test events to topic: {topic}")

    for i, event in enumerate(test_events):
        future = producer.send(
            topic,
            key=event["key"],
            value=event["value"],
            headers=event["headers"]
        )
        result = future.get(timeout=10)

        entry_type = next((h[1].decode() for h in event["headers"] if h[0] == "A_ENTTYP"), "N/A")
        print(f"  [{i+1}] Sent: key={event['key']}, A_ENTTYP={entry_type}, partition={result.partition}, offset={result.offset}")

    producer.flush()
    producer.close()
    print("[OK] All test events produced successfully")


def main():
    parser = argparse.ArgumentParser(description="IIDR CDC Test Event Producer")
    parser.add_argument("--bootstrap-server", default="localhost:9092", help="Kafka bootstrap server")
    parser.add_argument("--topic", default="iidr.CDC.TEST_ORDERS", help="Kafka topic name")
    parser.add_argument("--create-topic", action="store_true", help="Create topic if not exists")
    args = parser.parse_args()

    if args.create_topic:
        create_topic(args.bootstrap_server, args.topic)

    produce_iidr_events(args.bootstrap_server, args.topic)


if __name__ == "__main__":
    main()
