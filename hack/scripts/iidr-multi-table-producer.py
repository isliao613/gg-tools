#!/usr/bin/env python3
"""
IIDR CDC Multi-Table Test Event Producer

Produces test CDC events for multiple tables with IIDR-specific headers to Kafka.
Each table has its own topic for use with multi-connector setup.

Requires: pip install kafka-python

Usage:
    python iidr-multi-table-producer.py --bootstrap-server localhost:9092
"""

import argparse
import json
from datetime import datetime

try:
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
except ImportError:
    print("ERROR: kafka-python not installed. Run: pip install kafka-python")
    exit(1)


def create_topics(bootstrap_server, topics):
    """Create Kafka topics if they don't exist."""
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_server)
        existing_topics = admin.list_topics()
        new_topics = []
        for topic_name in topics:
            if topic_name not in existing_topics:
                new_topics.append(NewTopic(name=topic_name, num_partitions=1, replication_factor=1))
                print(f"[INFO] Will create topic: {topic_name}")
            else:
                print(f"[INFO] Topic already exists: {topic_name}")
        if new_topics:
            admin.create_topics(new_topics)
        admin.close()
    except Exception as e:
        print(f"[WARN] Could not create topics: {e}")


def produce_multi_table_events(bootstrap_server):
    """Produce test IIDR CDC events for multiple tables."""

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_server,
        key_serializer=lambda k: json.dumps(k).encode('utf-8') if k else None,
        value_serializer=lambda v: json.dumps(v).encode('utf-8') if v else None,
    )

    timestamp_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.000000000000")

    # Define events for multiple tables
    table_events = {
        "iidr.CDC.ORDERS": [
            # INSERT events for ORDERS
            {
                "key": {"ORDER_ID": 1001},
                "value": {"ORDER_ID": 1001, "CUSTOMER_ID": 1, "ORDER_DATE": "2026-01-15", "TOTAL": 150.00, "STATUS": "NEW"},
                "headers": [("TableName", b"ORDERS"), ("A_ENTTYP", b"PT"), ("A_TIMSTAMP", timestamp_now.encode('utf-8'))]
            },
            {
                "key": {"ORDER_ID": 1002},
                "value": {"ORDER_ID": 1002, "CUSTOMER_ID": 2, "ORDER_DATE": "2026-01-15", "TOTAL": 275.50, "STATUS": "NEW"},
                "headers": [("TableName", b"ORDERS"), ("A_ENTTYP", b"PT"), ("A_TIMSTAMP", timestamp_now.encode('utf-8'))]
            },
            # UPDATE event
            {
                "key": {"ORDER_ID": 1001},
                "value": {"ORDER_ID": 1001, "CUSTOMER_ID": 1, "ORDER_DATE": "2026-01-15", "TOTAL": 175.00, "STATUS": "PROCESSING"},
                "headers": [("TableName", b"ORDERS"), ("A_ENTTYP", b"UP"), ("A_TIMSTAMP", timestamp_now.encode('utf-8'))]
            },
        ],
        "iidr.CDC.PRODUCTS": [
            # INSERT events for PRODUCTS
            {
                "key": {"PRODUCT_ID": 101},
                "value": {"PRODUCT_ID": 101, "NAME": "Widget A", "PRICE": 29.99, "STOCK": 100, "CATEGORY": "Electronics"},
                "headers": [("TableName", b"PRODUCTS"), ("A_ENTTYP", b"PT"), ("A_TIMSTAMP", timestamp_now.encode('utf-8'))]
            },
            {
                "key": {"PRODUCT_ID": 102},
                "value": {"PRODUCT_ID": 102, "NAME": "Widget B", "PRICE": 49.99, "STOCK": 50, "CATEGORY": "Electronics"},
                "headers": [("TableName", b"PRODUCTS"), ("A_ENTTYP", b"PT"), ("A_TIMSTAMP", timestamp_now.encode('utf-8'))]
            },
            {
                "key": {"PRODUCT_ID": 103},
                "value": {"PRODUCT_ID": 103, "NAME": "Gadget X", "PRICE": 99.99, "STOCK": 25, "CATEGORY": "Gadgets"},
                "headers": [("TableName", b"PRODUCTS"), ("A_ENTTYP", b"PT"), ("A_TIMSTAMP", timestamp_now.encode('utf-8'))]
            },
            # UPDATE event
            {
                "key": {"PRODUCT_ID": 101},
                "value": {"PRODUCT_ID": 101, "NAME": "Widget A Pro", "PRICE": 34.99, "STOCK": 80, "CATEGORY": "Electronics"},
                "headers": [("TableName", b"PRODUCTS"), ("A_ENTTYP", b"UP"), ("A_TIMSTAMP", timestamp_now.encode('utf-8'))]
            },
            # DELETE event
            {
                "key": {"PRODUCT_ID": 103},
                "value": None,
                "headers": [("TableName", b"PRODUCTS"), ("A_ENTTYP", b"DL"), ("A_TIMSTAMP", timestamp_now.encode('utf-8'))]
            },
        ],
    }

    total_events = sum(len(events) for events in table_events.values())
    print(f"[INFO] Producing {total_events} events across {len(table_events)} tables")

    for topic, events in table_events.items():
        print(f"\n[INFO] Producing {len(events)} events to topic: {topic}")
        for i, event in enumerate(events):
            future = producer.send(
                topic,
                key=event["key"],
                value=event["value"],
                headers=event["headers"]
            )
            result = future.get(timeout=10)

            entry_type = next((h[1].decode() for h in event["headers"] if h[0] == "A_ENTTYP"), "N/A")
            print(f"  [{i+1}] Sent: key={event['key']}, A_ENTTYP={entry_type}, offset={result.offset}")

    producer.flush()
    producer.close()
    print("\n[OK] All multi-table events produced successfully")


def main():
    parser = argparse.ArgumentParser(description="IIDR CDC Multi-Table Test Event Producer")
    parser.add_argument("--bootstrap-server", default="localhost:9092", help="Kafka bootstrap server")
    parser.add_argument("--create-topics", action="store_true", help="Create topics if not exists")
    args = parser.parse_args()

    topics = ["iidr.CDC.ORDERS", "iidr.CDC.PRODUCTS"]

    if args.create_topics:
        create_topics(args.bootstrap_server, topics)

    produce_multi_table_events(args.bootstrap_server)


if __name__ == "__main__":
    main()
