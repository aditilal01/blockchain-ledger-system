"""
kafka_producer.py
Blockchain Transaction Ledger System
---------------------------------------------------
Simulates real-time transaction events using Kafka producer.
Partitioned by network (mainnet/testnet) for parallel consumption.

SETUP (run before using):
  pip install kafka-python
  docker run -d -p 2181:2181 zookeeper
  docker run -d -p 9092:9092 \
    -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
    confluentinc/cp-kafka

NOTE: Kafka producer/consumer are IMPLEMENTED but require Docker.
      The rest of the pipeline works without Kafka.
"""

import json
import hashlib
import random
import time
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

TOPIC            = "blockchain_transactions"
BOOTSTRAP_SERVER = "localhost:9092"
WALLETS          = [f"0x{hashlib.md5(str(i).encode()).hexdigest()[:40]}" for i in range(50)]


def build_transaction_event() -> dict:
    sender    = random.choice(WALLETS)
    receiver  = random.choice([w for w in WALLETS if w != sender])
    amount    = round(random.uniform(0.001, 100.0), 8)
    timestamp = datetime.utcnow().isoformat()
    tx_hash   = "0x" + hashlib.sha256(
        f"{sender}{receiver}{amount}{timestamp}{random.getrandbits(16)}".encode()
    ).hexdigest()

    return {
        "tx_hash":   tx_hash,
        "sender":    sender,
        "receiver":  receiver,
        "amount":    amount,
        "fee":       round(amount * 0.005, 8),
        "status":    "PENDING",
        "network":   random.choice(["mainnet", "testnet"]),
        "timestamp": timestamp,
    }


def run_producer(num_events: int = 1000, delay_ms: int = 10):
    """
    Produce `num_events` transaction events to Kafka topic.
    Uses `network` as partition key for ordered per-network delivery.
    """
    try:
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=[BOOTSTRAP_SERVER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            # Reliability settings
            acks="all",            # wait for all replicas
            retries=3,
            linger_ms=5,           # batch small messages
        )

        print(f"[✓] Kafka producer connected → topic: {TOPIC}")
        sent = 0

        for i in range(num_events):
            event = build_transaction_event()
            producer.send(
                topic=TOPIC,
                key=event["network"],           # partition key
                value=event,
            )
            sent += 1
            if sent % 100 == 0:
                print(f"  Sent {sent}/{num_events} events ...")
            time.sleep(delay_ms / 1000)

        producer.flush()
        producer.close()
        print(f"[✓] Producer finished: {sent} events sent to '{TOPIC}'")

    except ImportError:
        print("[!] kafka-python not installed. Run: pip install kafka-python")
        print("[!] Falling back to file-based simulation ...")
        _simulate_producer(num_events)


def _simulate_producer(num_events: int):
    """File-based fallback when Kafka is not available."""
    import os
    os.makedirs("data/raw", exist_ok=True)
    events = [build_transaction_event() for _ in range(num_events)]
    with open("data/raw/streaming_simulation.json", "w") as f:
        json.dump(events, f, indent=2)
    print(f"[✓] Simulated {num_events} streaming events → data/raw/streaming_simulation.json")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_producer(num_events=500, delay_ms=5)
