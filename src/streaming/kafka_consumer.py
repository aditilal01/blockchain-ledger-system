"""
kafka_consumer.py
Blockchain Transaction Ledger System
---------------------------------------------------
Consumes transaction events from Kafka topic.
Demonstrates: consumer groups, offset management, partition assignment.
"""

import json
import logging
import os

logger = logging.getLogger(__name__)

TOPIC            = "blockchain_transactions"
BOOTSTRAP_SERVER = "localhost:9092"
GROUP_ID         = "ledger-pipeline-consumers"


def process_event(event: dict) -> dict:
    """Validate and enrich incoming streaming event."""
    event["amount"] = max(float(event.get("amount", 0)), 0)
    if event["amount"] == 0:
        event["status"] = "INVALID"
    elif event["amount"] > 50:
        event["flag"] = "HIGH_VALUE"
    return event


def run_consumer(max_messages: int = 500):
    """
    Consume from Kafka with manual offset commit for exactly-once semantics.
    Partition assignment follows round-robin across consumer group members.
    """
    try:
        from kafka import KafkaConsumer
        from kafka import TopicPartition

        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[BOOTSTRAP_SERVER],
            group_id=GROUP_ID,
            auto_offset_reset="earliest",     # start from beginning if no committed offset
            enable_auto_commit=False,         # manual offset control
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            max_poll_records=100,
        )

        print(f"[✓] Consumer '{GROUP_ID}' subscribed to '{TOPIC}'")
        consumed, saved = 0, []

        for message in consumer:
            event = process_event(message.value)
            saved.append(event)
            consumed += 1

            # Manual commit every 100 messages (offset management)
            if consumed % 100 == 0:
                consumer.commit()
                print(f"  Consumed {consumed} | partition={message.partition} offset={message.offset}")

            if consumed >= max_messages:
                break

        consumer.commit()   # final commit
        consumer.close()

        # Persist consumed events
        _save_events(saved)
        print(f"[✓] Consumer finished: {consumed} messages processed")

    except ImportError:
        print("[!] kafka-python not installed. Using simulated file consumer ...")
        _simulate_consumer()


def _save_events(events: list):
    os.makedirs("data/processed", exist_ok=True)
    with open("data/processed/streamed_events.json", "w") as f:
        json.dump(events, f)
    print(f"[✓] Saved {len(events)} streamed events → data/processed/streamed_events.json")


def _simulate_consumer():
    src = "data/raw/streaming_simulation.json"
    if not os.path.exists(src):
        print("[!] Run kafka_producer.py first")
        return
    with open(src) as f:
        events = json.load(f)
    processed = [process_event(e) for e in events]
    _save_events(processed)
    print(f"[✓] Simulated consumer processed {len(processed)} events")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_consumer()
