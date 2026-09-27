"""Load the YAML Kafka settings used by the producer and consumer services."""

import os
from pathlib import Path

import yaml
from dotenv import load_dotenv


def load_kafka_settings(path: Path) -> tuple[dict, dict, str, str]:
    """Return producer config, consumer config, produce topic, and consume topic.

    Bootstrap servers come from KAFKA_BOOTSTRAP_SERVERS (Makefile, Compose, or
    the project .env). YAML only holds client/topic settings.
    """
    load_dotenv()
    raw = yaml.safe_load(path.read_text())
    topics = raw.pop("topics")
    produce_topic = topics["produce"]
    consume_topic = topics["consume"]

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    produce_topic = _override_topic(produce_topic)
    consume_topic = _override_topic(consume_topic)

    producer_config = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": raw["client.id"],
    }
    consumer_config = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": raw["group.id"],
        "auto.offset.reset": raw["auto.offset.reset"],
        "enable.auto.commit": bool(raw["enable.auto.commit"]),
    }
    return producer_config, consumer_config, produce_topic, consume_topic


def _override_topic(topic: str) -> str:
    """Apply the Compose topic names when those environment variables are set."""
    if topic == "transactions" and os.environ.get("KAFKA_TRANSACTIONS_TOPIC"):
        return os.environ["KAFKA_TRANSACTIONS_TOPIC"]
    if topic == "config_sync" and os.environ.get("KAFKA_CONTROL_TOPIC"):
        return os.environ["KAFKA_CONTROL_TOPIC"]
    return topic
