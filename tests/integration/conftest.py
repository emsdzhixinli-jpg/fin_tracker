"""Shared fixtures for real-Kafka integration tests."""

import os
import threading
import time
import uuid
from pathlib import Path

import numpy as np
import pytest
from confluent_kafka.admin import AdminClient

from services.consumer import TransactionConsumer
from services.producer import TransactionProducer
from tests.integration.support import DATA_DIR
from tests.integration.support import mirror_config_for_calc
from tests.integration.support import sample_config
from tests.integration.support import wait_until
from utils import kafka_config
from utils.constants import JSON_FILENAME


def _wait_for_transactions_consumer(
    producer: TransactionProducer,
    consumer: TransactionConsumer,
    timeout: float = 30.0,
) -> bool:
    """Wait for group assignment, then confirm the listener receives a notice."""
    # With auto.offset.reset=latest, messages sent before assignment are dropped.
    time.sleep(2)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        before = len(consumer._queued_notices)
        producer.notify_consume()
        if wait_until(lambda b=before: len(consumer._queued_notices) > b, timeout=5):
            return True
        time.sleep(0.5)
    return False


@pytest.fixture(scope="session")
def kafka_bootstrap() -> str:
    """Skip integration tests when no broker is reachable."""
    servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    try:
        AdminClient({"bootstrap.servers": servers}).list_topics(timeout=5)
    except Exception as exc:
        pytest.skip(f"Kafka not available at {servers}: {exc}")
    return servers


@pytest.fixture
def unique_kafka_groups(monkeypatch):
    """Fresh consumer group ids so each test only sees new messages."""
    suffix = uuid.uuid4().hex[:8]
    original = kafka_config.load_kafka_settings

    def wrapped(path: Path):
        producer_cfg, consumer_cfg, produce_topic, consume_topic = original(path)
        # Only messages produced after the listener starts should affect assertions.
        consumer_cfg = {
            **consumer_cfg,
            "group.id": f"int-{'producer' if 'producer' in path.name else 'consumer'}-{suffix}",
            "auto.offset.reset": "latest",
        }
        return producer_cfg, consumer_cfg, produce_topic, consume_topic

    monkeypatch.setattr("services.producer.kafka_config.load_kafka_settings", wrapped)
    monkeypatch.setattr("services.consumer.kafka_config.load_kafka_settings", wrapped)


@pytest.fixture
def config_path(tmp_path: Path) -> Path:
    """Shared JSON path used by both producer and consumer in a test."""
    path = tmp_path / JSON_FILENAME
    path.write_text(sample_config().model_dump_json(indent=4))
    return path


@pytest.fixture
def restore_data_config():
    """Restore data/simulation_config.json after tests that mirror calc() input."""
    path = DATA_DIR / JSON_FILENAME
    backup = path.read_text() if path.is_file() else None
    yield path
    if backup is not None:
        path.write_text(backup)


@pytest.fixture
def kafka_services(
    kafka_bootstrap,
    unique_kafka_groups,
    config_path: Path,
    restore_data_config,
):
    """Running producer config listener + consumer transactions listener."""
    mirror_config_for_calc(sample_config(), restore_data_config)
    np.random.seed(42)

    producer = TransactionProducer(config=sample_config(), config_path=config_path)
    consumer = TransactionConsumer(config=sample_config(), config_path=config_path)

    producer._stop.clear()
    consumer._stop.clear()

    producer_thread = threading.Thread(
        target=producer.listen_config_sync,
        name="it-producer-config",
        daemon=True,
    )
    consumer_thread = threading.Thread(
        target=consumer.listen_transactions,
        name="it-consumer-tx",
        daemon=True,
    )
    producer_thread.start()
    consumer_thread.start()
    # Wait until the transactions consumer is assigned and can read new messages.
    consumer.show_notice = False
    if not _wait_for_transactions_consumer(producer, consumer, timeout=30):
        pytest.fail(
            "Consumer did not receive a probe notice on the transactions topic. "
            "Is Kafka running (make kafka)?"
        )
    consumer._queued_notices.clear()

    yield producer, consumer

    producer._stop.set()
    consumer._stop.set()
    producer_thread.join(timeout=10)
    consumer_thread.join(timeout=10)
    producer._kafka_producer.flush(5)
    consumer._kafka_producer.flush(5)
