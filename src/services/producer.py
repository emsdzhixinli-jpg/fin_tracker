import json
import os
import signal
import threading
import uuid
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from confluent_kafka import Consumer
from confluent_kafka import KafkaError
from confluent_kafka import Producer

from utils import utils
utils.ensure_src_on_sys_path()
from data_models.configurations import SimulationConfig
from scripts import process_transactions as pt
from utils import cli_parser as clip
from utils import kafka_config
from utils.constants import COUNTERPARTIES
from utils.constants import DIRECTIONS
from utils.constants import JSON_FILENAME
from utils.constants import MIN_RECORDS_COUNT
from utils.constants import MAX_RECORDS_COUNT
from utils.constants import RND_MAX_SECONDS
from utils.constants import RND_MIN_SECONDS
from utils.constants import TRANSACTION_COLUMNS

KAFKA_SETTINGS_PATH = Path(__file__).resolve().parents[1] / "config" / "kafka_producer_config.yml"

logger = utils.set_logger(__name__)


class TransactionProducer():
    def __init__(
        self,
        config: SimulationConfig,
        config_path: Path
    ):
        self._stop = threading.Event()
        self.config = config
        self.config_path = config_path
        self.interval_seconds = np.random.randint(RND_MIN_SECONDS, RND_MAX_SECONDS)
        self.transactions: pd.DataFrame = self.__build_transactions(config)
        self.snapshot_datetime = self.transactions.iloc[-1]["transaction_datetime"]
        self._tx_lock = threading.Lock()
        (
            self._producer_kafka_config,
            self._consumer_kafka_config,
            self.transactions_topic,
            self.config_sync_topic,
        ) = kafka_config.load_kafka_settings(KAFKA_SETTINGS_PATH)
        self._kafka_producer = Producer(self._producer_kafka_config)
    
    def handler_terminate(self, signum=None, frame=None) -> None:
        """Handle SIGTERM for graceful shutdown."""
        logger.info(f"[TransactionProducer] Shutdown signal.")
        self._stop.set()

    def adjust_config(self, payload: dict | None = None) -> None:
        """Reload config from the shared JSON after a config_sync message."""
        try:
            raw = utils.read_text_locked(self.config_path)
            self.config = SimulationConfig.model_validate_json(raw)
            logger.info(
                f"[TransactionProducer] Configuration is updated from {self.config_path} "
                f"after {payload}:\n{self.config}"
            )
        except Exception as e:
            logger.error(f"[TransactionProducer] Failed to reload configuration: {e}")

    def reply_report(self, request_id: str | None) -> None:
        """Run calc on local transactions and publish the report text."""
        # Prevent from data race with the publishing background thread.
        with self._tx_lock:
            snapshot = self.transactions.loc[
                (self.transactions["transaction_datetime"] <= self.config.end_datetime) & \
                    (self.transactions["transaction_datetime"] >= self.config.start_datetime)
            ].copy() # Without blocking the publishing thread.
        report = pt.calc(transactions=snapshot, config=self.config)
        msg = {
            "type": "aggregation_report",
            "request_id": request_id,
            "report": report,
        }
        self._kafka_producer.produce(
            self.transactions_topic,
            value=json.dumps(msg).encode("utf-8"),
        )
        self._kafka_producer.flush(10)
        logger.info(f"[TransactionProducer] Replied with aggregation report for request_id={request_id}.")

    def notify_consume(self) -> None:
        """Notify the consumer that a new transaction was published."""
        msg = {
            "type": "transaction_notice",
            "id": str(uuid.uuid4()),
            "report": "Received new transaction."
        }
        self._kafka_producer.produce(
            self.transactions_topic,
            value=json.dumps(msg).encode("utf-8"),
        )
        self._kafka_producer.flush(10)

    def publish(self) -> None:
        """
        Publish a new transaction. Mimic latest transaction added.
        """
        with self._tx_lock:
            size = len(self.transactions)
            ts = self.transactions.iloc[-1]["transaction_datetime"] + timedelta(seconds=self.interval_seconds)
            self.transactions.loc[size] = {
                "transaction_datetime": ts,
                "transaction_type": np.random.choice(self.config.transaction_types),
                "counterparty": np.random.choice(list(COUNTERPARTIES)),
                "direction": np.random.choice(DIRECTIONS),
                "amount": np.random.uniform(1_000, 1_000_000) / 100
            }
            self.snapshot_datetime = ts
        self.notify_consume()

    def listen_config_sync(self) -> None:
        """Background loop: reload config, run calc, reply with the report."""
        kafka_consumer = Consumer(self._consumer_kafka_config)
        kafka_consumer.subscribe([self.config_sync_topic])
        logger.info(f"[TransactionProducer] Listening on {self.config_sync_topic}.")
        try:
            while not self._stop.is_set():
                message = kafka_consumer.poll(1.0) # Using second-based producer.
                if message is None:
                    continue
                if message.error():
                    if message.error().code() != KafkaError._PARTITION_EOF:
                        logger.error(f"[TransactionProducer] Kafka error: {message.error()}")
                    continue
                payload = json.loads(message.value().decode("utf-8"))
                action = payload.get("action")
                request_id = payload.get("request_id")
                if action == "request_snapshot":
                    self.reply_transactions(request_id)
                    continue
                self.adjust_config(payload)
                self.reply_report(request_id)
        finally:
            kafka_consumer.close()
    
    def run_forever(self) -> None:
        """Publish transactions until SIGTERM. Config updates arrive on Kafka."""
        signal.signal(signal.SIGTERM, self.handler_terminate)
        listener = threading.Thread(target=self.listen_config_sync, name="config-sync", daemon=True)
        listener.start()

        logger.info(f"[TransactionProducer] Personal Financial Tracker started... (PID: {os.getpid()})")
        try:
            while not self._stop.is_set():
                self.publish()
                self._stop.wait(timeout=self.interval_seconds)
        finally:
            self._stop.set()
            self._kafka_producer.flush(5)
        logger.info("[TransactionProducer] Stopped")

    def __build_transactions(self, config: SimulationConfig) -> pd.DataFrame:
        """
        Build initial transactions dataframe. Mimic historical transactions.
        Only called once at initialization.
        """
        span_seconds = int((config.end_datetime - config.start_datetime).total_seconds())
        records_count = np.random.randint(MIN_RECORDS_COUNT, MAX_RECORDS_COUNT)
        offsets = np.random.randint(0, span_seconds, size=records_count)

        transaction_dts = np.datetime64(config.start_datetime) + offsets.astype("timedelta64[s]")
        transaction_types = np.random.choice(config.transaction_types, size=records_count)
        counterparties = np.random.choice(list(COUNTERPARTIES), size=records_count)
        directions = np.random.choice(DIRECTIONS, size=records_count)
        amounts = np.random.uniform(1_000, 1_000_000, size=records_count) / 100

        transactions = pd.DataFrame({
            "transaction_datetime": transaction_dts,
            "transaction_type": transaction_types,
            "counterparty": counterparties,
            "direction": directions,
            "amount": amounts
        }, index=None, columns=TRANSACTION_COLUMNS)

        pos_mask = (transactions["transaction_type"] == "income") | \
            (transactions["transaction_type"] == "deposit")
        neg_mask = (transactions["transaction_type"] == "expense") | \
            (transactions["transaction_type"] == "withdrawal")
        transactions.loc[pos_mask, "direction"] = "+"
        transactions.loc[neg_mask, "direction"] = "-"
        transactions.sort_values(by="transaction_datetime", inplace=True)
        transactions.reset_index(drop=True, inplace=True)
        return transactions
    

    '''
    Extra function to double check the data.
    '''
    def reply_transactions(self, request_id: str | None) -> None:
        """Publish transactions whose datetime is no later than snapshot_datetime."""
        with self._tx_lock:
            mask = self.transactions["transaction_datetime"] <= self.snapshot_datetime
            subset = self.transactions.loc[mask]
            records = subset.assign(
                transaction_datetime=subset["transaction_datetime"].astype(str)
            ).to_dict(orient="records")
        msg = {
            "type": "transactions_snapshot",
            "request_id": request_id,
            "records": records,
        }
        self._kafka_producer.produce(
            self.transactions_topic,
            value=json.dumps(msg).encode("utf-8"),
        )
        self._kafka_producer.flush(10)
        logger.info(
            f"[TransactionProducer] Replied with {len(records)} transactions "
            f"for request_id={request_id}."
        )


def producer():
    """Collect a config, write it under the file lock, then publish transactions."""
    producer_config = clip.run_interactive_cli()
    config_path = Path(__file__).resolve().parents[2] / "data" / JSON_FILENAME
    utils.write_config_to_json(config=producer_config, output_path=config_path.parent)
    service = TransactionProducer(config=producer_config, config_path=config_path)
    service.reply_report(request_id=None)
    try:
        service.run_forever()
        logger.info("[TransactionProducer] Transaction producer stopped.")
    except Exception as e:
        logger.error(f"[TransactionProducer] Stopping transaction producer owing to error: {e}")
        service.handler_terminate()
        logger.info("[TransactionProducer] Transaction producer stopped.")


if __name__ == "__main__":
    producer()