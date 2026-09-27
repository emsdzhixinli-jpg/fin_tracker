import json
import signal
import threading
import time
import uuid
from multiprocessing import Process
from pathlib import Path

import pandas as pd
from confluent_kafka import Consumer
from confluent_kafka import KafkaError
from confluent_kafka import Producer

from utils import utils
utils.ensure_src_on_sys_path()
from data_models.configurations import SimulationConfig
from utils import cli_parser as clip
from utils import kafka_config
from utils.constants import JSON_FILENAME
from utils.constants import TRANSACTION_COLUMNS


logger = utils.set_logger(__name__)

KAFKA_SETTINGS_PATH = Path(__file__).resolve().parents[1] / "config" / "kafka_consumer_config.yml"
REPORT_REPLY_TIMEOUT_SECONDS = 30.0


class TransactionConsumer(Process):
    def __init__(
        self,
        config: SimulationConfig,
        config_path: Path
    ):
        super().__init__()
        # threading.Event: listener runs in a thread, not a spawned Process.
        self._stop = threading.Event()
        self.config = config
        self.config_path = config_path
        self.show_notice: bool = True
        self._notice_lock = threading.Lock()
        self._queued_notices: list[str] = []
        self._pending_request_id: str | None = None
        self._reply_event = threading.Event()
        self._reply_report: str | None = None
        self._reply_snapshot: pd.DataFrame | None = None # Extra for checking transaction details.
        (
            self._producer_kafka_config,
            self._consumer_kafka_config,
            self.config_sync_topic,
            self.transactions_topic,
        ) = kafka_config.load_kafka_settings(KAFKA_SETTINGS_PATH)
        self._kafka_producer = Producer(self._producer_kafka_config)
    
    def _flush_queued_notices(self) -> None:
        """Print notices that arrived while the config CLI was running."""
        with self._notice_lock:
            if not self._queued_notices:
                return
            notice = self._queued_notices[0]
            size = len(self._queued_notices)
            self._queued_notices = []
        print(f"[TransactionConsumer] #{size} {notice}")

    def _handle_transactions_message(self, payload: dict) -> None:
        """Route startup reports, request replies, and live notices."""
        if payload.get("type") == "aggregation_report":
            request_id = payload.get("request_id")
            report = payload.get("report", "")
            if request_id is None:
                print(report)
                return
            if request_id == self._pending_request_id:
                self._reply_report = report
                self._reply_event.set()
                logger.info(
                    f"[TransactionConsumer] Received aggregation report for request_id={request_id}."
                )
            return

        if payload.get("type") == "transactions_snapshot":
            self._handle_transactions_snapshot(payload)
            return

        notice = payload.get("report", "Received new transaction.")
        if self.show_notice:
            print(f"[TransactionConsumer] {notice}")
            return
        with self._notice_lock:
            self._queued_notices.append(notice)

    def handler_terminate(self, signum=None, frame=None) -> None:
        """Handle SIGTERM for graceful shutdown."""
        logger.info(f"[TransactionConsumer] Shutdown signal.")
        self._stop.set()

    def modify_config(self) -> None:
        """Modifying the configuration on user request."""
        try:
            self.config = clip.run_interactive_cli_modify(self.config.transaction_types)
            utils.write_config_to_json(config=self.config, output_path=self.config_path.parent)
            report = self.send_config_sync()
            print(report)
            self._flush_queued_notices()
            self.show_notice = True
        except KeyboardInterrupt:
            logger.info(f"[TransactionConsumer] Modify configuration signal interrupted.")
            self._stop.set()
        except Exception as e:
            logger.error(f"[TransactionConsumer] Failed to apply configuration: {e}")
            self._flush_queued_notices()
            self.show_notice = True

    def send_config_sync(self) -> str:
        """Ask the producer to reload config, run calc, and return the report text."""
        request_id = str(uuid.uuid4())
        self._reply_report = None
        self._reply_event.clear()
        self._pending_request_id = request_id

        payload = {
            "action": "reload_config",
            "request_id": request_id,
            "path": str(self.config_path),
        }
        self._kafka_producer.produce(
            self.config_sync_topic,
            value=json.dumps(payload).encode("utf-8"),
        )
        self._kafka_producer.flush(10)
        logger.info(
            f"[TransactionConsumer] Sent config reload on {self.config_sync_topic} (request_id={request_id})."
        )

        if not self._reply_event.wait(timeout=REPORT_REPLY_TIMEOUT_SECONDS):
            self._pending_request_id = None
            raise TimeoutError(
                f"Timed out after {REPORT_REPLY_TIMEOUT_SECONDS:.0f}s waiting for "
                f"aggregation_report (request_id={request_id})."
            )

        self._pending_request_id = None
        if self._reply_report is None:
            raise RuntimeError(f"Empty aggregation report for request_id={request_id}.")
        return self._reply_report

    def _handle_transactions_snapshot(self, payload: dict) -> None:
        """Rebuild the producer snapshot and complete a details request."""
        if payload.get("request_id") != self._pending_request_id:
            return
        frame = pd.DataFrame(payload.get("records", []), columns=list(TRANSACTION_COLUMNS))
        if "transaction_datetime" in frame.columns:
            frame["transaction_datetime"] = pd.to_datetime(frame["transaction_datetime"])
        self._reply_snapshot = frame
        self._reply_event.set()
        logger.info(
            f"[TransactionConsumer] Received {len(frame)} transactions "
            f"for request_id={payload.get('request_id')}."
        )

    def listen_transactions(self) -> None:
        """Keep polling Kafka; print or queue notices based on show_notice."""
        kafka_consumer = Consumer(self._consumer_kafka_config)
        kafka_consumer.subscribe([self.transactions_topic])
        logger.info(f"[TransactionConsumer] Listening on {self.transactions_topic}.")
        try:
            while not self._stop.is_set():
                message = kafka_consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    if message.error().code() != KafkaError._PARTITION_EOF:
                        logger.error(f"[TransactionConsumer] Kafka error: {message.error()}")
                    continue
                payload = json.loads(message.value().decode("utf-8"))
                self._handle_transactions_message(payload)
        finally:
            kafka_consumer.close()

    def run_forever(self) -> None:
        """Run the transaction consumer."""
        signal.signal(signal.SIGTERM, self.handler_terminate)
        listener = threading.Thread(
            target=self.listen_transactions,
            name="transaction-notices",
            daemon=True,
        )
        listener.start()
        # Let the Kafka consumer join the group before the first config_sync request.
        time.sleep(2)

        while not self._stop.is_set():
            print("Next track? [Y/n/d] (Y - modify config, n - quit consumer, d - view transaction details): ")
            answer = input().strip().lower()
            if answer in ("d", "details"):
                self.show_notice = False
                self.request_transaction_details()
                continue
            if answer not in ("", "y", "yes"):
                break

            self.show_notice = False
            self.modify_config()
        self._stop.set()
        self._kafka_producer.flush(5)

    '''
    Extra function to double check the data.
    '''
    def request_transaction_details(self) -> None:
        """Ask the producer for a snapshot of self.transactions and print it."""
        request_id = str(uuid.uuid4())
        self._reply_snapshot = None
        self._reply_event.clear()
        self._pending_request_id = request_id

        payload = {
            "action": "request_snapshot",
            "request_id": request_id,
        }
        self._kafka_producer.produce(
            self.config_sync_topic,
            value=json.dumps(payload).encode("utf-8"),
        )
        self._kafka_producer.flush(10)
        logger.info(
            f"[TransactionConsumer] Sent snapshot request on {self.config_sync_topic} "
            f"(request_id={request_id})."
        )

        try:
            if not self._reply_event.wait(timeout=REPORT_REPLY_TIMEOUT_SECONDS):
                raise TimeoutError(
                    f"Timed out after {REPORT_REPLY_TIMEOUT_SECONDS:.0f}s waiting for "
                    f"transactions_snapshot (request_id={request_id})."
                )
            if self._reply_snapshot is None:
                raise RuntimeError(f"Empty transactions snapshot for request_id={request_id}.")
            print(self._reply_snapshot.to_string(index=False))
        except Exception as e:
            logger.error(f"[TransactionConsumer] Failed to load transaction details: {e}")
        finally:
            self._pending_request_id = None
            self._flush_queued_notices()
            self.show_notice = True


def consumer():
    """Wait for the shared config, then run the interactive consumer.

    Compose only waits until the producer container has started. The JSON is
    written after the producer CLI finishes, so we poll for that file here.
    """
    config_path = Path(__file__).resolve().parents[2] / "data" / JSON_FILENAME
    try:
        logger.info(f"[TransactionConsumer] Waiting for shared config at {config_path}.")
        raw = utils.wait_and_read_text_locked(config_path)
        config = SimulationConfig.model_validate_json(raw)
        service = TransactionConsumer(config=config, config_path=config_path)
        service.run_forever()
        logger.info("[TransactionConsumer] Transaction consumer stopped.")
    except Exception as e:
        logger.error(f"[TransactionConsumer] Stopping transaction consumer owing to error: {e}")
        logger.info("[TransactionConsumer] Transaction consumer stopped.")


if __name__ == "__main__":
    consumer()
