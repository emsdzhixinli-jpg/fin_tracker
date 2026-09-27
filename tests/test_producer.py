"""Unit tests for TransactionProducer with Kafka mocked out."""

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

import pandas as pd
import pytest

from data_models.configurations import SimulationConfig
from services.producer import TransactionProducer
from services.producer import producer as run_producer
from utils.constants import COUNTERPARTIES
from utils.constants import DIRECTIONS
from utils.constants import MAX_RECORDS_COUNT
from utils.constants import MIN_RECORDS_COUNT
from utils.constants import TRANSACTION_COLUMNS


def sample_config(**overrides) -> SimulationConfig:
    """A short August 2026 window with deposit/transfer types."""
    values = dict(
        start_datetime=datetime(2026, 8, 1, 0, 0, 0),
        end_datetime=datetime(2026, 8, 31, 0, 0, 0),
        transaction_types=["deposit", "transfer"],
        aggregation_methods=["sum"],
        filter_rules=None,
    )
    values.update(overrides)
    return SimulationConfig(**values)


@pytest.fixture
def config_path(tmp_path: Path) -> Path:
    """Write a SimulationConfig JSON under a temp data path."""
    path = tmp_path / "simulation_config.json"
    path.write_text(sample_config().model_dump_json())
    return path


@pytest.fixture
def kafka_producer_mock() -> MagicMock:
    """Stand-in for confluent_kafka.Producer."""
    return MagicMock()


@pytest.fixture
def tx_producer(config_path: Path, kafka_producer_mock: MagicMock) -> TransactionProducer:
    """Build a TransactionProducer without talking to a real Kafka broker."""
    with (
        patch("services.producer.Producer", return_value=kafka_producer_mock),
        patch(
            "services.producer.kafka_config.load_kafka_settings",
            return_value=({}, {}, "transactions", "config_sync"),
        ),
    ):
        return TransactionProducer(config=sample_config(), config_path=config_path)


def _decode_produced(kafka_producer_mock: MagicMock) -> dict:
    """Decode the last produce() payload as JSON."""
    raw = kafka_producer_mock.produce.call_args.kwargs.get("value")
    if raw is None:
        raw = kafka_producer_mock.produce.call_args.args[1]
    return json.loads(raw.decode("utf-8"))


class TestBuildTransactions:
    """Tests for __build_transactions via TransactionProducer init."""

    def test_columns_sorted_and_record_count(self, tx_producer: TransactionProducer):
        """Initial frame uses expected columns, sorted times, and bounded size."""
        df = tx_producer.transactions
        assert list(df.columns) == list(TRANSACTION_COLUMNS)
        assert MIN_RECORDS_COUNT <= len(df) < MAX_RECORDS_COUNT
        assert df["transaction_datetime"].is_monotonic_increasing
        assert set(df["counterparty"]).issubset(set(COUNTERPARTIES))
        assert set(df["direction"]).issubset(set(DIRECTIONS))

    def test_direction_forced_for_income_and_expense(self, config_path: Path, kafka_producer_mock: MagicMock):
        """Income/deposit get '+'; expense/withdrawal get '-'."""
        config = sample_config(
            transaction_types=["income", "expense", "deposit", "withdrawal"],
        )
        with (
            patch("services.producer.Producer", return_value=kafka_producer_mock),
            patch(
                "services.producer.kafka_config.load_kafka_settings",
                return_value=({}, {}, "transactions", "config_sync"),
            ),
        ):
            service = TransactionProducer(config=config, config_path=config_path)

        income_or_deposit = service.transactions["transaction_type"].isin(["income", "deposit"])
        expense_or_withdrawal = service.transactions["transaction_type"].isin(
            ["expense", "withdrawal"]
        )
        assert income_or_deposit.any() or expense_or_withdrawal.any()
        assert (service.transactions.loc[income_or_deposit, "direction"] == "+").all()
        assert (service.transactions.loc[expense_or_withdrawal, "direction"] == "-").all()


class TestAdjustConfig:
    """Tests for adjust_config reloading the shared JSON."""

    def test_reloads_config_from_path(self, tx_producer: TransactionProducer, config_path: Path):
        """Valid JSON on disk replaces self.config."""
        updated = sample_config(
            transaction_types=["income", "expense"],
            aggregation_methods=["mean"],
        )
        config_path.write_text(updated.model_dump_json())
        tx_producer.adjust_config(payload={"action": "reload"})
        assert tx_producer.config.transaction_types == ["income", "expense"]
        assert tx_producer.config.aggregation_methods == ["mean"]

    def test_keeps_prior_config_on_read_error(self, tx_producer: TransactionProducer, config_path: Path):
        """A bad file leaves the in-memory config unchanged."""
        prior = tx_producer.config.model_copy()
        config_path.write_text("not-json")
        tx_producer.adjust_config(payload={"action": "reload"})
        assert tx_producer.config == prior


class TestNotifyConsume:
    """Tests for notify_consume Kafka notice messages."""

    def test_publishes_transaction_notice(self, tx_producer: TransactionProducer, kafka_producer_mock: MagicMock):
        """Notice payload uses the transactions topic and expected type."""
        tx_producer.notify_consume()
        kafka_producer_mock.produce.assert_called_once()
        assert kafka_producer_mock.produce.call_args.args[0] == "transactions"
        payload = _decode_produced(kafka_producer_mock)
        assert payload["type"] == "transaction_notice"
        assert payload["report"] == "Received new transaction."
        assert "id" in payload
        kafka_producer_mock.flush.assert_called_once_with(10)


class TestReplyReport:
    """Tests for reply_report aggregation replies."""

    def test_publishes_aggregation_report(self, tx_producer: TransactionProducer, kafka_producer_mock: MagicMock):
        """calc() output is wrapped and published with the request id."""
        with patch("services.producer.pt.calc", return_value="REPORT TEXT") as calc:
            tx_producer.reply_report(request_id="req-1")
        calc.assert_called_once()
        payload = _decode_produced(kafka_producer_mock)
        assert payload == {
            "type": "aggregation_report",
            "request_id": "req-1",
            "report": "REPORT TEXT",
        }
        kafka_producer_mock.flush.assert_called_once_with(10)


class TestPublish:
    """Tests for publish appending one synthetic transaction."""

    def test_appends_row_and_notifies(self, tx_producer: TransactionProducer, kafka_producer_mock: MagicMock):
        """One new row is added and a consume notice is sent."""
        before = len(tx_producer.transactions)
        last_ts = tx_producer.transactions.iloc[-1]["transaction_datetime"]
        tx_producer.interval_seconds = 5
        with (
            patch("services.producer.np.random.choice", side_effect=["deposit", "A", "+"]),
            patch("services.producer.np.random.uniform", return_value=4250.0),
        ):
            tx_producer.publish()
        assert len(tx_producer.transactions) == before + 1
        newest = tx_producer.transactions.iloc[-1]
        assert newest["transaction_type"] == "deposit"
        assert newest["counterparty"] == "A"
        assert newest["direction"] == "+"
        assert newest["amount"] == 42.5
        assert newest["transaction_datetime"] == last_ts + pd.Timedelta(seconds=5)
        assert tx_producer.snapshot_datetime == newest["transaction_datetime"]
        payload = _decode_produced(kafka_producer_mock)
        assert payload["type"] == "transaction_notice"


class TestListenConfigSync:
    """Tests for listen_config_sync message handling."""

    def test_reload_calls_adjust_and_reply(self, tx_producer: TransactionProducer):
        """Non-snapshot actions reload config then reply with a report."""
        message = MagicMock()
        message.error.return_value = None
        message.value.return_value = json.dumps(
            {"action": "reload", "request_id": "req-9"}
        ).encode("utf-8")

        def poll(_timeout):
            if not hasattr(poll, "seen"):
                poll.seen = True
                return message
            tx_producer._stop.set()
            return None

        consumer = MagicMock()
        consumer.poll.side_effect = poll
        with (
            patch("services.producer.Consumer", return_value=consumer),
            patch.object(tx_producer, "adjust_config") as adjust,
            patch.object(tx_producer, "reply_report") as reply,
            patch.object(tx_producer, "reply_transactions") as reply_tx,
        ):
            tx_producer.listen_config_sync()

        consumer.subscribe.assert_called_once_with(["config_sync"])
        adjust.assert_called_once()
        reply.assert_called_once_with("req-9")
        reply_tx.assert_not_called()
        consumer.close.assert_called_once()

    def test_request_snapshot_replies_transactions(self, tx_producer: TransactionProducer):
        """request_snapshot skips adjust_config and publishes a snapshot."""
        message = MagicMock()
        message.error.return_value = None
        message.value.return_value = json.dumps(
            {"action": "request_snapshot", "request_id": "snap-1"}
        ).encode("utf-8")

        def poll(_timeout):
            if not hasattr(poll, "seen"):
                poll.seen = True
                return message
            tx_producer._stop.set()
            return None

        consumer = MagicMock()
        consumer.poll.side_effect = poll
        with (
            patch("services.producer.Consumer", return_value=consumer),
            patch.object(tx_producer, "adjust_config") as adjust,
            patch.object(tx_producer, "reply_report") as reply,
            patch.object(tx_producer, "reply_transactions") as reply_tx,
        ):
            tx_producer.listen_config_sync()

        reply_tx.assert_called_once_with("snap-1")
        adjust.assert_not_called()
        reply.assert_not_called()
        consumer.close.assert_called_once()


class TestProducerEntrypoint:
    """Tests for the producer() CLI entrypoint."""

    def test_writes_config_starts_service_and_replies(self):
        """CLI config is stored, then the service replies and runs forever."""
        config = sample_config()
        service = MagicMock()
        with (
            patch("services.producer.clip.run_interactive_cli", return_value=config) as cli,
            patch("services.producer.utils.write_config_to_json") as write_json,
            patch("services.producer.TransactionProducer", return_value=service) as ctor,
        ):
            run_producer()

        cli.assert_called_once()
        write_json.assert_called_once()
        assert write_json.call_args.kwargs["config"] == config
        ctor.assert_called_once()
        assert ctor.call_args.kwargs["config"] == config
        service.reply_report.assert_called_once_with(request_id=None)
        service.run_forever.assert_called_once()

    def test_terminates_on_run_forever_error(self):
        """Exceptions from run_forever trigger handler_terminate."""
        config = sample_config()
        service = MagicMock()
        service.run_forever.side_effect = RuntimeError("boom")
        with (
            patch("services.producer.clip.run_interactive_cli", return_value=config),
            patch("services.producer.utils.write_config_to_json"),
            patch("services.producer.TransactionProducer", return_value=service),
        ):
            run_producer()

        service.handler_terminate.assert_called_once()
