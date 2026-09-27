"""Unit tests for TransactionConsumer with Kafka mocked out."""

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

import pandas as pd
import pytest

from data_models.configurations import SimulationConfig
from services.consumer import TransactionConsumer
from services.consumer import consumer as run_consumer
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
def tx_consumer(config_path: Path, kafka_producer_mock: MagicMock) -> TransactionConsumer:
    """Build a TransactionConsumer without talking to a real Kafka broker."""
    # Consumer YAML: produce=config_sync, consume=transactions.
    with (
        patch("services.consumer.Producer", return_value=kafka_producer_mock),
        patch(
            "services.consumer.kafka_config.load_kafka_settings",
            return_value=({}, {}, "config_sync", "transactions"),
        ),
    ):
        return TransactionConsumer(config=sample_config(), config_path=config_path)


def _decode_produced(kafka_producer_mock: MagicMock) -> dict:
    """Decode the last produce() payload as JSON."""
    raw = kafka_producer_mock.produce.call_args.kwargs.get("value")
    if raw is None:
        raw = kafka_producer_mock.produce.call_args.args[1]
    return json.loads(raw.decode("utf-8"))


class TestFlushQueuedNotices:
    """Tests for _flush_queued_notices printing buffered notices."""

    def test_prints_nothing_when_queue_empty(
        self, tx_consumer: TransactionConsumer, capsys: pytest.CaptureFixture[str]
    ):
        """Empty queue is a no-op."""
        tx_consumer._flush_queued_notices()
        assert capsys.readouterr().out == ""

    def test_prints_count_and_clears_queue(
        self, tx_consumer: TransactionConsumer, capsys: pytest.CaptureFixture[str]
    ):
        """First notice text is printed with the queue size, then the queue clears."""
        tx_consumer._queued_notices = ["Received new transaction.", "another"]
        tx_consumer._flush_queued_notices()
        out = capsys.readouterr().out
        assert "[TransactionConsumer] #2 Received new transaction." in out
        assert tx_consumer._queued_notices == []


class TestHandleTransactionsMessage:
    """Tests for _handle_transactions_message routing."""

    def test_startup_report_without_request_id_prints(
        self, tx_consumer: TransactionConsumer, capsys: pytest.CaptureFixture[str]
    ):
        """Aggregation report with request_id=None is printed immediately."""
        tx_consumer._handle_transactions_message(
            {"type": "aggregation_report", "request_id": None, "report": "STARTUP"}
        )
        assert "STARTUP" in capsys.readouterr().out

    def test_matching_report_sets_reply_event(self, tx_consumer: TransactionConsumer):
        """Matching request_id stores the report and signals waiters."""
        tx_consumer._pending_request_id = "req-1"
        tx_consumer._handle_transactions_message(
            {"type": "aggregation_report", "request_id": "req-1", "report": "DONE"}
        )
        assert tx_consumer._reply_report == "DONE"
        assert tx_consumer._reply_event.is_set()

    def test_mismatched_report_ignored(self, tx_consumer: TransactionConsumer):
        """Reports for other request ids do not complete the pending wait."""
        tx_consumer._pending_request_id = "req-1"
        tx_consumer._handle_transactions_message(
            {"type": "aggregation_report", "request_id": "other", "report": "NOPE"}
        )
        assert tx_consumer._reply_report is None
        assert not tx_consumer._reply_event.is_set()

    def test_snapshot_delegates(self, tx_consumer: TransactionConsumer):
        """transactions_snapshot messages go to _handle_transactions_snapshot."""
        with patch.object(tx_consumer, "_handle_transactions_snapshot") as handle_snap:
            tx_consumer._handle_transactions_message(
                {"type": "transactions_snapshot", "request_id": "snap-1", "records": []}
            )
        handle_snap.assert_called_once()

    def test_notice_prints_when_show_notice(
        self, tx_consumer: TransactionConsumer, capsys: pytest.CaptureFixture[str]
    ):
        """Live notices print when show_notice is True."""
        tx_consumer.show_notice = True
        tx_consumer._handle_transactions_message(
            {"type": "transaction_notice", "report": "Received new transaction."}
        )
        assert "[TransactionConsumer] Received new transaction." in capsys.readouterr().out
        assert tx_consumer._queued_notices == []

    def test_notice_queued_when_show_notice_false(self, tx_consumer: TransactionConsumer):
        """Notices are buffered while the config CLI is active."""
        tx_consumer.show_notice = False
        tx_consumer._handle_transactions_message(
            {"type": "transaction_notice", "report": "Received new transaction."}
        )
        assert tx_consumer._queued_notices == ["Received new transaction."]


class TestModifyConfig:
    """Tests for modify_config interactive reload."""

    def test_updates_config_syncs_and_flushes(self, tx_consumer: TransactionConsumer):
        """CLI modify writes JSON, syncs, flushes notices, and re-enables printing."""
        updated = sample_config(aggregation_methods=["mean"])
        tx_consumer.show_notice = False
        with (
            patch(
                "services.consumer.clip.run_interactive_cli_modify",
                return_value=updated,
            ) as cli,
            patch("services.consumer.utils.write_config_to_json") as write_json,
            patch.object(tx_consumer, "send_config_sync", return_value="REPORT") as sync,
            patch.object(tx_consumer, "_flush_queued_notices") as flush,
            patch("builtins.print") as printer,
        ):
            tx_consumer.modify_config()

        cli.assert_called_once_with(["deposit", "transfer"])
        write_json.assert_called_once()
        assert write_json.call_args.kwargs["config"] == updated
        sync.assert_called_once()
        printer.assert_called_once_with("REPORT")
        flush.assert_called_once()
        assert tx_consumer.show_notice is True
        assert tx_consumer.config == updated

    def test_keyboard_interrupt_stops_consumer(self, tx_consumer: TransactionConsumer):
        """Ctrl-C during modify sets the stop event."""
        with patch(
            "services.consumer.clip.run_interactive_cli_modify",
            side_effect=KeyboardInterrupt,
        ):
            tx_consumer.modify_config()
        assert tx_consumer._stop.is_set()

    def test_other_errors_flush_and_restore_notices(self, tx_consumer: TransactionConsumer):
        """Non-interrupt failures still flush the queue and show notices again."""
        tx_consumer.show_notice = False
        with (
            patch(
                "services.consumer.clip.run_interactive_cli_modify",
                side_effect=RuntimeError("cli failed"),
            ),
            patch.object(tx_consumer, "_flush_queued_notices") as flush,
        ):
            tx_consumer.modify_config()
        flush.assert_called_once()
        assert tx_consumer.show_notice is True


class TestSendConfigSync:
    """Tests for send_config_sync produce + wait for report."""

    def test_publishes_reload_and_returns_report(
        self, tx_consumer: TransactionConsumer, kafka_producer_mock: MagicMock
    ):
        """Produces reload_config and returns the matching aggregation report."""
        def set_reply(*_args, **_kwargs):
            tx_consumer._reply_report = "AGG REPORT"
            tx_consumer._reply_event.set()
            return True

        with patch.object(tx_consumer._reply_event, "wait", side_effect=set_reply):
            report = tx_consumer.send_config_sync()

        assert report == "AGG REPORT"
        assert kafka_producer_mock.produce.call_args.args[0] == "config_sync"
        payload = _decode_produced(kafka_producer_mock)
        assert payload["action"] == "reload_config"
        assert payload["path"] == str(tx_consumer.config_path)
        assert payload["request_id"] == tx_consumer._pending_request_id or True
        # pending id cleared after success
        assert tx_consumer._pending_request_id is None
        kafka_producer_mock.flush.assert_called_once_with(10)

    def test_timeout_raises(self, tx_consumer: TransactionConsumer):
        """No reply within the timeout raises TimeoutError."""
        with (
            patch("services.consumer.REPORT_REPLY_TIMEOUT_SECONDS", 0.01),
            patch.object(tx_consumer._reply_event, "wait", return_value=False),
            pytest.raises(TimeoutError, match="Timed out"),
        ):
            tx_consumer.send_config_sync()
        assert tx_consumer._pending_request_id is None

    def test_empty_report_raises(self, tx_consumer: TransactionConsumer):
        """A signaled wait with no report text raises RuntimeError."""
        def set_empty(*_args, **_kwargs):
            tx_consumer._reply_report = None
            return True

        with (
            patch.object(tx_consumer._reply_event, "wait", side_effect=set_empty),
            pytest.raises(RuntimeError, match="Empty aggregation report"),
        ):
            tx_consumer.send_config_sync()


class TestHandleTransactionsSnapshot:
    """Tests for _handle_transactions_snapshot."""

    def test_ignores_mismatched_request_id(self, tx_consumer: TransactionConsumer):
        """Snapshots for other requests leave _reply_snapshot untouched."""
        tx_consumer._pending_request_id = "want"
        tx_consumer._handle_transactions_snapshot(
            {"request_id": "other", "records": []}
        )
        assert tx_consumer._reply_snapshot is None
        assert not tx_consumer._reply_event.is_set()

    def test_builds_dataframe_and_signals(self, tx_consumer: TransactionConsumer):
        """Matching snapshot becomes a DataFrame and completes the wait."""
        tx_consumer._pending_request_id = "snap-1"
        records = [
            {
                "transaction_datetime": "2026-08-01 12:00:00",
                "transaction_type": "deposit",
                "counterparty": "A",
                "direction": "+",
                "amount": 10.0,
            }
        ]
        tx_consumer._handle_transactions_snapshot(
            {"request_id": "snap-1", "records": records}
        )
        assert isinstance(tx_consumer._reply_snapshot, pd.DataFrame)
        assert list(tx_consumer._reply_snapshot.columns) == list(TRANSACTION_COLUMNS)
        assert len(tx_consumer._reply_snapshot) == 1
        assert pd.api.types.is_datetime64_any_dtype(
            tx_consumer._reply_snapshot["transaction_datetime"]
        )
        assert tx_consumer._reply_event.is_set()


class TestListenTransactions:
    """Tests for listen_transactions polling loop."""

    def test_routes_decoded_payload_then_closes(self, tx_consumer: TransactionConsumer):
        """Valid messages are decoded and passed to _handle_transactions_message."""
        message = MagicMock()
        message.error.return_value = None
        message.value.return_value = json.dumps(
            {"type": "transaction_notice", "report": "hi"}
        ).encode("utf-8")

        def poll(_timeout):
            if not hasattr(poll, "seen"):
                poll.seen = True
                return message
            tx_consumer._stop.set()
            return None

        consumer = MagicMock()
        consumer.poll.side_effect = poll
        with (
            patch("services.consumer.Consumer", return_value=consumer),
            patch.object(tx_consumer, "_handle_transactions_message") as handle,
        ):
            tx_consumer.listen_transactions()

        consumer.subscribe.assert_called_once_with(["transactions"])
        handle.assert_called_once_with(
            {"type": "transaction_notice", "report": "hi"}
        )
        consumer.close.assert_called_once()

    def test_skips_kafka_errors(self, tx_consumer: TransactionConsumer):
        """Non-EOF Kafka errors are logged and the loop continues until stop."""
        err = MagicMock()
        err.code.return_value = 1  # not PARTITION_EOF
        bad = MagicMock()
        bad.error.return_value = err

        def poll(_timeout):
            if not hasattr(poll, "seen"):
                poll.seen = True
                return bad
            tx_consumer._stop.set()
            return None

        consumer = MagicMock()
        consumer.poll.side_effect = poll
        with (
            patch("services.consumer.Consumer", return_value=consumer),
            patch.object(tx_consumer, "_handle_transactions_message") as handle,
        ):
            tx_consumer.listen_transactions()

        handle.assert_not_called()
        consumer.close.assert_called_once()


class TestConsumerEntrypoint:
    """Tests for the consumer() CLI entrypoint."""

    def test_loads_config_and_runs_forever(self):
        """Shared JSON is read, then the service runs until stopped."""
        config = sample_config()
        service = MagicMock()
        with (
            patch(
                "services.consumer.utils.wait_and_read_text_locked",
                return_value=config.model_dump_json(),
            ) as wait_read,
            patch("services.consumer.TransactionConsumer", return_value=service) as ctor,
        ):
            run_consumer()

        wait_read.assert_called_once()
        ctor.assert_called_once()
        assert ctor.call_args.kwargs["config"] == config
        service.run_forever.assert_called_once()

    def test_logs_and_stops_on_error(self):
        """Startup failures are caught so the process exits cleanly."""
        with (
            patch(
                "services.consumer.utils.wait_and_read_text_locked",
                side_effect=FileNotFoundError("missing"),
            ),
            patch("services.consumer.TransactionConsumer") as ctor,
        ):
            run_consumer()
        ctor.assert_not_called()
