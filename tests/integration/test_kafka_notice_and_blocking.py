"""Integration tests: producer publish/notices and consumer blocking behavior."""

from datetime import datetime
from unittest.mock import patch

import numpy as np
import pytest

from tests.integration.support import mirror_config_for_calc
from tests.integration.support import sample_config
from tests.integration.support import wait_until
from utils import utils


pytestmark = pytest.mark.integration


class TestNoticePath:
    """publish() on producer -> listen_transactions on consumer."""

    def test_publish_delivers_transaction_notice(self, kafka_services):
        producer, consumer = kafka_services
        consumer.show_notice = False

        producer.interval_seconds = 1
        with (
            patch("services.producer.np.random.choice", side_effect=["deposit", "A", "+"]),
            patch("services.producer.np.random.uniform", return_value=5000.0),
        ):
            producer.publish()

        assert wait_until(lambda: len(consumer._queued_notices) == 1, timeout=20)
        assert consumer._queued_notices == ["Received new transaction."]


class TestBlockingDuringConfigUpdate:
    """Notices queue while config is updating; then report + batched notice count."""

    def test_queued_notices_and_report_after_modify_config(self, kafka_services, restore_data_config, capsys):
        producer, consumer = kafka_services
        consumer.show_notice = False
        consumer._queued_notices.clear()
        notice_count = 3

        producer.interval_seconds = 1
        for i in range(1, notice_count + 1):
            with (
                patch("services.producer.np.random.choice", side_effect=["deposit", "A", "+"]),
                patch("services.producer.np.random.uniform", return_value=5000.0),
            ):
                producer.publish()
            assert wait_until(
                lambda n=i: len(consumer._queued_notices) == n,
                timeout=20,
            )

        updated = sample_config(
            start_datetime=datetime(2026, 8, 1, 0, 0, 0),
            end_datetime=datetime(2026, 8, 15, 0, 0, 0),
            aggregation_methods=["count"],
        )

        def fake_cli_modify(_transaction_types):
            utils.write_config_to_json(
                config=updated,
                output_path=consumer.config_path.parent,
            )
            mirror_config_for_calc(updated, restore_data_config)
            return updated

        with patch(
            "services.consumer.clip.run_interactive_cli_modify",
            side_effect=fake_cli_modify,
        ):
            consumer.modify_config()

        out = capsys.readouterr().out
        assert f"[TransactionConsumer] #{notice_count} Received new transaction." in out
        assert "The count of the transactions between" in out
        assert consumer._queued_notices == []
        assert consumer.show_notice is True
        assert producer.config.aggregation_methods == ["count"]


class TestBlockingDuringTransactionDetails:
    """Notices queue during details request; snapshot printed then batched notices."""

    def test_snapshot_and_queued_publish_count(
        self, kafka_services, capsys
    ):
        producer, consumer = kafka_services
        consumer.show_notice = False
        consumer._queued_notices.clear()
        notice_count = 2

        producer.interval_seconds = 1
        for i in range(1, notice_count + 1):
            with (
                patch("services.producer.np.random.choice", side_effect=["transfer", "B", "-"]),
                patch("services.producer.np.random.uniform", return_value=8000.0),
            ):
                producer.publish()
            assert wait_until(
                lambda n=i: len(consumer._queued_notices) == n,
                timeout=20,
            )

        expected_rows = len(
            producer.transactions[
                producer.transactions["transaction_datetime"] <= producer.snapshot_datetime
            ]
        )

        consumer.request_transaction_details()
        out = capsys.readouterr().out

        assert "transaction_datetime" in out
        assert "transfer" in out
        assert consumer._reply_snapshot is not None
        assert len(consumer._reply_snapshot) == expected_rows
        assert f"[TransactionConsumer] #{notice_count} Received new transaction." in out
        assert consumer._queued_notices == []
        assert consumer.show_notice is True
