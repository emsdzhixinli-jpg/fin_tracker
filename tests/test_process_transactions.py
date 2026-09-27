"""Tests for the core aggregation and formatting functions."""

from datetime import datetime

import pandas as pd

from data_models.configurations import SimulationConfig
from scripts.process_transactions import aggregate_transactions
from scripts.process_transactions import build_aggregation_report
from scripts.process_transactions import format_method_header
from scripts.process_transactions import format_result_table
from utils.rules import apply_filter_rules


def sample_transactions() -> pd.DataFrame:
    """A few rows with repeated counterparties and both directions."""
    return pd.DataFrame({
        "transaction_type": ["deposit", "deposit", "transfer", "transfer"],
        "direction": ["+", "+", "-", "-"],
        "counterparty": ["A", "A", "B", "C"],
        "amount": [10.0, 5.5, 3.0, 2.0],
    })


def sample_config(filter_rules=None) -> SimulationConfig:
    """A config covering August 2026."""
    return SimulationConfig(
        start_datetime=datetime(2026, 8, 1, 0, 0, 0),
        end_datetime=datetime(2026, 8, 31, 0, 0, 0),
        transaction_types=["deposit", "transfer"],
        aggregation_methods=["sum"],
        filter_rules=filter_rules,
    )


class TestAggregateTransactions:
    """Tests for aggregation and filter rules used before aggregation."""

    def test_sum(self):
        """Sum groups by type and direction and counts unique counterparties."""
        result = aggregate_transactions(sample_transactions(), "sum")
        deposit = result[
            (result["transaction_type"] == "deposit") & (result["direction"] == "+")
        ].iloc[0]
        transfer = result[
            (result["transaction_type"] == "transfer") & (result["direction"] == "-")
        ].iloc[0]
        assert deposit["amount"] == 15.5
        assert deposit["counterparties_count"] == 1
        assert transfer["amount"] == 5.0
        assert transfer["counterparties_count"] == 2
        assert list(result["transaction_type"]) == ["deposit", "transfer"]

    def test_count(self):
        """Count uses the number of rows in each group as the amount."""
        result = aggregate_transactions(sample_transactions(), "count")
        deposit = result[
            (result["transaction_type"] == "deposit") & (result["direction"] == "+")
        ].iloc[0]
        assert deposit["amount"] == 2

    def test_apply_filter_rules_and_on_amount(self):
        """And connector keeps rows that match every amount rule."""
        rules = ("and", [("transfer", ">=", 3.0), ("transfer", "<=", 10.0)])
        filtered = apply_filter_rules(sample_transactions(), rules)
        assert list(filtered["amount"]) == [3.0]

    def test_apply_filter_rules_or_on_transaction_type(self):
        """Or connector keeps rows that match any type/amount rule."""
        rules = ("or", [("deposit", ">", 6.0), ("transfer", "<=", 2.0)])
        filtered = apply_filter_rules(sample_transactions(), rules)
        assert list(filtered["amount"]) == [10.0, 2.0]


class TestBuildAggregationReport:
    """build_aggregation_report applies filter_rules only, not datetime windows."""

    def test_sums_all_rows_without_filter_rules(self):
        """Every transaction row is aggregated; config dates appear only in headers."""
        frame = pd.DataFrame({
            "transaction_datetime": [
                datetime(2026, 8, 1, 12, 0, 0),
                datetime(2026, 8, 15, 12, 0, 0),
            ],
            "transaction_type": ["deposit", "deposit"],
            "direction": ["+", "+"],
            "counterparty": ["A", "B"],
            "amount": [100.0, 200.0],
        })
        config = SimulationConfig(
            start_datetime=datetime(2026, 8, 10, 0, 0, 0),
            end_datetime=datetime(2026, 8, 20, 0, 0, 0),
            transaction_types=["deposit"],
            aggregation_methods=["sum"],
            filter_rules=None,
        )
        report = build_aggregation_report(config, frame)
        assert "300.0" in report


class TestFormat:
    """Tests for format_method_header and format_result_table."""

    def test_method_header_without_filter(self):
        """A missing rule list says that no filter is used."""
        header = format_method_header(sample_config(), "sum", None)
        assert "2026-08-01 00:00:00" in header
        assert "2026-08-31 00:00:00" in header
        assert header.endswith("with no filter:")

    def test_method_header_with_filter_rules(self):
        """Stored rules are named in the header with their connector."""
        rules = ("and", [("income", "<", 1000.0)])
        header = format_method_header(sample_config(rules), "sum", rules)
        assert "with filter rules (income:<:1000.0):" in header

    def test_method_header_with_or_filter_rules(self):
        """Or-connected rules are joined with 'or' in the header."""
        rules = ("or", [("income", "<=", 1000.0), ("expense", ">", 500.0)])
        header = format_method_header(sample_config(rules), "sum", rules)
        assert "with filter rules (income:<=:1000.0 or expense:>:500.0):" in header

    def test_result_table(self):
        """The table has matching borders and includes grouped amounts."""
        result = aggregate_transactions(sample_transactions(), "sum")
        table = format_result_table(result)
        lines = table.splitlines()
        assert "Transaction Type" in lines[1]
        assert "Directions" in lines[1]
        assert "deposit" in table
        assert "15.5" in table
        assert lines[0] == lines[2] == lines[-1]
        assert set(lines[0]) == {"-"}
