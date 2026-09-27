"""Aggregate generated bank transactions and print a table per method."""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_models.configurations import SimulationConfig
from utils import rules
from utils import utils
from utils.constants import DATETIME_FORMAT
from utils.constants import JSON_FILENAME

utils.ensure_src_on_sys_path()

logger = utils.set_logger(__name__)

TABLE_HEADERS = (
    "Transaction Type",
    "Directions",
    "Amount",
    "Counterparties Count",
)
GROUP_COLUMNS = ["transaction_type", "direction"]


def load_simulation_config(path: Path) -> SimulationConfig:
    """Load a simulation config from JSON.

    The shared lock blocks this read until an in-progress locked write finishes,
    so calc() cannot observe a half-written config file.
    """
    return SimulationConfig.model_validate_json(utils.read_text_locked(path))


def aggregate_transactions(transactions: pd.DataFrame, method: str) -> pd.DataFrame:
    """Group by transaction type and direction, then aggregate amount."""
    try:
        grouped = transactions.groupby(GROUP_COLUMNS, as_index=False)
        result = grouped.agg(
            amount=("amount", method),
            counterparties_count=("counterparty", "nunique"),
        )
        return result.sort_values(GROUP_COLUMNS).reset_index(drop=True)
    except Exception as e:
        logger.error(f"[aggregate_transactions] {method} cannot be applied to the calculation. "
                    f"Please try another aggregation method. Error: {e}")
        return pd.DataFrame()


def format_filter_clause(filter_rules) -> str:
    """Describe whether filter rules apply."""
    if not filter_rules:
        return "with no filter:"
    connector, rules = filter_rules
    joiner = f" {connector} "
    rule_text = joiner.join(
        f"{field}:{operator}:{value}" for field, operator, value in rules
    )
    return f"with filter rules ({rule_text}):"


def format_method_header(config: SimulationConfig, method: str, filter_rules) -> str:
    """Build the sentence that introduces one aggregation table."""
    start = config.start_datetime.strftime(DATETIME_FORMAT)
    end = config.end_datetime.strftime(DATETIME_FORMAT)
    clause = format_filter_clause(filter_rules)
    return (
        f"The {method} of the transactions between {start} and {end} "
        f"are as follows, {clause}"
    )


def format_amount(amount: float, method: str) -> str:
    """Format an aggregated amount for the table."""
    if method == "count":
        return str(int(amount))
    return f"{amount:.2f}"


def format_table_line(cells: tuple[str, ...], widths: list[int]) -> str:
    """Format one bordered table row."""
    padded = [f" {str(cell).ljust(widths[e])} " for e, cell in enumerate(cells)]
    return "|" + "|".join(padded) + "|"


def build_table(result: pd.DataFrame, col_widths: list[int]) -> str:
    """Build the table for the given aggregation method."""
    header_line = format_table_line(cells=TABLE_HEADERS, widths=col_widths)
    border = "-" * len(header_line)
    lines = [border, header_line + "|", border]
    for row in result.itertuples(index=False):
        lines.append(format_table_line(cells=row, widths=col_widths))
    lines.append(border)
    return "\n".join(lines)


def format_result_table(result: pd.DataFrame) -> str:
    """Format the result table for the given aggregation method."""
    # Get the column widths
    col_widths = []
    for e, col in enumerate(result.columns):
        cell_widths = len(TABLE_HEADERS[e]) + 2
        col_widths.append(max(cell_widths, result[col].astype(str).str.len().max()))
    
    # Build the table
    output: str = build_table(result, col_widths)
    return output


@utils.timer
def build_aggregation_report(config: SimulationConfig, transactions: pd.DataFrame) -> str:
    """Build the full report for every aggregation method."""
    filtered: pd.DataFrame = rules.apply_filter_rules(transactions, config.filter_rules)
    blocks = []
    for method in config.aggregation_methods:
        result = aggregate_transactions(transactions=filtered, method=method)
        if result.empty:
            blocks.append(f"Aggregation method {method} is not applicable to the data.")
        else:
            header = format_method_header(config, method, config.filter_rules)
            table = format_result_table(result=result)
            blocks.append(f"{header}\n{table}")
    return "\n\n".join(blocks)


def calc(transactions: pd.DataFrame, config: SimulationConfig) -> str:
    """Load shared config and build the aggregation report text."""
    report = build_aggregation_report(config, transactions)
    logger.info("[calc] Built aggregation report.")
    return report


if __name__ == "__main__":
    print(calc(pd.DataFrame()))
