"""Filter rules applied to transaction records before aggregation."""

import operator as op
from functools import reduce

import pandas as pd

from utils import utils

logger = utils.set_logger(__name__)

# Comparison operators accepted in <field>:<operator>:<value> rules.
_OPERATORS = {
    "==": op.eq,
    "!=": op.ne,
    "<": op.lt,
    "<=": op.le,
    ">": op.gt,
    ">=": op.ge
}


def apply_filter_rules(
    transactions: pd.DataFrame,
    filter_rules: tuple[str, list[tuple[str, str, float]]] | None,
) -> pd.DataFrame:
    """Return transactions matching filter_rules.

    filter_rules is (connector, [(field, operator, value), (field, operator, value), ...])
    where connector is 'and', 'or', or ''. '' means no connector is used.
    """
    if filter_rules is None:
        logger.info("No filter rules; all transaction records are used.")
        return transactions

    connector, rules = filter_rules
    masks = [_build_rule_mask(transactions, field, operator, value) for field, operator, value in rules]
    combined = reduce(lambda x, y: x & y if connector == "and" else x | y, masks) if len(masks) > 1 else masks[0]
    filtered = transactions.loc[combined].reset_index(drop=True)
    logger.info(f"[apply_filter_rules] Kept {len(filtered)}/{len(transactions)} rows with connector={connector!r}.")
    return filtered


def _build_rule_mask(
    transactions: pd.DataFrame,
    field: str,
    operator: str,
    value: float,
) -> pd.Series:
    """Build one boolean mask for a single (field, operator, value) rule."""
    compare_fn = _OPERATORS.get(operator)
    if compare_fn is None:
        logger.error(f"Unsupported operator {operator!r} for field {field!r}.")
    trans_types = transactions["transaction_type"].unique()
    if field not in trans_types:
        logger.warning(f"Field {field} not found in {trans_types}.")
    return (transactions["transaction_type"] == field) & compare_fn(transactions["amount"], value)
