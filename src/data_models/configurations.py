from datetime import datetime
from pydantic import BaseModel
from typing import Optional


class SimulationConfig(BaseModel):
    """User settings for generating bank transactions and later aggregations."""
    start_datetime: datetime
    end_datetime: datetime
    transaction_types: list[str]
    aggregation_methods: list[str]
    # (connector, [(field, operator, value), (field, operator, value), ...])
    filter_rules: Optional[tuple[str, list[tuple[str, str, float]]]]


class DateRange(BaseModel):
    """A range of dates."""
    start_datetime: datetime
    end_datetime: datetime
