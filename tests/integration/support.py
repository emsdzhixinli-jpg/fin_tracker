"""Helpers for integration tests (no pytest fixtures)."""

import time
from datetime import datetime
from pathlib import Path

from data_models.configurations import SimulationConfig


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"


def sample_config(**overrides) -> SimulationConfig:
    """Default simulation config for integration runs."""
    values = dict(
        start_datetime=datetime(2026, 8, 1, 0, 0, 0),
        end_datetime=datetime(2026, 8, 10, 0, 0, 0),
        transaction_types=["deposit", "transfer"],
        aggregation_methods=["sum"],
        filter_rules=None,
    )
    values.update(overrides)
    return SimulationConfig(**values)


def wait_until(predicate, timeout: float = 30.0, interval: float = 0.25) -> bool:
    """Poll until predicate() is true or timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def mirror_config_for_calc(config: SimulationConfig, data_config_path: Path) -> None:
    """Write config JSON used by legacy calc() paths that read data/simulation_config.json."""
    data_config_path.write_text(config.model_dump_json(indent=4))
