import fcntl
import functools
import json
import logging
import os
import sys
import time
from pathlib import Path

from data_models.configurations import SimulationConfig
from utils.constants import DATE_FORMAT
from utils.constants import DATETIME_FORMAT
from utils.constants import JSON_FILENAME


def set_logger(name: str) -> logging.Logger:
    """Set up a logger for the project."""
    log_type = logging.INFO
    logging.basicConfig(
        level=log_type,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger(name)
    return logger


def ensure_src_on_sys_path() -> Path:
    """Insert the src directory at the front of sys.path when it is missing."""
    src_path = Path(__file__).resolve().parents[2] / "src"
    src_path_str = str(src_path)
    if src_path_str not in sys.path:
        sys.path.insert(0, src_path_str)
    return src_path


def format_config_to_dict(config: SimulationConfig) -> dict:
    types = ", ".join(config.transaction_types)
    input_config = {
        "start_datetime": config.start_datetime.strftime(DATETIME_FORMAT),
        "end_datetime": config.end_datetime.strftime(DATETIME_FORMAT),
        "transaction_types": types,
        "aggregation_methods": config.aggregation_methods,
        "filter_rules": config.filter_rules,
    }
    return input_config


def format_config_recap(config: SimulationConfig) -> str:
    """Format the collected settings for the confirmation step."""
    config_info: dict = format_config_to_dict(config)
    lines = "Review your settings:\n"
    for k, v in config_info.items():
        lines += f"  {k}: {v}\n"
    return lines.strip()


def _config_lock_path(path: Path) -> Path:
    """Sidecar lock file so readers and writers share one flock inode."""
    return path.with_name(path.name + ".lock")


def read_text_locked(path: Path) -> str:
    """Read a file under a shared flock.

    Writers using write_text_locked hold the matching exclusive lock, so this
    waits until a config replace has finished and then sees the full file.
    """
    lock_path = _config_lock_path(path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a+") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_SH)
        try:
            return path.read_text()
        except Exception as e:
            logging.getLogger(__name__).error(f"Error reading file {path}: {e}")
            raise
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def wait_and_read_text_locked(
    path: Path,
    timeout_seconds: float = 600,
    poll_seconds: float = 1.0,
) -> str:
    """Wait until path exists, then read it under the shared flock.

    Used by the consumer so it does not start before the producer finishes the
    interactive CLI and writes simulation_config.json.
    """
    deadline = time.monotonic() + timeout_seconds
    while True:
        if path.is_file():
            return read_text_locked(path)
        if time.monotonic() >= deadline:
            raise FileNotFoundError(
                f"Timed out after {timeout_seconds:.0f}s waiting for {path}. "
                "Finish the producer config CLI so the shared JSON is written."
            )
        time.sleep(poll_seconds)


def write_text_locked(path: Path, content: str) -> None:
    """Replace a file under an exclusive flock.

    The new content is written to a temp file and moved into place with
    os.replace while the lock is held, so another process cannot observe a
    partial JSON document.
    """
    lock_path = _config_lock_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a+") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            temporary = path.with_name(path.name + ".tmp")
            temporary.write_text(content)
            os.replace(temporary, path)
        except Exception as e:
            logging.getLogger(__name__).error(f"Error writing file {path}: {e}")
            raise
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def write_config_to_json(
    config: SimulationConfig,
    json_file_name: str = JSON_FILENAME,
    output_path: Path = Path(__file__).resolve().parents[2] / "data",
) -> None:
    """Serialize SimulationConfig to JSON under the shared file lock."""
    payload = json.dumps(config.model_dump(mode="json"), indent=4)
    write_text_locked(output_path / json_file_name, payload)
    logging.getLogger(__name__).info(
        f"[write_config_to_json] Stored config to {output_path / json_file_name}."
    )


def timer(func):
    """Log how long a function takes to run (perf_counter, milliseconds)."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        # Convert seconds to milliseconds for finer-grained timing logs.
        elapsed_ms = (time.perf_counter() - start) * 1000
        logging.getLogger(func.__module__).info(
            f"[{func.__name__}] completed in {elapsed_ms:.2f}ms"
        )
        return result
    return wrapper
