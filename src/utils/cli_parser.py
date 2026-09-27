from datetime import datetime

from utils import utils
utils.ensure_src_on_sys_path()

from data_models.configurations import DateRange
from data_models.configurations import SimulationConfig
from utils.constants import DATETIME_FORMAT


logger = utils.set_logger(__name__)


def run_interactive_cli() -> SimulationConfig:
    """Run the interactive CLI and return one configuration."""
    while True:
        config = collect_simulation_config()
        if confirm_config(config):
            print("Initialization successful. Returning configuration.")
            return config
        print("Initialization failed. Starting over.")
    

def run_interactive_cli_modify(transaction_types: list[str]) -> SimulationConfig:
    """Run the interactive CLI to get new request from the user, and return one configuration.
    """
    while True:
        date_range = prompt_date_range()
        config = SimulationConfig(
            start_datetime=date_range.start_datetime,
            end_datetime=date_range.end_datetime,
            transaction_types=transaction_types, # Remain unchanged
            aggregation_methods=prompt_aggregation_method(),
            filter_rules=prompt_filter_rules()
        )
        if confirm_config(config):
            print("Modification successful. Returning configuration.")
            return config
        print("Modification failed. Starting over.")


def collect_simulation_config() -> SimulationConfig:
    """Run the input steps and return one configuration."""
    date_range = prompt_date_range()
    transaction_types = prompt_transaction_types()
    aggregation_methods = prompt_aggregation_method()
    filter_rules = prompt_filter_rules()
    return SimulationConfig(
        start_datetime=date_range.start_datetime,
        end_datetime=date_range.end_datetime,
        transaction_types=transaction_types,
        aggregation_methods=aggregation_methods,
        filter_rules=filter_rules
    )


def prompt_date_range() -> DateRange:
    """Ask the user for a date range and parse it into a DateRange object."""
    print("Enter a date range in the format yyyy-mm-dd or yyyy-mm-dd HH:MM:SS, and separated by a comma.")
    print("If hour, minutes, and seconds are omitted, they are filled with 0.")
    print("Examples:")
    print("  2026-01-01, 2026-01-31 will be regarded as 2026-01-01 00:00:00 to 2026-01-31 23:59:59")
    print("  2026-01-01 09:30:00, 2026-01-31 18:00:00")
    while True:
        date_range_text = input("Date range: ")
        try:
            date_range: DateRange = _parse_date_range(date_range_text)
            return date_range
        except Exception as e:
            logger.error(f"[prompt_date_range] Failed to parse date range: {e}")
            print(e)


def prompt_transaction_types() -> list[str]:
    """Ask the user for a list of transaction types and parse it into a list of transaction types."""
    print("Enter a list of transaction types separated by commas.")
    print("Examples:")
    print("  income, expense, transfer, deposit, withdrawal")
    while True:
        transaction_types_text = input("Transaction types: ")
        if not transaction_types_text:
            logger.warning(f"[prompt_transaction_types] Transaction types text is empty.")
            print("Invalid input. Please re-enter the transaction types.")
            continue
        transaction_types = [item.strip() for item in transaction_types_text.split(",")]
        return transaction_types


def prompt_aggregation_method() -> list[str]:
    """Ask the user for a list of aggregation methods and parse it into a list of aggregation methods."""
    print("Enter a list of aggregation methods separated by commas.")
    print("Examples:")
    print("  sum, mean, count, std, max, min")
    while True:
        aggregation_method_text = input("Aggregation method: ")
        if not aggregation_method_text:
            logger.warning(f"[prompt_aggregation_method] Aggregation method text is empty.")
            print("Invalid input. Please re-enter the aggregation method.")
            continue
        aggregation_method = [item.strip() for item in aggregation_method_text.split(",")]
        return aggregation_method


def prompt_filter_rules() -> tuple[str, list[tuple[str, str, float]]] | None:
    """Ask for filter rules joined by 'and' (;) or 'or' (,).

    Returns None when unused, otherwise (connector, rules) where connector is
    'and' or 'or' and each rule is (field, operator, value).
    """
    print(
        "(Optional) Enter filter rules as "
        "<field>:<operator>:<value> separated by commas if it is an 'or' clause, "
        "and separated by ';' if it is an 'and' clause. "
        "Please do not use both 'or' and 'and' in the same rule."
    )
    print("Examples:")
    print("  income:<=:1000, expense:>:500")
    print("  amount:>=:100; direction:==:1")
    while True:
        filter_rules_text = input("Aggregation rules: ").strip()
        if not filter_rules_text:
            logger.warning(f"[prompt_filter_rules] Filtering rules is not used.")
            return None
        try:
            return _parse_filter_rules(filter_rules_text)
        except Exception as e:
            logger.warning(f"[prompt_filter_rules] Invalid filter rules: {e}")
            print("Invalid input. Please re-enter the filtering rules.")


def _parse_filter_rules(
    filter_rules_text: str,
) -> tuple[str, list[tuple[str, str, float]]]:
    """Parse one filter expression into (connector, rules)."""
    has_comma = "," in filter_rules_text
    has_semicolon = ";" in filter_rules_text
    if has_comma and has_semicolon:
        raise ValueError("Do not mix ',' (or) and ';' (and) in the same rule.")

    if has_comma:
        connector = "or"
        items = filter_rules_text.split(",")
    elif has_semicolon:
        connector = "and"
        items = filter_rules_text.split(";")
    else:
        # No connector is used.
        connector = ""
        items = [filter_rules_text]

    filter_rules: list[tuple[str, str, float]] = []
    for item in items:
        parts = [part.strip() for part in item.strip().split(":")]
        if len(parts) != 3:
            raise ValueError(
                f"Wrong input {parts}. Each rule must be <field>:<operator>:<value>."
            )
        filter_rules.append((parts[0], parts[1], float(parts[2])))
    return connector, filter_rules


def confirm_config(config: SimulationConfig) -> bool:
    """Show the recap and ask whether to proceed."""
    print(utils.format_config_recap(config))
    answer = input("Proceed (default: yes)? [Y/n]: ").strip().lower()
    return answer in ("", "y", "yes")


def _parse_datetime(content: str) -> datetime:
    """Parse a datetime string into a datetime object."""
    try:
        return datetime.strptime(content.strip(), DATETIME_FORMAT)
    except Exception as e:
        logger.warning(f"[_parse_datetime] Failed to parse datetime: {e}")
        raise ValueError(f"Failed to parse datetime: {e}")


def _parse_date(content: str) -> datetime:
    """Parse a date string into a datetime object."""
    try:
        content = content.strip() + " 00:00:00"
        return datetime.strptime(content, DATETIME_FORMAT)
    except Exception as e:
        logger.warning(f"[_parse_date] Failed to parse date: {e}")
        raise ValueError(f"Failed to parse date: {e}")


def _parse_date_range(content: str) -> DateRange:
    """Parse a date range string into a DateRange object."""
    try:
        start_datetime, end_datetime = content.strip().split(",")
        return DateRange(
            start_datetime=_parse_datetime(start_datetime),
            end_datetime=_parse_datetime(end_datetime))
    except:
        try:
            start_datetime, end_datetime = content.split(",")
            return DateRange(
                start_datetime=_parse_date(start_datetime),
                end_datetime=_parse_date(end_datetime))
        except Exception as e:
            logger.error(f"[_parse_date_range] Failed to parse date range: {e}")
            raise ValueError(f"Failed to parse date range: {e}")


if __name__ == "__main__":
    config = run_interactive_cli()
    start_datetime: datetime = config.start_datetime
    end_datetime: datetime = config.end_datetime
    transaction_types: list[str] = config.transaction_types
    aggregation_methods: list[str] = config.aggregation_methods
    filter_rules: tuple[str, list[tuple[str, str, float]]] | None = config.filter_rules
    print(f"Start datetime: {start_datetime}")
    print(f"End datetime: {end_datetime}")
    print(f"Transaction types: {transaction_types}")
    print(f"Aggregation methods: {aggregation_methods}")
    print(f"Filter rules: {filter_rules}")
