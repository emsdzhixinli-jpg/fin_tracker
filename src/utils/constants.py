# Date and time formats
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"

# Transaction configurations
COUNTERPARTIES = "ABCDEFGHIJ"
DIRECTIONS = ("+", "-")
JSON_FILENAME = "simulation_config.json"
TRANSACTION_COLUMNS = (
    "transaction_datetime",
    "transaction_type",
    "counterparty",
    "direction",
    "amount",
)
MIN_RECORDS_COUNT=5
MAX_RECORDS_COUNT=20

# Transaction producer
RND_MIN_SECONDS = 3
RND_MAX_SECONDS = 30
