# FinTransTracker

A small **personal financial tracker** simulation. You define how “historical” transactions should look via initial configuration and subsequential updates via consumer CLI.

- The producer generates and streams new events periodically (random) and send the new transaction notification over Kafka.
- The consumer lets you change analysis settings (config) on demand and view aggregation reports or raw transaction details.

---

## Purpose

This tool mimics a personal financial tracker that **analyzes cash-flow data according to user input**:

- **Producer**: Builds an in-memory transaction history from your initial settings, then periodically publishes new transactions (like live bank events).
- **Consumer**: Subscribes to those events, lets you **reload configuration** (date range, aggregation methods, filter rules), and prints **aggregation tables** or a **transaction snapshot** on request.

It is designed as an **event-driven** demo: Kafka carries control messages and notifications between two Python processes, while `data/simulation_config.json` shares configuration under a file lock. (In reality, a database like MongoDB should be used to cater for distributed system.)

---



## User guide

1. Enter the **initial configuration** in the **producer** terminal first. That defines the simulated historical window, which transaction types exist in the simulation, and the first aggregation/filter settings.
2. Start the **consumer** in a second terminal after the shared JSON file exists.
3. Use `Y` to re-run analysis with new dates, aggregations, or filters; use `d` to inspect raw rows the producer has published up to its snapshot time; use `n` to quit the consumer service.
4. You can modify `.env` to modify the Kakfa service connection (used in a few places).

---



## Assumptions

1. Financial transactions mimic real-life **events**. The tool lets a single user monitor transactions **in near real time** or run **on-demand (EOD-style)** analysis via the consumer. **(Event-driven design.)**
2. The service is ONLY for one user for simplicity.
3. Transactions are simplified to a small set of fields that matter for cash-flow tracking: datetime, type, counterparty, direction, and amount.
4. Shared file `simulation_config.json` is a simple, fast choice for this demo. In production, a database (for example MongoDB) would better suit a distributed deployment and avoid torn reads/writes. Here, `fcntl` **file locks** on a sidecar `*.lock` file coordinate readers and writers.
5. **Filter rules** apply to **transaction types and amounts** (as rule fields such as `income`, `expense`, `amount`), **not** to transaction **direction** (`+` / `-`).

---



## Requirements


| Requirement             | Notes                                             |
| ----------------------- | ------------------------------------------------- |
| **Docker**              | Runs Kafka and topic creation (`docker compose`). |
| **Python 3.11+**        | Application code and tests.                       |
| **Virtual environment** | Recommended; project assumes `./venv`.            |


Install Python dependencies:

```bash
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Copy or edit `.env` if needed. Host processes use `KAFKA_BOOTSTRAP_SERVERS=localhost:9092` (default).

---



## How to use

Use **three terminals** (or run producer/consumer in Docker; see `docker-compose.yml`).

### 1. Start Kafka

```bash
make kafka
```

This starts the broker and creates topics `transactions` and `config_sync`.

### 2. Start the producer (terminal 1)

```bash
make producer
```

You will be prompted for the **initial configuration** (see [Expected input](#expected-input)). After you confirm, the producer:

1. Writes `data/simulation_config.json`.
2. Publishes an initial aggregation report.
3. Starts generating new transactions at random intervals (about 30–180 seconds/transaction) at the background.



### Run tests

Tests use `Makefile.test` and the same `venv` / `PYTHONPATH=src` as the app. From the project root:


| Command                             | What it runs                                                                                                              | Kafka required?              |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------- | ---------------------------- |
| `make -f Makefile.test unit`        | Unit tests in `tests/test_process_transactions.py`, `tests/test_producer.py`, and `tests/test_consumer.py` (Kafka mocked) | No                           |
| `make -f Makefile.test integration` | Integration tests under `tests/integration/` (`pytest -m integration`)                                                    | Yes — run `make kafka` first |
| `make -f Makefile.test all`         | Unit tests, then integration tests                                                                                        | Integration part needs Kafka |
| `make -f Makefile.test help`        | Lists the targets above                                                                                                   | —                            |


Examples:

```bash
make -f Makefile.test unit
make kafka
make -f Makefile.test integration
make -f Makefile.test all
```

Integration tests expect a broker at `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092` from `.env`).

### 3. Start the consumer (terminal 2)

```bash
make consumer
```

The consumer waits until `simulation_config.json` exists, then shows:

```text
Next track? [Y/n/d] (Y - modify config, n - quit consumer, d - view transaction details):
```


| Key                 | Action                                                                                                                                                                                                  |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Y** / Enter       | Open the **modify config** CLI (date range, aggregation methods, filter rules). Transaction types stay as set at producer startup. Sends a reload to the producer and prints an **aggregation report**. |
| **d** / **details** | Request a **transaction snapshot** (rows with datetime ≤ producer `snapshot_datetime`). Queued “new transaction” notices are shown after the table.                                                     |
| **n**               | Exit the consumer.                                                                                                                                                                                      |




### 4. Stop Kafka (optional)

```bash
make kafka-down
```

---



## Expected input

Prompts are interactive; formats match `src/utils/cli_parser.py`.

### Initial configuration (producer)

1. **Date range** — comma-separated start and end.
  - `yyyy-mm-dd` or `yyyy-mm-dd HH:MM:SS`  
  - Example: `2026-08-01,2026-08-31` → start `2026-08-01 00:00:00`, end `2026-08-31 23:59:59`.
2. **Transaction types** — comma-separated, e.g. `income,expense,transfer`
  (also supported: `deposit`, `withdrawal`).
3. **Aggregation methods** — comma-separated, e.g. `sum,mean,count`
  (also: `std`, `max`, `min`).
4. **Filter rules (optional)** — `<field>:<operator>:<value>`
  - **OR** within a group: separate with `,`  
  - **AND**: separate with `;`  
  - Do not mix `,` and `;` in one rule set.  
  - Example: `income:>=:300;income:<:6800`
5. Confirm with **Y** (default) or restart if you answer **n**.



### Modify configuration (consumer)

Same as above for **date range**, **aggregation methods**, and **filter rules**. **Transaction types are not changed** on modify (they remain the list from producer initialization).

---



## Expected output



### Aggregation report

After producer startup or after a successful consumer config modify, you see bordered tables per aggregation method. Example from a run with date range `2026-08-01`–`2026-08-31`, methods `sum,max,min`, and filter `income:>=:300;income:<:6800` (amounts vary by seed and live publishes):

```text
The sum of the transactions between 2026-08-01 00:00:00 and 2026-08-31 00:00:00 are as follows, with filter rules (income:>=:300.0 and income:<:6800.0):
-----------------------------------------------------------------------------------
| Transaction Type   | Directions   | Amount             | Counterparties Count   ||
-----------------------------------------------------------------------------------
| income             | +            | 9208.326164757958  | 2                      |
| income             | -            | 6176.3919972178965 | 1                      |
-----------------------------------------------------------------------------------

The max of the transactions between 2026-08-01 00:00:00 and 2026-08-31 00:00:00 are as follows, with filter rules (income:>=:300.0 and income:<:6800.0):
-----------------------------------------------------------------------------------
| Transaction Type   | Directions   | Amount             | Counterparties Count   ||
-----------------------------------------------------------------------------------
| income             | +            | 5330.969527065925  | 2                      |
| income             | -            | 6176.3919972178965 | 1                      |
-----------------------------------------------------------------------------------

The min of the transactions between 2026-08-01 00:00:00 and 2026-08-31 00:00:00 are as follows, with filter rules (income:>=:300.0 and income:<:6800.0):
-----------------------------------------------------------------------------------
| Transaction Type   | Directions   | Amount             | Counterparties Count   ||
-----------------------------------------------------------------------------------
| income             | +            | 3877.356637692033  | 2                      |
| income             | -            | 6176.3919972178965 | 1                      |
-----------------------------------------------------------------------------------
```



### Live notices

While the consumer is idle at the prompt, new transactions may print:

```text
[TransactionConsumer] Received new transaction.
```

During config modify or **d** (details), notices are **queued** and printed in one batch afterward, e.g. `[TransactionConsumer] #3 Received new transaction.`

### Transaction details (`d`)

A plain-text table of columns: `transaction_datetime`, `transaction_type`, `counterparty`, `direction`, `amount`.

---



## Test examples

Values below are **illustrative**. Amounts and row counts depend on the random seed and live publishes—**adjust dates and filter thresholds to match your actual simulated data. You can use it to test the CLI tool.**

### Example 1 — Initial setup (producer)

```text
# Date range
2026-08-01,2026-08-31

# Transaction types
income,expense,transfer

# Aggregation methods
sum,mean,count

# Filter rules (optional)
income:>=:300;income:<:6800
```



### Example 1 — Modify via consumer

Transaction types stay `income,expense,transfer` from initialization.

```text
# Date range
2026-08-15 00:00:00,2026-08-30 10:18:00

# Aggregation methods
max,min

# Filter rules (optional)
expense:>=:1000;expense:<:3000
```

**Please remember to modify inputs according to the real data in your run.**

---



## Project structure

```text
FinTransTracker/
├── data/
│   └── simulation_config.json      # Shared config (file lock: *.lock)
├── docs/
│   └── fintrans-kafka-system-structure.png
├── src/
│   ├── config/
│   │   ├── kafka_consumer_config.yml
│   │   └── kafka_producer_config.yml
│   ├── data_models/
│   │   └── configurations.py       # SimulationConfig (Pydantic)
│   ├── scripts/
│   │   └── process_transactions.py # Filtering + aggregation (calc)
│   ├── services/
│   │   ├── producer.py             # Generate/publish transactions; config sync listener
│   │   └── consumer.py             # Notices; config modify; snapshot request
│   └── utils/
│       ├── cli_parser.py           # Interactive prompts
│       ├── constants.py
│       ├── kafka_config.py
│       ├── rules.py                # Filter rule application
│       └── utils.py                # Config JSON I/O with flock
├── tests/
│   ├── integration/                # Kafka integration tests
│   ├── test_consumer.py
│   ├── test_producer.py
│   └── test_process_transactions.py
├── docker-compose.yml
├── Makefile                        # kafka, producer, consumer
├── Makefile.test                   # unit / integration tests
├── requirements.txt
└── README.md
```

**Kafka topics**


| Topic          | Role                                                                        |
| -------------- | --------------------------------------------------------------------------- |
| `config_sync`  | Consumer → producer: `reload_config`, `request_snapshot`                    |
| `transactions` | Producer → consumer: notices, `aggregation_report`, `transactions_snapshot` |


---



## Data flow

FinTransTracker data flow

The diagram summarizes three paths. Below is how the **running code** maps to each part (producer on the left, consumer on the right, Kafka in the middle).

### 1. Configuration sync

1. You change settings in the **consumer** CLI; the consumer writes `simulation_config.json` under an exclusive file lock.
2. The consumer sends `reload_config` with a `request_id` on `config_sync`.
3. The **producer** listener reloads the JSON, filters transactions by the new date range, runs `calc`, and publishes an `aggregation_report` on `transactions` with the same `request_id`.
4. The **consumer** listener matches `request_id`, unblocks the waiting prompt, and prints the report.

This keeps analysis settings in sync without restarting the producer process.

### 2. Transaction workflow (live notices)

1. The **producer** appends a row to its in-memory table and publishes a `transaction_notice` on `transactions`.
2. The **consumer** prints the notice (or queues it while you are in modify/details mode).

This mimics real-time monitoring of new financial events.

### 3. Transaction detail workflow

1. You press `d` in the consumer. It sends `request_snapshot` with a `request_id` on `config_sync`.
2. The **producer** filters rows with `transaction_datetime <= snapshot_datetime`, serializes them, and publishes `transactions_snapshot` on `transactions`.
3. The **consumer** rebuilds a DataFrame, prints the table, then flushes any queued notices.

Request and reply use the same `request_id` so concurrent prompts do not cross wires.

---



## License

This project is licensed under the [MIT License](LICENSE).