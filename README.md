# FinancePipe

FinancePipe is a small daily market-data pipeline that reads a configurable list
of symbols, fetches their latest candle from Yahoo Finance, and appends the
results to local Delta Lake tables using [dlt](https://dlthub.com/).

It keeps market tickers and currency pairs in separate tables while giving both
the same schema, making the output straightforward to query and extend.

## Features

- Loads equity, ETF, fund, and forex symbols from CSV reference files
- Cleans whitespace, empty values, and duplicate symbols before extraction
- Fetches the latest one-day OHLCV candle through `yfinance`
- Writes native Delta tables to a configurable filesystem directory
- Skips symbols for which Yahoo Finance returns no data
- Supports local runs and scheduled weekday runs with GitHub Actions

## How It Works

```text
reference_tickers.csv ────> market_tickers ──┐
                                             ├──> Delta Lake
reference_currencies.csv ─> currencies ──────┘
                   Yahoo Finance + dlt
```

Each resource is loaded independently with append semantics. A failed or empty
response for one symbol does not prevent the remaining symbols from being
processed.

## Requirements

- Python 3.12
- Internet access to Yahoo Finance
- Poetry or `pip`

## Quick Start

Clone the repository, create the local configuration, install the dependencies,
and run the pipeline:

```bash
poetry env use 3.12
poetry install
printf 'REFERENCE_DATA_DIR=./data/reference\nTARGET_DELTA_DIR=./data/delta_lake\n' > .env
poetry run python market_pipeline.py
```

To use `pip` instead:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
printf 'REFERENCE_DATA_DIR=./data/reference\nTARGET_DELTA_DIR=./data/delta_lake\n' > .env
python market_pipeline.py
```

Successful runs print a dlt load summary for each non-empty resource.

## Configuration

FinancePipe loads configuration from environment variables and automatically
reads a `.env` file when one is present.

| Variable | Description | Example |
| --- | --- | --- |
| `REFERENCE_DATA_DIR` | Directory containing the two input CSV files | `./data/reference` |
| `TARGET_DELTA_DIR` | Directory in which dlt creates the Delta dataset | `./data/delta_lake` |

Create a `.env` file in the project root:

```dotenv
REFERENCE_DATA_DIR=./data/reference
TARGET_DELTA_DIR=./data/delta_lake
```

Both variables are required. The pipeline exits with status `1` and logs a
clear error when either value or either reference file is missing.

## Reference Data

Both CSV files must contain a column named `symbol`.

`data/reference/reference_tickers.csv`:

```csv
symbol
AAPL
MSFT
SPY
```

`data/reference/reference_currencies.csv`:

```csv
symbol
EURUSD=X
USDJPY=X
GBPUSD=X
```

Symbols must use Yahoo Finance's ticker format. Edit either file to change what
the next run extracts.

## Output

The pipeline creates the `financepipe` dataset beneath `TARGET_DELTA_DIR` and
loads two Delta tables:

| Table | Source file | Intended data |
| --- | --- | --- |
| `market_tickers` | `reference_tickers.csv` | Equities, ETFs, and funds |
| `currencies` | `reference_currencies.csv` | Yahoo Finance forex pairs |

Both tables use the following record shape:

| Column | Description |
| --- | --- |
| `symbol` | Yahoo Finance symbol |
| `date` | Candle date in ISO `YYYY-MM-DD` format |
| `open` | Opening price |
| `high` | Highest price |
| `low` | Lowest price |
| `close` | Closing price |
| `volume` | Trading volume, when provided |

The exact filesystem layout also includes metadata managed by dlt and Delta
Lake. Treat the configured target directory as pipeline-managed storage.

## Load Behavior

FinancePipe uses `write_disposition="append"` and `table_format="delta"`.
Running it repeatedly is safe for the table structure, but it does not currently
deduplicate records. Multiple runs on the same trading day can therefore append
the same symbol and date more than once.

Yahoo Finance can return no candle for an invalid symbol, a market holiday, or
a temporary data delay. FinancePipe logs a warning and skips that symbol.

## Scheduled Runs

The workflow in `.github/workflows/daily_extraction.yml` runs:

- At 22:00 UTC every Monday through Friday
- On demand through GitHub's **Run workflow** action

The workflow uses Python 3.12, installs `requirements.txt`, and runs the pipeline
with repository-relative data directories.

> [!NOTE]
> Files written to `./data/delta_lake` on a GitHub-hosted runner disappear when
> the job ends. Add an artifact-upload step or configure persistent cloud
> storage before relying on scheduled runs for durable data.

## Project Structure

```text
FinancePipe/
├── .github/
│   └── workflows/
│       └── daily_extraction.yml
├── data/
│   └── reference/
│       ├── reference_currencies.csv
│       └── reference_tickers.csv
├── spec/
│   └── spec_requirements.md
├── market_pipeline.py
├── pyproject.toml
├── requirements.txt
└── README.md
```

## Troubleshooting

**Missing environment variables**

Create a `.env` file in the project root and ensure both required values are
non-empty.

**Reference file not found**

Check that `REFERENCE_DATA_DIR` points to a directory containing
`reference_tickers.csv` and `reference_currencies.csv`.

**Missing `symbol` column**

The header is case-sensitive and must be exactly `symbol`.

**No data for a symbol**

Confirm the symbol on Yahoo Finance and use its expected suffix or pair format,
such as `EURUSD=X` for forex.
