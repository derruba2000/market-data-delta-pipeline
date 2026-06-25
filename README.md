# FinancePipe

FinancePipe is a small market-data pipeline that reads a configurable list of
symbols, fetches daily candles from Yahoo Finance, and merges the results into
local Delta Lake tables using [dlt](https://dlthub.com/).

It keeps market tickers and currency pairs in separate tables while giving both
the same schema, making the output straightforward to query and extend.

## Features

- Loads equity, ETF, fund, and forex symbols from CSV reference files
- Cleans whitespace, empty values, and duplicate symbols before extraction
- Fetches either the latest one-day candle or an inclusive date interval
- Supports command-line symbol overrides for market tickers and FX pairs
- Writes native Delta tables to a configurable filesystem directory
- Upserts records using `symbol` and `date` as a composite primary key
- Skips symbols for which Yahoo Finance returns no data
- Supports local runs and scheduled weekday runs with GitHub Actions

## How It Works

```text
reference_tickers.csv ────> market_tickers ──┐
                                             ├──> Delta Lake
reference_currencies.csv ─> currencies ──────┘
                   Yahoo Finance + dlt
```

Each resource is loaded independently with merge semantics. A failed or empty
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

## Date Range

With no date arguments, FinancePipe preserves its default behavior and fetches
the latest available one-day candle:

```bash
poetry run python market_pipeline.py
```

To fetch an interval, provide both dates in `YYYY-MM-DD` format. Both boundaries
are inclusive:

```bash
poetry run python market_pipeline.py \
  --from-date 2026-01-01 \
  --to-date 2026-01-31
```

Camel-case aliases are also accepted:

```bash
poetry run python market_pipeline.py \
  --fromDate 2026-01-01 \
  --toDate 2026-01-31
```

Both arguments must be supplied together, and `from-date` cannot be later than
`to-date`. Non-trading days within the interval simply produce no candles.

## Symbol Overrides

By default, FinancePipe extracts every symbol in the two reference CSV files.
Use `--symbols` to replace the market ticker list for one run:

```bash
poetry run python market_pipeline.py --symbols AAPL MSFT SPY
```

Use `--fx-symbols` to replace the currency list:

```bash
poetry run python market_pipeline.py --fx-symbols EURUSD=X GBPUSD=X
```

Values may also be comma-separated, and both overrides can be combined with a
date interval:

```bash
poetry run python market_pipeline.py \
  --symbols AAPL,MSFT \
  --fx-symbols EURUSD=X,GBPUSD=X \
  --from-date 2026-01-01 \
  --to-date 2026-01-31
```

Each override affects only its own category. For example, when only `--symbols`
is provided, FX pairs are still loaded from `reference_currencies.csv`.
Duplicate and blank command-line values are removed.

## SQLite-backed Pipeline

`sqlite_market_pipeline.py` provides the same Yahoo Finance and Delta Lake
loading behavior while also integrating with an existing SQLite database.
Pass the database path with `--sqlite-db`:

```bash
poetry run python sqlite_market_pipeline.py \
  --sqlite-db ./data/finance.db
```

The pipeline:

- Reads market symbols and IDs from `securities`
- Ignores rows whose `asset_class` is `CASH` (case-insensitive)
- Continues to read FX symbols from `reference_currencies.csv`, unless
  `--fx-symbols` is supplied
- Merges market and FX candles into the existing Delta tables
- Upserts market candles into `price_history` using `(security_id, date)`
- Upserts FX candles into `fx_rate_history` using
  `(base_currency_code, quote_currency_code, date)`
- Logs missing symbols, extraction failures, and Delta loading failures in
  `import_error_logs`, then continues processing other symbols/resources

Yahoo FX symbols must identify a six-letter pair, such as `EURUSD=X`, so the
pipeline can populate the base and quote currency columns. Date arguments and
FX overrides work exactly as they do in `market_pipeline.py`:

```bash
poetry run python sqlite_market_pipeline.py \
  --sqlite-db ./data/finance.db \
  --fx-symbols EURUSD=X GBPUSD=X \
  --from-date 2026-01-01 \
  --to-date 2026-01-31
```

## Security Reference Pipeline

`security_reference_pipeline.py` reads the same
`reference_tickers.csv` file and stores normalized Yahoo Finance reference data
in an existing SQLite database:

```bash
poetry run python security_reference_pipeline.py \
  --sqlite-db /Users/joaoramo/Data/trading_experiment/portfolio_management.sqlite3
```

Use `--symbols` to override the reference file for one run:

```bash
poetry run python security_reference_pipeline.py \
  --sqlite-db ./data/finance.db \
  --symbols MSFT AAPL VWRP.L
```

By default, the nearest option expiration is loaded when options are available.
Change the number of expirations or disable option extraction:

```bash
# Load the nearest three expirations.
poetry run python security_reference_pipeline.py \
  --sqlite-db ./data/finance.db \
  --options-expirations 3

# Skip options.
poetry run python security_reference_pipeline.py \
  --sqlite-db ./data/finance.db \
  --options-expirations 0
```

The pipeline creates and refreshes these normalized tables:

| Table | Contents |
| --- | --- |
| `yahoo_security_snapshots` | Common ticker profile fields and the raw info payload |
| `yahoo_security_info` | Typed general-info attributes |
| `yahoo_calendar_events` | Earnings, dividend, and other calendar values |
| `yahoo_analyst_targets` | Analyst current, high, low, mean, and median targets |
| `yahoo_financial_facts` | Annual and quarterly financial-statement facts |
| `yahoo_option_contracts` | Calls and puts for the requested expirations |
| `yahoo_fund_profiles` | Fund family, category, structure, fees, and description |
| `yahoo_fund_asset_allocation` | Stock, bond, cash, and other allocations |
| `yahoo_fund_holdings` | Ranked top holdings and portfolio weights |
| `yahoo_fund_sector_weightings` | Economic-sector exposure |
| `yahoo_fund_metrics` | Fund operations and equity/bond statistics |
| `yahoo_fund_performance` | Trailing, annual, and category-relative returns |

Every table uses the original Yahoo `symbol` as part of its key. Each ticker is
updated in one SQLite transaction, and a failure for one symbol does not stop
the remaining symbols.

Daily historical prices remain the responsibility of `market_pipeline.py` and
`sqlite_market_pipeline.py`. Yahoo WebSocket `live()` data is streaming data,
so it is intentionally not included in this repeatable snapshot pipeline.

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

FinancePipe uses Delta Lake with dlt's `merge`/`upsert` strategy. The composite
primary key is:

```text
(symbol, date)
```

Running the same day or date interval repeatedly updates matching records and
inserts missing records instead of creating duplicates.

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
