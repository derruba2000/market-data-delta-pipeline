# FinancePipe

FinancePipe is a collection of market-data pipelines that fetch daily candles
from Yahoo Finance, maintain security reference data, and load curated ETF
lists into a local SQLite database and Delta Lake storage.

## Pipelines

| Script | Purpose | Output |
| --- | --- | --- |
| [`market_pipeline.py`](#1-market_pipelinepy) | Fetch daily candles for equities, ETFs, and FX pairs | Delta Lake tables |
| [`sqlite_market_pipeline.py`](#2-sqlite_market_pipelinepy) | Same as above, plus upsert candles into SQLite `price_history` / `fx_rate_history` | Delta Lake + SQLite |
| [`security_reference_pipeline.py`](#3-security_reference_pipelinepy) | Pull normalized Yahoo Finance reference data for every security | SQLite snapshot tables |
| [`morningstar_etf_pipeline.py`](#4-morningstar_etf_pipelinepy) | Load Morningstar ETF CSV exports into the `securities` table | SQLite `securities` |

---

## Requirements

- Python 3.12
- Internet access to Yahoo Finance (pipelines 1–3)
- Poetry or `pip`

### Install with Poetry

```bash
poetry env use 3.12
poetry install
```

### Install with pip

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Configuration

Pipelines 1 and 2 read two environment variables and automatically load a
`.env` file from the project root when one is present.

| Variable | Description | Example |
| --- | --- | --- |
| `REFERENCE_DATA_DIR` | Directory containing the input CSV reference files | `./data/reference` |
| `TARGET_DELTA_DIR` | Directory in which dlt creates the Delta dataset | `./data/delta_lake` |

Create a `.env` file in the project root:

```dotenv
REFERENCE_DATA_DIR=./data/reference
TARGET_DELTA_DIR=./data/delta_lake
```

Both variables are required. The pipeline exits with a clear error when either
value or either reference file is missing.

### Reference CSV files

Both files must contain a column named `symbol`.

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

---

## 1. market_pipeline.py

Fetches daily OHLCV candles from Yahoo Finance for every symbol in the two
reference files and merges the results into local Delta Lake tables.

```text
reference_tickers.csv ────> market_tickers ──┐
                                             ├──> Delta Lake
reference_currencies.csv ─> currencies ──────┘
                   Yahoo Finance + dlt
```

Each resource is loaded independently; a failed response for one symbol does
not prevent the remaining symbols from being processed.

### Run

```bash
# Latest one-day candle for all reference symbols (CSV mode)
poetry run python market_pipeline.py

# Read symbols from the securities table of an existing SQLite database
poetry run python market_pipeline.py \
  --sqlite-db /path/to/portfolio_management.sqlite3

# Inclusive date range with database symbols
poetry run python market_pipeline.py \
  --sqlite-db /path/to/portfolio_management.sqlite3 \
  --from-date 2026-01-01 \
  --to-date   2026-01-31

# Camel-case aliases are accepted
poetry run python market_pipeline.py \
  --fromDate 2026-01-01 \
  --toDate   2026-01-31
```

When `--sqlite-db` is provided, the pipeline reads every non-`CASH` ticker from
the `securities` table (ordered by ticker) instead of `reference_tickers.csv`.
Passing `--symbols` alongside `--sqlite-db` overrides the database and uses the
explicit list instead.

### Symbol overrides

```bash
# Replace the ticker list for one run
poetry run python market_pipeline.py --symbols AAPL MSFT SPY

# Replace the FX list for one run
poetry run python market_pipeline.py --fx-symbols EURUSD=X GBPUSD=X

# Combine overrides with a date range (comma-separated values also work)
poetry run python market_pipeline.py \
  --symbols    AAPL,MSFT \
  --fx-symbols EURUSD=X,GBPUSD=X \
  --from-date  2026-01-01 \
  --to-date    2026-01-31
```

An override affects only its own category; unoverridden categories still read
from the reference file.

### Delta output

| Table | Source | Contents |
| --- | --- | --- |
| `market_tickers` | `reference_tickers.csv` | Equities, ETFs, funds |
| `currencies` | `reference_currencies.csv` | Yahoo Finance forex pairs |

Both tables share the same schema:

| Column | Description |
| --- | --- |
| `symbol` | Yahoo Finance symbol |
| `date` | Candle date (`YYYY-MM-DD`) |
| `open` | Opening price |
| `high` | Daily high |
| `low` | Daily low |
| `close` | Closing price |
| `volume` | Trading volume |

The merge key is `(symbol, date)`.

---

## 2. sqlite_market_pipeline.py

Provides the same Delta Lake behavior as `market_pipeline.py` while also
upserting candles into an existing SQLite database.

### Run

```bash
# Fetch today's candles and upsert into SQLite
poetry run python sqlite_market_pipeline.py \
  --sqlite-db /path/to/portfolio_management.sqlite3

# With a date range and FX overrides
poetry run python sqlite_market_pipeline.py \
  --sqlite-db /path/to/portfolio_management.sqlite3 \
  --fx-symbols EURUSD=X GBPUSD=X \
  --from-date  2026-01-01 \
  --to-date    2026-01-31
```

### What it does

- Reads market symbols and their IDs from the `securities` table
- Ignores rows whose `asset_class` is `CASH` (case-insensitive)
- Reads FX symbols from `reference_currencies.csv` unless `--fx-symbols` is
  supplied
- Merges candles into Delta tables (same as `market_pipeline.py`)
- Upserts market candles into `price_history` on `(security_id, date)`
- Upserts FX candles into `fx_rate_history` on
  `(base_currency_code, quote_currency_code, date)`
- Logs failures in `import_error_logs` and continues with remaining symbols

Yahoo FX symbols must be six-letter pairs such as `EURUSD=X` so the pipeline
can split them into base and quote currency columns.

---

## 3. security_reference_pipeline.py

Pulls normalized reference data from Yahoo Finance for every ticker in
`reference_tickers.csv` and stores it in snapshot tables inside an existing
SQLite database.

### Run

```bash
# Load all reference symbols
poetry run python security_reference_pipeline.py \
  --sqlite-db /path/to/portfolio_management.sqlite3

# Override the symbol list for one run
poetry run python security_reference_pipeline.py \
  --sqlite-db /path/to/portfolio_management.sqlite3 \
  --symbols MSFT AAPL VWRP.L

# Control option-chain extraction (default: 1 nearest expiration)
poetry run python security_reference_pipeline.py \
  --sqlite-db /path/to/portfolio_management.sqlite3 \
  --options-expirations 3   # load nearest three expirations

poetry run python security_reference_pipeline.py \
  --sqlite-db /path/to/portfolio_management.sqlite3 \
  --options-expirations 0   # skip options entirely
```

### Tables populated

| Table | Contents |
| --- | --- |
| `yahoo_security_snapshots` | Common ticker profile fields and the raw info JSON |
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

Each ticker is processed in a single SQLite transaction; a failure for one
symbol does not stop the remaining symbols.

---

## 4. morningstar_etf_pipeline.py

Reads Morningstar ETF CSV exports (one or more files in a directory) and
upserts each fund into the `securities` table of an existing SQLite database.

### Run

```bash
# Using the default CSV directory
poetry run python morningstar_etf_pipeline.py \
  --sqlite-db /path/to/portfolio_management.sqlite3

# Specify a custom CSV directory
poetry run python morningstar_etf_pipeline.py \
  --sqlite-db /path/to/portfolio_management.sqlite3 \
  --csv-dir   /path/to/morningstar_etf
```

### CSV format

Each CSV file must contain these columns (as exported from Morningstar):

| Column | Description |
| --- | --- |
| `Fund Name` | Full fund name |
| `Ticker` | Morningstar ticker, e.g. `etfs:ARCX:AVLV` or `etfs:BATS:JPLD` |
| `Active/Passive` | `Active` or `Passive` |
| `Morningstar Category` | e.g. `Large Value`, `Intermediate Core Bond` |
| `Morningstar Rating` | Numeric 1–5 (empty if unrated) |
| `Annualized 5-Year Total Return %` | Numeric, may be empty |

The pipeline reads **all** `*.csv` files found in the directory, deduplicates
on `Ticker`, and processes them in a single pass.

### Ticker extraction

The last colon-separated segment of the Morningstar ticker becomes the
database ticker symbol:

| Morningstar ticker | Database ticker |
| --- | --- |
| `etfs:ARCX:VOOG` | `VOOG` |
| `etfs:BATS:JPLD` | `JPLD` |
| `etfs:XNAS:IEFA` | `IEFA` |

### Column derivation

| `securities` column | Source |
| --- | --- |
| `ticker` | Last segment of the Morningstar ticker |
| `name` | `Fund Name` |
| `asset_class` | Inferred from `Morningstar Category` — bond-related keywords → `BOND`, otherwise `EQUITY` |
| `currency_code` | Derived from the exchange segment: `ARCX`/`BATS`/`XNAS` → `USD`, `XLON` → `GBP`, `XETR`/`XAMS`/`XPAR`/`XMIL` → `EUR` |
| `asset_subclass` | Always `ETF` |
| `description` | `"Active \| Large Value \| Morningstar Rating: ★★★★★ \| Annualized 5Y Return: 12.51%"` |
| `source` | `morningstar_etf_pipeline` |
| `created_at` | ISO-8601 UTC timestamp — set on first insert, never overwritten |

### Upsert behaviour

- If the ticker does **not** exist: INSERT and set `created_at` to the current
  UTC timestamp.
- If the ticker **already exists**: UPDATE all columns except `created_at`.

The pipeline is idempotent — running it twice does not duplicate rows and does
not overwrite the original insertion timestamp.

### Schema migration

The pipeline automatically adds the `created_at` and `source` columns to the
`securities` table if they are not already present. No manual migration is
needed.

---

## Scheduled Runs

The workflow in `.github/workflows/daily_extraction.yml` runs:

- At 22:00 UTC every Monday through Friday
- On demand through GitHub's **Run workflow** action

The workflow uses Python 3.12, installs `requirements.txt`, and runs
`market_pipeline.py` with repository-relative data directories.

> [!NOTE]
> Files written to `./data/delta_lake` on a GitHub-hosted runner disappear when
> the job ends. Add an artifact-upload step or configure persistent cloud
> storage before relying on scheduled runs for durable data.

---

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
├── sqlite_market_pipeline.py
├── security_reference_pipeline.py
├── morningstar_etf_pipeline.py
├── pyproject.toml
├── requirements.txt
└── README.md
```

---

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

**No CSV files found (Morningstar pipeline)**

Ensure `--csv-dir` points to a directory that contains at least one `*.csv`
file exported from Morningstar.
