

To accommodate your requirement for a configurable storage directory, the architecture relies on two `.env` variables:

-- Use poetry and python 3.12
-- the source used for the pipleines should be yahoo finance API

The .env variable must have a target folder variable and the input folder variable


* `REFERENCE_DATA_DIR`: The path to the folder containing your source CSV files.
* `TARGET_DELTA_DIR`: The path to the local or network file system directory where `dlt` will initialize and append the Delta tables.

---

## Technical Specifications (Context for AI)

```ini
# Expected .env File Template
REFERENCE_DATA_DIR="./data/reference"
TARGET_DELTA_DIR="./data/delta_lake"

```

### Key Rules for AI Execution:

1. **Idempotency:** Pipelines must be runnable multiple times a day without corrupting data.
2. **Delta Constraints:** Ensure schemas match daily. Use `table_format="delta"` and `write_disposition="append"`.
3. **Data Isolation:** `market_tickers` and `currencies` must be loaded into distinct paths inside `TARGET_DELTA_DIR`.

---

## Epic 1: Project Environment and Configuration Framework

> **Description:** Establish the runtime environment, dependency management, and configuration loading via environment variables to allow flexible folder definition for both reference inputs and Delta outputs.

### Story 1.1: Configure Project Dependencies and Environment Management

* **User Story:** As a developer, I want a standardized `requirements.txt` and a `.env` loader so that the project initializes with all necessary data-lake libraries and variable configurations.
* **Acceptance Criteria:**
* [ ] Provide a `requirements.txt` containing: `dlt[deltalake]`, `yfinance`, `pandas`, `python-dotenv`.
* [ ] Implement a robust `.env` loading routine in `market_pipeline.py`.
* [ ] Throw an explicit, clear error message if `REFERENCE_DATA_DIR` or `TARGET_DELTA_DIR` are missing from the environment.



### Story 1.2: Build Dynamic File Path Resolver

* **User Story:** As the pipeline engine, I want to programmatically resolve the exact locations of the reference CSV files and target Delta tables using the system environment variables.
* **Acceptance Criteria:**
* [ ] Use Python's `os` or `pathlib` to join `REFERENCE_DATA_DIR` with `reference_tickers.csv` and `reference_currencies.csv`.
* [ ] Dynamically pass `TARGET_DELTA_DIR` to the `dlt.pipeline` `destination` configuration so that it writes locally to that specific folder hierarchy.



---

## Epic 2: CSV Reference Data Ingestion

> **Description:** Build the ingestion capabilities to dynamically parse reference symbol files before hitting the market data APIs.

### Story 2.1: Robust CSV Reader for Market Tickers & Currencies

* **User Story:** As an ETL worker, I want to safely ingest the `reference_tickers.csv` and `reference_currencies.csv` from the configured directory into clean Python lists.
* **Acceptance Criteria:**
* [ ] Implement a helper function `load_reference_symbols(file_name)`.
* [ ] Parse the `symbol` column from the CSV.
* [ ] Clean strings (strip whitespaces, drop empty rows, drop duplicates).
* [ ] Gracefully handle missing files by logging an error and exiting without crashing silently.



---

## Epic 3: Market Data Extraction & Delta Table Loading via dlt

> **Description:** Implement `dlt` sources and resources to fetch daily market candles from Yahoo Finance and incrementally append them to standalone Delta tables.

### Story 3.1: Implement Market Tickers Resource (Stocks/ETFs/Funds)

* **User Story:** As a data engineer, I want a `dlt` resource that loops through the stock/ETF symbols and fetches their latest single-day metrics.
* **Acceptance Criteria:**
* [ ] Create a resource decorated with `@dlt.resource(name="market_tickers", write_disposition="append")`.
* [ ] Call `yf.Ticker(symbol).history(period="1d")` for each ticker.
* [ ] Format the output row as a dictionary containing: `symbol`, `date`, `open`, `high`, `low`, `close`, and `volume`.
* [ ] Use `yield` to stream data points iteratively.



### Story 3.2: Implement Currency/Forex Resource

* **User Story:** As a data analyst, I want currency pairings extracted into an isolated pipeline resource so that Forex data is kept strictly separate from equities.
* **Acceptance Criteria:**
* [ ] Create a resource decorated with `@dlt.resource(name="currencies", write_disposition="append")`.
* [ ] Accept the ticker formats required by Yahoo Finance (e.g., `EURUSD=X`).
* [ ] Structure the payload dictionary identically to the market data schema for uniformity.



### Story 3.3: Execute dlt Delta Lake Native Pipeline Engine

* **User Story:** As a system coordinator, I want to instantiate the `dlt` pipeline pointing to the `.env`-defined filesystem and run both resources in Delta format.
* **Acceptance Criteria:**
* [ ] Configure `dlt.pipeline` using `destination='filesystem'` and pass the value of `TARGET_DELTA_DIR` as the bucket/folder URL.
* [ ] Invoke `pipeline.run(...)` ensuring `table_format="delta"` is explicitly declared.
* [ ] Verify that execution logs or stdout prints the pipeline execution summaries upon success.



---

## Epic 4: Continuous Integration & Scheduling via GitHub Actions

> **Description:** Automate the execution of the ingestion script on a daily schedule utilizing GitHub Actions workflows while mapping parameters safely.

### Story 4.1: Establish Daily Extraction GitHub Workflow

* **User Story:** As a system administrator, I want a GitHub Actions workflow that executes the script automatically every weekday.
* **Acceptance Criteria:**
* [ ] Create `.github/workflows/daily_extraction.yml`.
* [ ] Set a CRON schedule to execute once a day at market close (e.g., `0 22 * * 1-5`).
* [ ] Include `workflow_dispatch` to allow testing runs manually via the GitHub UI.
* [ ] Implement steps for Checking out code, setting up Python 3.10+, and running `pip install -r requirements.txt`.



### Story 4.2: Build Environment Variable Mapping for CI Run

* **User Story:** As a security coordinator, I want the GitHub Action runner to map folder variables natively so the script runs without modifications in the cloud environment.
* **Acceptance Criteria:**
* [ ] Within the workflow step running `market_pipeline.py`, define an `env` block.
* [ ] Map `REFERENCE_DATA_DIR` and `TARGET_DELTA_DIR` (For local/workspace testing inside GitHub runners, these can point to relative repository paths like `./data/reference` and `./data/delta_lake`).
* *Note for AI runner:* If a remote cloud bucket is used later, this configuration block is where the AI or developer will swap out variables for S3/GCS keys.