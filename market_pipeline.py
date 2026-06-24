from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import dlt
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
LOGGER = logging.getLogger(__name__)


def load_config() -> tuple[Path, Path]:
    """Load mandatory environment variables and convert them to paths."""
    load_dotenv()

    reference_data_dir_raw = os.getenv("REFERENCE_DATA_DIR")
    target_delta_dir_raw = os.getenv("TARGET_DELTA_DIR")

    missing_keys = [
        key
        for key, value in {
            "REFERENCE_DATA_DIR": reference_data_dir_raw,
            "TARGET_DELTA_DIR": target_delta_dir_raw,
        }.items()
        if value is None or not value.strip()
    ]
    if missing_keys:
        raise ValueError(
            "Missing required environment variables: "
            + ", ".join(missing_keys)
            + ". Define them in your .env file."
        )

    reference_data_dir = Path(reference_data_dir_raw).expanduser()
    target_delta_dir = Path(target_delta_dir_raw).expanduser()

    return reference_data_dir, target_delta_dir


def load_reference_symbols(file_name: str, reference_data_dir: Path) -> list[str]:
    """Load symbol values from a reference CSV and return a clean unique list."""
    file_path = reference_data_dir / file_name

    if not file_path.exists():
        LOGGER.error("Reference file not found: %s", file_path)
        raise FileNotFoundError(f"Reference file not found: {file_path}")

    try:
        frame = pd.read_csv(file_path)
    except Exception as exc:  # pragma: no cover - defensive branch
        LOGGER.error("Failed reading CSV %s: %s", file_path, exc)
        raise

    if "symbol" not in frame.columns:
        raise ValueError(f"Required column 'symbol' not found in {file_path}")

    cleaned_symbols = (
        frame["symbol"]
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .drop_duplicates()
        .tolist()
    )

    return cleaned_symbols


def fetch_latest_candle(symbol: str) -> dict[str, Any] | None:
    """Fetch the latest 1-day candle from Yahoo Finance for a symbol."""
    try:
        history = yf.Ticker(symbol).history(period="1d")
    except Exception as exc:  # pragma: no cover - network/runtime defensive branch
        LOGGER.error("Yahoo Finance request failed for %s: %s", symbol, exc)
        return None

    if history.empty:
        LOGGER.warning("No data returned for symbol: %s", symbol)
        return None

    latest_row = history.iloc[-1]
    candle_date = history.index[-1]

    return {
        "symbol": symbol,
        "date": candle_date.date().isoformat(),
        "open": None if pd.isna(latest_row.get("Open")) else float(latest_row["Open"]),
        "high": None if pd.isna(latest_row.get("High")) else float(latest_row["High"]),
        "low": None if pd.isna(latest_row.get("Low")) else float(latest_row["Low"]),
        "close": None if pd.isna(latest_row.get("Close")) else float(latest_row["Close"]),
        "volume": None if pd.isna(latest_row.get("Volume")) else int(latest_row["Volume"]),
    }


@dlt.resource(name="market_tickers", write_disposition="append")
def market_tickers(symbols: list[str]):
    for symbol in symbols:
        record = fetch_latest_candle(symbol)
        if record:
            yield record


@dlt.resource(name="currencies", write_disposition="append")
def currencies(symbols: list[str]):
    for symbol in symbols:
        record = fetch_latest_candle(symbol)
        if record:
            yield record


def run() -> int:
    try:
        reference_data_dir, target_delta_dir = load_config()
        market_symbols = load_reference_symbols("reference_tickers.csv", reference_data_dir)
        currency_symbols = load_reference_symbols("reference_currencies.csv", reference_data_dir)
    except (ValueError, FileNotFoundError) as exc:
        LOGGER.error("Configuration/reference loading error: %s", exc)
        return 1

    if not market_symbols and not currency_symbols:
        LOGGER.warning("No symbols found in reference files. Nothing to load.")
        return 0

    target_delta_dir.mkdir(parents=True, exist_ok=True)

    filesystem_destination = dlt.destinations.filesystem(
        bucket_url=target_delta_dir.resolve().as_uri()
    )
    pipeline = dlt.pipeline(
        pipeline_name="financepipe_market_data",
        destination=filesystem_destination,
        dataset_name="financepipe",
    )

    if market_symbols:
        market_info = pipeline.run(
            market_tickers(market_symbols),
            table_format="delta",
        )
        print("market_tickers load summary:")
        print(market_info)

    if currency_symbols:
        currency_info = pipeline.run(
            currencies(currency_symbols),
            table_format="delta",
        )
        print("currencies load summary:")
        print(currency_info)

    return 0


if __name__ == "__main__":
    raise SystemExit(run())
