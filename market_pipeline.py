from __future__ import annotations

import argparse
import logging
import os
import sqlite3
from collections.abc import Sequence
from datetime import date, timedelta
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


def parse_iso_date(value: str) -> date:
    """Parse a command-line date in ISO YYYY-MM-DD format."""
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}'. Expected YYYY-MM-DD."
        ) from exc


def parse_symbol_list(values: list[str]) -> list[str]:
    """Normalize space- or comma-separated symbols while preserving order."""
    symbols: list[str] = []
    seen: set[str] = set()

    for value in values:
        for item in value.split(","):
            symbol = item.strip()
            if symbol and symbol not in seen:
                symbols.append(symbol)
                seen.add(symbol)

    return symbols


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse and validate the optional extraction interval."""
    parser = argparse.ArgumentParser(
        description=(
            "Fetch Yahoo Finance daily candles and merge them into Delta tables."
        )
    )
    parser.add_argument(
        "--sqlite-db",
        "--database",
        dest="sqlite_db",
        type=Path,
        default=None,
        help=(
            "Path to an existing SQLite database. When provided, market symbols "
            "are read from the 'securities' table instead of reference_tickers.csv."
        ),
    )
    parser.add_argument(
        "--from-date",
        "--fromDate",
        dest="from_date",
        type=parse_iso_date,
        help="First date to extract, inclusive (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--to-date",
        "--toDate",
        dest="to_date",
        type=parse_iso_date,
        help="Last date to extract, inclusive (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        metavar="SYMBOL",
        help=(
            "Market symbols to extract instead of reference_tickers.csv. "
            "Accepts space- or comma-separated values."
        ),
    )
    parser.add_argument(
        "--fx-symbols",
        "--fxSymbols",
        dest="fx_symbols",
        nargs="+",
        metavar="FX_SYMBOL",
        help=(
            "FX symbols to extract instead of reference_currencies.csv. "
            "Accepts space- or comma-separated values."
        ),
    )

    args = parser.parse_args(argv)
    if args.sqlite_db is not None:
        args.sqlite_db = args.sqlite_db.expanduser()
    args.symbols = parse_symbol_list(args.symbols) if args.symbols else None
    args.fx_symbols = (
        parse_symbol_list(args.fx_symbols) if args.fx_symbols else None
    )
    if args.symbols == []:
        parser.error("--symbols must contain at least one non-empty symbol.")
    if args.fx_symbols == []:
        parser.error("--fx-symbols must contain at least one non-empty symbol.")

    if (args.from_date is None) != (args.to_date is None):
        parser.error("--from-date and --to-date must be provided together.")

    if (
        args.from_date is not None
        and args.to_date is not None
        and args.from_date > args.to_date
    ):
        parser.error("--from-date cannot be later than --to-date.")

    return args


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


def load_securities_from_db(database_path: Path) -> list[str]:
    """Load non-cash tickers from the securities table in deterministic order."""
    if not database_path.is_file():
        raise FileNotFoundError(f"SQLite database not found: {database_path}")

    connection = sqlite3.connect(database_path)
    try:
        rows = connection.execute(
            """
            SELECT TRIM(ticker)
            FROM securities
            WHERE UPPER(TRIM(asset_class)) <> 'CASH'
              AND TRIM(ticker) <> ''
            ORDER BY ticker
            """
        ).fetchall()
    finally:
        connection.close()

    return [str(row[0]) for row in rows]


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


def fetch_candles(
    symbol: str,
    from_date: date | None = None,
    to_date: date | None = None,
) -> list[dict[str, Any]]:
    """Fetch daily candles for a symbol, defaulting to the latest day."""
    history_kwargs: dict[str, Any]
    if from_date is None or to_date is None:
        history_kwargs = {"period": "1d"}
    else:
        history_kwargs = {
            "start": from_date.isoformat(),
            # yfinance treats end as exclusive; the CLI interval is inclusive.
            "end": (to_date + timedelta(days=1)).isoformat(),
            "interval": "1d",
        }

    LOGGER.info("Fetching Yahoo Finance data for symbol: %s", symbol)

    try:
        history = yf.Ticker(symbol).history(**history_kwargs)
    except Exception as exc:  # pragma: no cover - network/runtime defensive branch
        LOGGER.error("Yahoo Finance request failed for %s: %s", symbol, exc)
        return []

    if history.empty:
        LOGGER.warning("No data returned for symbol: %s", symbol)
        return []

    records: list[dict[str, Any]] = []
    for candle_timestamp, candle in history.iterrows():
        records.append(
            {
                "symbol": symbol,
                "date": candle_timestamp.date().isoformat(),
                "open": None
                if pd.isna(candle.get("Open"))
                else float(candle["Open"]),
                "high": None
                if pd.isna(candle.get("High"))
                else float(candle["High"]),
                "low": None if pd.isna(candle.get("Low")) else float(candle["Low"]),
                "close": None
                if pd.isna(candle.get("Close"))
                else float(candle["Close"]),
                "volume": None
                if pd.isna(candle.get("Volume"))
                else int(candle["Volume"]),
            }
        )

    return records


MERGE_DISPOSITION = {"disposition": "merge", "strategy": "upsert"}
PRIMARY_KEY = ("symbol", "date")


@dlt.resource(
    name="market_tickers",
    write_disposition=MERGE_DISPOSITION,
    primary_key=PRIMARY_KEY,
)
def market_tickers(
    symbols: list[str],
    from_date: date | None = None,
    to_date: date | None = None,
):
    for symbol in symbols:
        yield from fetch_candles(symbol, from_date, to_date)


@dlt.resource(
    name="currencies",
    write_disposition=MERGE_DISPOSITION,
    primary_key=PRIMARY_KEY,
)
def currencies(
    symbols: list[str],
    from_date: date | None = None,
    to_date: date | None = None,
):
    for symbol in symbols:
        yield from fetch_candles(symbol, from_date, to_date)


def run(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        reference_data_dir, target_delta_dir = load_config()
        if args.sqlite_db is not None and args.symbols is None:
            market_symbols = load_securities_from_db(args.sqlite_db)
            LOGGER.info(
                "Loaded %d securities from database: %s",
                len(market_symbols),
                args.sqlite_db,
            )
        else:
            market_symbols = args.symbols or load_reference_symbols(
                "reference_tickers.csv", reference_data_dir
            )
        currency_symbols = args.fx_symbols or load_reference_symbols(
            "reference_currencies.csv", reference_data_dir
        )
    except (ValueError, FileNotFoundError, sqlite3.Error) as exc:
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
            market_tickers(market_symbols, args.from_date, args.to_date),
            table_format="delta",
        )
        print("market_tickers load summary:")
        print(market_info)

    if currency_symbols:
        currency_info = pipeline.run(
            currencies(currency_symbols, args.from_date, args.to_date),
            table_format="delta",
        )
        print("currencies load summary:")
        print(currency_info)

    return 0


if __name__ == "__main__":
    raise SystemExit(run())
