from __future__ import annotations

import argparse
import logging
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import dlt

from market_pipeline import (
    MERGE_DISPOSITION,
    PRIMARY_KEY,
    fetch_candles,
    load_config,
    load_reference_symbols,
    parse_iso_date,
    parse_symbol_list,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
LOGGER = logging.getLogger(__name__)
PIPELINE_NAME = "financepipe_sqlite_market_data"


@dataclass(frozen=True)
class Security:
    id: int
    ticker: str


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch Yahoo Finance candles, merge them into Delta tables, and "
            "upsert them into an existing SQLite finance database."
        )
    )
    parser.add_argument(
        "--sqlite-db",
        "--database",
        dest="sqlite_db",
        type=Path,
        required=True,
        help="Path to the SQLite database containing the securities table.",
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
    args.sqlite_db = args.sqlite_db.expanduser()
    args.fx_symbols = (
        parse_symbol_list(args.fx_symbols) if args.fx_symbols else None
    )

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


def connect_database(database_path: Path) -> sqlite3.Connection:
    """Open an existing SQLite database without accidentally creating one."""
    if not database_path.is_file():
        raise FileNotFoundError(f"SQLite database not found: {database_path}")

    connection = sqlite3.connect(database_path)
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def validate_database_schema(connection: sqlite3.Connection) -> None:
    required_columns = {
        "securities": {"id", "ticker", "asset_class"},
        "price_history": {
            "security_id",
            "symbol",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
        },
        "fx_rate_history": {
            "base_currency_code",
            "quote_currency_code",
            "symbol",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
        },
        "import_error_logs": {
            "id",
            "error_message",
            "timestamp",
            "pipeline_name",
        },
    }

    for table_name, expected_columns in required_columns.items():
        actual_columns = {
            row[1]
            for row in connection.execute(
                f"PRAGMA table_info({table_name})"
            ).fetchall()
        }
        missing_columns = expected_columns - actual_columns
        if missing_columns:
            raise ValueError(
                f"SQLite table '{table_name}' is missing columns: "
                + ", ".join(sorted(missing_columns))
            )


def load_securities(connection: sqlite3.Connection) -> list[Security]:
    """Load unique non-cash securities in deterministic ticker order."""
    rows = connection.execute(
        """
        SELECT id, TRIM(ticker)
        FROM securities
        WHERE UPPER(TRIM(asset_class)) <> 'CASH'
          AND TRIM(ticker) <> ''
        ORDER BY ticker
        """
    ).fetchall()
    return [Security(id=int(row[0]), ticker=str(row[1])) for row in rows]


def parse_fx_symbol(symbol: str) -> tuple[str, str] | None:
    """Return the ISO base/quote pair from a Yahoo symbol such as EURUSD=X."""
    pair = symbol.strip().upper()
    if pair.endswith("=X"):
        pair = pair[:-2]
    pair = pair.replace("/", "").replace("-", "")

    if len(pair) != 6 or not pair.isalpha():
        return None
    return pair[:3], pair[3:]


def fetch_records(
    symbols: Sequence[str],
    from_date: date | None,
    to_date: date | None,
    connection: sqlite3.Connection,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for symbol in symbols:
        try:
            symbol_records = fetch_candles(symbol, from_date, to_date)
        except Exception as exc:  # Keep one bad symbol from stopping the batch.
            message = f"Failed fetching symbol {symbol}: {exc}"
            LOGGER.exception(message)
            log_import_error(connection, message)
            continue

        if not symbol_records:
            message = (
                f"No Yahoo Finance data returned for symbol {symbol}; "
                "the symbol may be invalid or delisted."
            )
            LOGGER.error(message)
            log_import_error(connection, message)
            continue

        records.extend(symbol_records)

    return deduplicate_records(records)


def deduplicate_records(
    records: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Keep one candle per Delta primary key to prevent ambiguous merges."""
    unique_records: dict[tuple[str, str], dict[str, Any]] = {}
    for record in records:
        unique_records[(record["symbol"], record["date"])] = record

    duplicates_removed = len(records) - len(unique_records)
    if duplicates_removed:
        LOGGER.warning(
            "Removed %d duplicate candle rows before loading.",
            duplicates_removed,
        )
    return list(unique_records.values())


def log_import_error(
    connection: sqlite3.Connection,
    error_message: str,
    pipeline_name: str = PIPELINE_NAME,
) -> None:
    """Persist an error immediately so a later pipeline failure cannot erase it."""
    try:
        connection.execute(
            """
            INSERT INTO import_error_logs (
                error_message, timestamp, pipeline_name
            ) VALUES (?, ?, ?)
            """,
            (
                error_message,
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
                pipeline_name,
            ),
        )
        connection.commit()
    except sqlite3.Error:
        LOGGER.exception("Could not write to import_error_logs")


def load_delta_resource(
    pipeline: Any,
    resource: Any,
    resource_name: str,
    connection: sqlite3.Connection,
) -> bool:
    """Load one Delta resource without blocking SQLite or other resources."""
    try:
        load_info = pipeline.run(resource, table_format="delta")
        print(f"{resource_name} load summary:")
        print(load_info)
        return True
    except Exception as exc:
        message = f"Delta load failed for {resource_name}: {exc}"
        LOGGER.exception(message)
        log_import_error(connection, message)

        # A failed dlt package is retried before new work on the next run. The
        # source data is reproducible, so discard it and regenerate it cleanly.
        discard_pending_packages(pipeline, connection, resource_name)
        return False


def discard_pending_packages(
    pipeline: Any,
    connection: sqlite3.Connection,
    context: str,
) -> None:
    try:
        pipeline.drop_pending_packages()
    except Exception as exc:
        message = f"Could not discard pending dlt package ({context}): {exc}"
        LOGGER.exception(message)
        log_import_error(connection, message)


@dlt.resource(
    name="market_tickers",
    write_disposition=MERGE_DISPOSITION,
    primary_key=PRIMARY_KEY,
)
def market_tickers(records: Sequence[dict[str, Any]]):
    yield from records


@dlt.resource(
    name="currencies",
    write_disposition=MERGE_DISPOSITION,
    primary_key=PRIMARY_KEY,
)
def currencies(records: Sequence[dict[str, Any]]):
    yield from records


def upsert_price_history(
    connection: sqlite3.Connection,
    records: Sequence[dict[str, Any]],
    security_ids: dict[str, int],
) -> int:
    rows = [
        (
            security_ids[record["symbol"]],
            record["symbol"],
            record["date"],
            record["open"],
            record["high"],
            record["low"],
            record["close"],
            record["volume"],
        )
        for record in records
        if record["symbol"] in security_ids and record["close"] is not None
    ]
    connection.executemany(
        """
        INSERT INTO price_history (
            security_id, symbol, date, open, high, low, close, volume
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(security_id, date) DO UPDATE SET
            symbol = excluded.symbol,
            open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            volume = excluded.volume
        """,
        rows,
    )
    return len(rows)


def upsert_fx_rate_history(
    connection: sqlite3.Connection,
    records: Sequence[dict[str, Any]],
) -> int:
    rows: list[tuple[Any, ...]] = []
    invalid_symbols: set[str] = set()

    for record in records:
        currencies_pair = parse_fx_symbol(record["symbol"])
        if currencies_pair is None:
            invalid_symbols.add(record["symbol"])
            continue
        if record["close"] is None:
            continue

        base_currency, quote_currency = currencies_pair
        rows.append(
            (
                base_currency,
                quote_currency,
                record["symbol"],
                record["date"],
                record["open"],
                record["high"],
                record["low"],
                record["close"],
                record["volume"],
            )
        )

    for symbol in sorted(invalid_symbols):
        LOGGER.warning(
            "Cannot derive a 3-letter base/quote pair from FX symbol %s; "
            "its SQLite rows were skipped.",
            symbol,
        )

    connection.executemany(
        """
        INSERT INTO fx_rate_history (
            base_currency_code, quote_currency_code, symbol, date,
            open, high, low, close, volume
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(base_currency_code, quote_currency_code, date) DO UPDATE SET
            symbol = excluded.symbol,
            open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            volume = excluded.volume
        """,
        rows,
    )
    return len(rows)


def run(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    connection: sqlite3.Connection | None = None

    try:
        reference_data_dir, target_delta_dir = load_config()
        connection = connect_database(args.sqlite_db)
        with connection:
            validate_database_schema(connection)
            securities = load_securities(connection)
        fx_symbols = args.fx_symbols or load_reference_symbols(
            "reference_currencies.csv", reference_data_dir
        )
    except (ValueError, FileNotFoundError, sqlite3.Error) as exc:
        LOGGER.error("Configuration/database loading error: %s", exc)
        if connection is not None:
            connection.close()
        return 1

    try:
        market_records = fetch_records(
            [security.ticker for security in securities],
            args.from_date,
            args.to_date,
            connection,
        )
        fx_records = fetch_records(
            fx_symbols,
            args.from_date,
            args.to_date,
            connection,
        )

        with connection:
            price_count = upsert_price_history(
                connection,
                market_records,
                {security.ticker: security.id for security in securities},
            )
            fx_count = upsert_fx_rate_history(connection, fx_records)

        LOGGER.info(
            "SQLite upsert complete: %d price_history rows and "
            "%d fx_rate_history rows.",
            price_count,
            fx_count,
        )

        try:
            target_delta_dir.mkdir(parents=True, exist_ok=True)
            destination = dlt.destinations.filesystem(
                bucket_url=target_delta_dir.resolve().as_uri()
            )
            pipeline = dlt.pipeline(
                pipeline_name=PIPELINE_NAME,
                destination=destination,
                dataset_name="financepipe",
            )

            if pipeline.has_pending_data:
                message = (
                    "Discarding a pending dlt load package left by an earlier "
                    "failed run before loading fresh records."
                )
                LOGGER.warning(message)
                log_import_error(connection, message)
                discard_pending_packages(pipeline, connection, "startup")
        except Exception as exc:
            message = f"Could not initialize the Delta pipeline: {exc}"
            LOGGER.exception(message)
            log_import_error(connection, message)
            return 1

        if market_records:
            load_delta_resource(
                pipeline,
                market_tickers(market_records),
                "market_tickers",
                connection,
            )

        if fx_records:
            load_delta_resource(
                pipeline,
                currencies(fx_records),
                "currencies",
                connection,
            )
    except (sqlite3.Error, ValueError) as exc:
        LOGGER.error("Pipeline/database update failed: %s", exc)
        return 1
    finally:
        connection.close()

    if not securities and not fx_symbols:
        LOGGER.warning("No non-cash securities or FX symbols found.")

    return 0


if __name__ == "__main__":
    raise SystemExit(run())
