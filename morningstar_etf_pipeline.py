"""
Morningstar ETF reference pipeline.

Reads Morningstar ETF CSV exports and upserts each fund into the ``securities``
table of an existing SQLite database.  Two columns are added to the table if
they are not already present:

* ``created_at``  – ISO-8601 timestamp set only on the initial INSERT and never
                    overwritten by subsequent updates.
* ``source``      – name of the pipeline that inserted the row.

Usage
-----
    python morningstar_etf_pipeline.py \\
        --sqlite-db /path/to/portfolio_management.sqlite3 \\
        --csv-dir   /path/to/morningstar_etf

The pipeline is idempotent: running it twice does not duplicate rows.
"""

from __future__ import annotations

import argparse
import logging
import re
import sqlite3
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
LOGGER = logging.getLogger(__name__)

PIPELINE_NAME = "morningstar_etf_pipeline"

# ---------------------------------------------------------------------------
# Exchange → ISO 4217 currency code
# ---------------------------------------------------------------------------
_EXCHANGE_CURRENCY: dict[str, str] = {
    # US exchanges
    "ARCX": "USD",   # NYSE Arca
    "BATS": "USD",   # CBOE BZX
    "XNAS": "USD",   # NASDAQ
    "XNYS": "USD",   # NYSE
    "XASE": "USD",   # NYSE American
    # European / UK exchanges
    "XLON": "GBP",   # London Stock Exchange
    "XETR": "EUR",   # Xetra (Frankfurt)
    "XAMS": "EUR",   # Euronext Amsterdam
    "XPAR": "EUR",   # Euronext Paris
    "XMIL": "EUR",   # Borsa Italiana (Milan)
    "XBRU": "EUR",   # Euronext Brussels
    "XLIS": "EUR",   # Euronext Lisbon
    "XDUB": "EUR",   # Euronext Dublin
    "XSTU": "EUR",   # Börse Stuttgart
}

# ---------------------------------------------------------------------------
# Morningstar category → canonical asset_class (max 11 chars)
# ---------------------------------------------------------------------------
_BOND_KEYWORDS = re.compile(
    r"bond|fixed|inflation|treasury|muni|credit|income|yield|duration",
    re.IGNORECASE,
)


def _infer_asset_class(morningstar_category: str) -> str:
    """Return 'BOND' for fixed-income categories, otherwise 'EQUITY'."""
    if _BOND_KEYWORDS.search(morningstar_category or ""):
        return "BOND"
    return "EQUITY"


# ---------------------------------------------------------------------------
# Ticker / exchange extraction
# ---------------------------------------------------------------------------

def _parse_ms_ticker(raw: str) -> tuple[str, str]:
    """
    Parse a Morningstar ticker like ``etfs:ARCX:AVLV``.

    Returns ``(exchange, ticker)``, e.g. ``("ARCX", "AVLV")``.
    The ticker is always the last colon-separated segment.
    """
    parts = raw.strip().split(":")
    ticker = parts[-1]
    exchange = parts[-2] if len(parts) >= 2 else ""
    return exchange, ticker


def _infer_currency(exchange: str) -> str:
    return _EXCHANGE_CURRENCY.get(exchange.upper(), "USD")


# ---------------------------------------------------------------------------
# Description builder
# ---------------------------------------------------------------------------

def _build_description(
    active_passive: str,
    category: str,
    rating: str,
    five_year_return: str,
) -> str:
    parts: list[str] = []

    if active_passive:
        parts.append(active_passive.strip())

    if category:
        parts.append(category.strip())

    if rating:
        try:
            stars = int(float(rating))
            parts.append(f"Morningstar Rating: {'★' * stars}")
        except (ValueError, TypeError):
            parts.append(f"Morningstar Rating: {rating}")

    if five_year_return:
        try:
            pct = float(five_year_return)
            parts.append(f"Annualized 5Y Return: {pct:.2f}%")
        except (ValueError, TypeError):
            parts.append(f"Annualized 5Y Return: {five_year_return}")

    return " | ".join(parts)


# ---------------------------------------------------------------------------
# Schema migration
# ---------------------------------------------------------------------------

def _ensure_extra_columns(conn: sqlite3.Connection) -> None:
    """Add ``created_at`` and ``source`` columns if they are missing."""
    existing = {
        row[1]
        for row in conn.execute("PRAGMA table_info(securities)")
    }
    with conn:
        if "created_at" not in existing:
            conn.execute(
                "ALTER TABLE securities ADD COLUMN created_at TEXT"
            )
            LOGGER.info("Added column 'created_at' to securities table.")
        if "source" not in existing:
            conn.execute(
                "ALTER TABLE securities ADD COLUMN source TEXT"
            )
            LOGGER.info("Added column 'source' to securities table.")


# ---------------------------------------------------------------------------
# CSV loading
# ---------------------------------------------------------------------------

def load_all_csvs(csv_dir: Path) -> pd.DataFrame:
    """Read all ``*.csv`` files from *csv_dir* and return a combined DataFrame."""
    frames: list[pd.DataFrame] = []
    for path in sorted(csv_dir.glob("*.csv")):
        LOGGER.info("Reading %s", path.name)
        df = pd.read_csv(path, dtype=str)
        df.columns = [c.strip() for c in df.columns]
        frames.append(df)

    if not frames:
        raise FileNotFoundError(f"No CSV files found in {csv_dir}")

    combined = pd.concat(frames, ignore_index=True)

    # Deduplicate on the raw Morningstar ticker (keep first occurrence)
    if "Ticker" in combined.columns:
        combined = combined.drop_duplicates(subset=["Ticker"], keep="first")

    return combined


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def transform(df: pd.DataFrame) -> list[dict]:
    """Return a list of record dicts ready to upsert into ``securities``."""
    records: list[dict] = []
    for _, row in df.iterrows():
        raw_ticker: str = str(row.get("Ticker", "")).strip()
        if not raw_ticker:
            continue

        exchange, ticker = _parse_ms_ticker(raw_ticker)

        name = str(row.get("Fund Name", "")).strip()
        active_passive = str(row.get("Active/Passive", "")).strip()
        category = str(row.get("Morningstar Category", "")).strip()
        rating = str(row.get("Morningstar Rating", "")).strip()
        five_year_return = str(row.get("Annualized 5-Year Total Return %", "")).strip()

        # Normalise NaN / "nan" strings that pandas sometimes produces
        for var_name in ("name", "active_passive", "category", "rating", "five_year_return"):
            val = locals()[var_name]
            if val.lower() == "nan":
                if var_name == "name":
                    name = ""
                elif var_name == "active_passive":
                    active_passive = ""
                elif var_name == "category":
                    category = ""
                elif var_name == "rating":
                    rating = ""
                elif var_name == "five_year_return":
                    five_year_return = ""

        if not ticker:
            LOGGER.warning("Skipping row with empty ticker: %s", row.to_dict())
            continue
        if not name:
            LOGGER.warning("Skipping row with empty name for ticker %s", ticker)
            continue

        asset_class = _infer_asset_class(category)
        currency_code = _infer_currency(exchange)
        description = _build_description(active_passive, category, rating, five_year_return)

        records.append(
            {
                "ticker": ticker,
                "name": name,
                "asset_class": asset_class,
                "currency_code": currency_code,
                "description": description or None,
                "asset_subclass": "ETF",
                "source": PIPELINE_NAME,
            }
        )

    return records


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------

def upsert_securities(
    conn: sqlite3.Connection,
    records: list[dict],
    now: str,
) -> tuple[int, int]:
    """
    Upsert *records* into the ``securities`` table.

    * INSERT → sets ``created_at = now``
    * UPDATE (ticker already present) → updates all columns **except** ``created_at``

    Returns ``(inserted, updated)``.
    """
    sql = """
        INSERT INTO securities
            (ticker, name, asset_class, currency_code, description,
             asset_subclass, source, created_at)
        VALUES
            (:ticker, :name, :asset_class, :currency_code, :description,
             :asset_subclass, :source, :created_at)
        ON CONFLICT(ticker) DO UPDATE SET
            name          = excluded.name,
            asset_class   = excluded.asset_class,
            currency_code = excluded.currency_code,
            description   = excluded.description,
            asset_subclass = excluded.asset_subclass,
            source        = excluded.source
    """
    inserted = 0
    updated = 0

    with conn:
        for rec in records:
            ticker = rec["ticker"]
            exists = conn.execute(
                "SELECT 1 FROM securities WHERE ticker = ?", (ticker,)
            ).fetchone()

            payload = {**rec, "created_at": now}
            conn.execute(sql, payload)

            if exists:
                updated += 1
            else:
                inserted += 1

    return inserted, updated


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Load Morningstar ETF reference CSVs into the securities table "
            "of an existing SQLite database."
        )
    )
    parser.add_argument(
        "--sqlite-db",
        "--database",
        dest="sqlite_db",
        type=Path,
        required=True,
        help="Path to the target SQLite database.",
    )
    parser.add_argument(
        "--csv-dir",
        dest="csv_dir",
        type=Path,
        default=Path("/Users/joaoramo/Data/trading_experiment/morningstar_etf"),
        help=(
            "Directory containing the Morningstar ETF CSV exports "
            "(default: %(default)s)."
        ),
    )
    args = parser.parse_args(argv)
    args.sqlite_db = args.sqlite_db.expanduser()
    args.csv_dir = args.csv_dir.expanduser()
    return args


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)

    if not args.sqlite_db.exists():
        raise FileNotFoundError(f"Database not found: {args.sqlite_db}")
    if not args.csv_dir.is_dir():
        raise FileNotFoundError(f"CSV directory not found: {args.csv_dir}")

    conn = sqlite3.connect(args.sqlite_db)

    try:
        _ensure_extra_columns(conn)

        df = load_all_csvs(args.csv_dir)
        LOGGER.info("Loaded %d unique ETF rows from CSV files.", len(df))

        records = transform(df)
        LOGGER.info("Transformed %d valid records.", len(records))

        now = datetime.now(timezone.utc).isoformat()
        inserted, updated = upsert_securities(conn, records, now)

        LOGGER.info(
            "Upsert complete: %d inserted, %d updated (source='%s').",
            inserted,
            updated,
            PIPELINE_NAME,
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
