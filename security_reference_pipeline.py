from __future__ import annotations

import argparse
import json
import logging
import math
import sqlite3
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf
from yahooquery import Ticker as YahooQueryTicker

from market_pipeline import load_config, load_reference_symbols, parse_symbol_list
from sqlite_market_pipeline import connect_database, log_import_error


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
LOGGER = logging.getLogger(__name__)
PIPELINE_NAME = "financepipe_security_reference"

FINANCIAL_STATEMENTS = {
    "balance_sheet": ("balance_sheet", "annual"),
    "quarterly_balance_sheet": ("balance_sheet", "quarterly"),
    "income_stmt": ("income_statement", "annual"),
    "quarterly_income_stmt": ("income_statement", "quarterly"),
    "cash_flow": ("cash_flow", "annual"),
    "quarterly_cash_flow": ("cash_flow", "quarterly"),
}

SNAPSHOT_TABLES = (
    "yahoo_security_info",
    "yahoo_calendar_events",
    "yahoo_analyst_targets",
    "yahoo_financial_facts",
    "yahoo_option_contracts",
    "yahoo_fund_asset_allocation",
    "yahoo_fund_holdings",
    "yahoo_fund_sector_weightings",
    "yahoo_fund_metrics",
    "yahoo_fund_performance",
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Extract normalized Yahoo Finance security and fund reference data "
            "into an existing SQLite database."
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
        "--symbols",
        nargs="+",
        metavar="SYMBOL",
        help=(
            "Symbols to extract instead of reference_tickers.csv. Accepts "
            "space- or comma-separated values."
        ),
    )
    parser.add_argument(
        "--options-expirations",
        type=int,
        default=1,
        help=(
            "Number of nearest option expirations to load per symbol. "
            "Use 0 to skip options (default: 1)."
        ),
    )
    args = parser.parse_args(argv)
    args.sqlite_db = args.sqlite_db.expanduser()
    args.symbols = parse_symbol_list(args.symbols) if args.symbols else None
    if args.symbols == []:
        parser.error("--symbols must contain at least one non-empty symbol.")
    if args.options_expirations < 0:
        parser.error("--options-expirations cannot be negative.")
    return args


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS yahoo_security_snapshots (
            symbol TEXT PRIMARY KEY,
            quote_type TEXT,
            short_name TEXT,
            long_name TEXT,
            currency TEXT,
            exchange TEXT,
            market TEXT,
            timezone TEXT,
            website TEXT,
            industry TEXT,
            sector TEXT,
            category TEXT,
            fund_family TEXT,
            legal_type TEXT,
            business_summary TEXT,
            raw_info_json TEXT,
            extracted_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS yahoo_security_info (
            symbol TEXT NOT NULL,
            attribute TEXT NOT NULL,
            value_text TEXT,
            value_number REAL,
            value_boolean INTEGER,
            value_date TEXT,
            extracted_at TEXT NOT NULL,
            PRIMARY KEY (symbol, attribute)
        );

        CREATE TABLE IF NOT EXISTS yahoo_calendar_events (
            symbol TEXT NOT NULL,
            event_name TEXT NOT NULL,
            event_index INTEGER NOT NULL,
            value_text TEXT,
            value_number REAL,
            value_date TEXT,
            extracted_at TEXT NOT NULL,
            PRIMARY KEY (symbol, event_name, event_index)
        );

        CREATE TABLE IF NOT EXISTS yahoo_analyst_targets (
            symbol TEXT NOT NULL,
            target_name TEXT NOT NULL,
            target_value REAL,
            extracted_at TEXT NOT NULL,
            PRIMARY KEY (symbol, target_name)
        );

        CREATE TABLE IF NOT EXISTS yahoo_financial_facts (
            symbol TEXT NOT NULL,
            statement_type TEXT NOT NULL,
            frequency TEXT NOT NULL,
            period_end TEXT NOT NULL,
            metric TEXT NOT NULL,
            value REAL,
            extracted_at TEXT NOT NULL,
            PRIMARY KEY (
                symbol, statement_type, frequency, period_end, metric
            )
        );

        CREATE TABLE IF NOT EXISTS yahoo_option_contracts (
            symbol TEXT NOT NULL,
            expiration_date TEXT NOT NULL,
            option_type TEXT NOT NULL,
            contract_symbol TEXT NOT NULL,
            last_trade_date TEXT,
            strike REAL,
            last_price REAL,
            bid REAL,
            ask REAL,
            price_change REAL,
            percent_change REAL,
            volume INTEGER,
            open_interest INTEGER,
            implied_volatility REAL,
            in_the_money INTEGER,
            contract_size TEXT,
            currency TEXT,
            extracted_at TEXT NOT NULL,
            PRIMARY KEY (symbol, contract_symbol)
        );

        CREATE TABLE IF NOT EXISTS yahoo_fund_profiles (
            symbol TEXT PRIMARY KEY,
            family TEXT,
            category_name TEXT,
            legal_type TEXT,
            description TEXT,
            manager_name TEXT,
            manager_bio TEXT,
            annual_expense_ratio REAL,
            annual_holdings_turnover REAL,
            total_net_assets REAL,
            extracted_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS yahoo_fund_asset_allocation (
            symbol TEXT NOT NULL,
            asset_class TEXT NOT NULL,
            weight REAL,
            extracted_at TEXT NOT NULL,
            PRIMARY KEY (symbol, asset_class)
        );

        CREATE TABLE IF NOT EXISTS yahoo_fund_holdings (
            symbol TEXT NOT NULL,
            holding_rank INTEGER NOT NULL,
            holding_symbol TEXT,
            holding_name TEXT,
            weight REAL,
            extracted_at TEXT NOT NULL,
            PRIMARY KEY (symbol, holding_rank)
        );

        CREATE TABLE IF NOT EXISTS yahoo_fund_sector_weightings (
            symbol TEXT NOT NULL,
            sector TEXT NOT NULL,
            weight REAL,
            extracted_at TEXT NOT NULL,
            PRIMARY KEY (symbol, sector)
        );

        CREATE TABLE IF NOT EXISTS yahoo_fund_metrics (
            symbol TEXT NOT NULL,
            metric_group TEXT NOT NULL,
            metric TEXT NOT NULL,
            value_text TEXT,
            value_number REAL,
            extracted_at TEXT NOT NULL,
            PRIMARY KEY (symbol, metric_group, metric)
        );

        CREATE TABLE IF NOT EXISTS yahoo_fund_performance (
            symbol TEXT NOT NULL,
            performance_type TEXT NOT NULL,
            period TEXT NOT NULL,
            as_of_date TEXT NOT NULL DEFAULT '',
            value REAL,
            category_value REAL,
            extracted_at TEXT NOT NULL,
            PRIMARY KEY (symbol, performance_type, period, as_of_date)
        );

        CREATE INDEX IF NOT EXISTS ix_yahoo_financial_facts_symbol
            ON yahoo_financial_facts (symbol);
        CREATE INDEX IF NOT EXISTS ix_yahoo_option_contracts_symbol_expiration
            ON yahoo_option_contracts (symbol, expiration_date);
        CREATE INDEX IF NOT EXISTS ix_yahoo_fund_holdings_symbol
            ON yahoo_fund_holdings (symbol);
        """
    )


def extract_symbol(
    symbol: str,
    options_expirations: int,
    yfinance_factory: Any = yf.Ticker,
    yahooquery_factory: Any = YahooQueryTicker,
) -> dict[str, Any]:
    extracted_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    ticker = yfinance_factory(symbol)

    info = _safe_mapping(lambda: ticker.info, symbol, "general info")
    calendar = _safe_mapping(lambda: ticker.calendar, symbol, "calendar")
    analyst_targets = _safe_mapping(
        lambda: ticker.analyst_price_targets,
        symbol,
        "analyst targets",
    )

    financial_facts: list[dict[str, Any]] = []
    for attribute, (statement_type, frequency) in FINANCIAL_STATEMENTS.items():
        statement = _safe_frame(
            lambda attribute=attribute: getattr(ticker, attribute),
            symbol,
            f"{frequency} {statement_type}",
        )
        financial_facts.extend(
            _financial_fact_rows(
                symbol,
                statement,
                statement_type,
                frequency,
                extracted_at,
            )
        )

    options = _option_rows(
        symbol,
        ticker,
        options_expirations,
        extracted_at,
    )
    yfinance_fund = _extract_yfinance_fund_data(symbol, ticker)
    yahooquery_fund = _extract_yahooquery_fund_data(
        symbol,
        yahooquery_factory,
    )
    if not any(
        (
            info,
            calendar,
            analyst_targets,
            financial_facts,
            options,
            yfinance_fund,
            yahooquery_fund,
        )
    ):
        raise ValueError("Yahoo Finance returned no usable reference data.")

    return {
        "symbol": symbol,
        "extracted_at": extracted_at,
        "snapshot": _snapshot_row(symbol, info, yfinance_fund, extracted_at),
        "info": _mapping_rows(symbol, info, extracted_at),
        "calendar": _calendar_rows(symbol, calendar, extracted_at),
        "analyst_targets": _analyst_target_rows(
            symbol,
            analyst_targets,
            extracted_at,
        ),
        "financial_facts": financial_facts,
        "options": options,
        "fund_profile": _fund_profile_row(
            symbol,
            yfinance_fund,
            yahooquery_fund,
            extracted_at,
        ),
        "fund_asset_allocation": _fund_asset_allocation_rows(
            symbol,
            yfinance_fund,
            yahooquery_fund,
            extracted_at,
        ),
        "fund_holdings": _fund_holding_rows(
            symbol,
            yfinance_fund,
            yahooquery_fund,
            extracted_at,
        ),
        "fund_sector_weightings": _fund_sector_rows(
            symbol,
            yfinance_fund,
            yahooquery_fund,
            extracted_at,
        ),
        "fund_metrics": _fund_metric_rows(
            symbol,
            yfinance_fund,
            yahooquery_fund,
            extracted_at,
        ),
        "fund_performance": _fund_performance_rows(
            symbol,
            yahooquery_fund,
            extracted_at,
        ),
    }


def load_symbol_snapshot(
    connection: sqlite3.Connection,
    extracted: Mapping[str, Any],
) -> dict[str, int]:
    symbol = str(extracted["symbol"])
    for table in SNAPSHOT_TABLES:
        connection.execute(f"DELETE FROM {table} WHERE symbol = ?", (symbol,))
    connection.execute(
        "DELETE FROM yahoo_fund_profiles WHERE symbol = ?",
        (symbol,),
    )

    _upsert_snapshot(connection, extracted["snapshot"])
    counts = {
        "security_info": _insert_rows(
            connection,
            "yahoo_security_info",
            extracted["info"],
        ),
        "calendar_events": _insert_rows(
            connection,
            "yahoo_calendar_events",
            extracted["calendar"],
        ),
        "analyst_targets": _insert_rows(
            connection,
            "yahoo_analyst_targets",
            extracted["analyst_targets"],
        ),
        "financial_facts": _insert_rows(
            connection,
            "yahoo_financial_facts",
            extracted["financial_facts"],
        ),
        "option_contracts": _insert_rows(
            connection,
            "yahoo_option_contracts",
            extracted["options"],
        ),
        "fund_asset_allocation": _insert_rows(
            connection,
            "yahoo_fund_asset_allocation",
            extracted["fund_asset_allocation"],
        ),
        "fund_holdings": _insert_rows(
            connection,
            "yahoo_fund_holdings",
            extracted["fund_holdings"],
        ),
        "fund_sector_weightings": _insert_rows(
            connection,
            "yahoo_fund_sector_weightings",
            extracted["fund_sector_weightings"],
        ),
        "fund_metrics": _insert_rows(
            connection,
            "yahoo_fund_metrics",
            extracted["fund_metrics"],
        ),
        "fund_performance": _insert_rows(
            connection,
            "yahoo_fund_performance",
            extracted["fund_performance"],
        ),
    }
    if extracted["fund_profile"] is not None:
        _insert_rows(
            connection,
            "yahoo_fund_profiles",
            [extracted["fund_profile"]],
        )
        counts["fund_profiles"] = 1
    else:
        counts["fund_profiles"] = 0
    return counts


def _upsert_snapshot(
    connection: sqlite3.Connection,
    row: Mapping[str, Any],
) -> None:
    columns = list(row)
    placeholders = ", ".join("?" for _ in columns)
    updates = ", ".join(
        f"{column} = excluded.{column}"
        for column in columns
        if column != "symbol"
    )
    connection.execute(
        f"""
        INSERT INTO yahoo_security_snapshots ({", ".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT(symbol) DO UPDATE SET {updates}
        """,
        tuple(row[column] for column in columns),
    )


def _insert_rows(
    connection: sqlite3.Connection,
    table: str,
    rows: Sequence[Mapping[str, Any]],
) -> int:
    if not rows:
        return 0
    columns = list(rows[0])
    placeholders = ", ".join("?" for _ in columns)
    connection.executemany(
        f"""
        INSERT INTO {table} ({", ".join(columns)})
        VALUES ({placeholders})
        """,
        [tuple(row[column] for column in columns) for row in rows],
    )
    return len(rows)


def _safe_mapping(
    getter: Any,
    symbol: str,
    label: str,
) -> dict[str, Any]:
    try:
        value = getter()
    except Exception as exc:
        LOGGER.warning("%s unavailable for %s: %s", label, symbol, exc)
        return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _safe_frame(
    getter: Any,
    symbol: str,
    label: str,
) -> pd.DataFrame:
    try:
        value = getter()
    except Exception as exc:
        LOGGER.warning("%s unavailable for %s: %s", label, symbol, exc)
        return pd.DataFrame()
    return value if isinstance(value, pd.DataFrame) else pd.DataFrame()


def _snapshot_row(
    symbol: str,
    info: Mapping[str, Any],
    fund: Mapping[str, Any],
    extracted_at: str,
) -> dict[str, Any]:
    overview = fund.get("fund_overview", {})
    return {
        "symbol": symbol,
        "quote_type": _text(info.get("quoteType")),
        "short_name": _text(info.get("shortName")),
        "long_name": _text(info.get("longName")),
        "currency": _text(info.get("currency")),
        "exchange": _text(info.get("exchange")),
        "market": _text(info.get("market")),
        "timezone": _text(info.get("exchangeTimezoneName")),
        "website": _text(info.get("website")),
        "industry": _text(info.get("industry")),
        "sector": _text(info.get("sector")),
        "category": _text(info.get("category") or overview.get("categoryName")),
        "fund_family": _text(info.get("fundFamily") or overview.get("family")),
        "legal_type": _text(info.get("legalType") or overview.get("legalType")),
        "business_summary": _text(
            info.get("longBusinessSummary") or fund.get("description")
        ),
        "raw_info_json": json.dumps(
            _json_safe(info),
            sort_keys=True,
            separators=(",", ":"),
        ),
        "extracted_at": extracted_at,
    }


def _mapping_rows(
    symbol: str,
    values: Mapping[str, Any],
    extracted_at: str,
) -> list[dict[str, Any]]:
    rows = []
    for attribute, value in sorted(values.items()):
        typed = _typed_value(value)
        rows.append(
            {
                "symbol": symbol,
                "attribute": str(attribute),
                **typed,
                "extracted_at": extracted_at,
            }
        )
    return rows


def _calendar_rows(
    symbol: str,
    calendar: Mapping[str, Any],
    extracted_at: str,
) -> list[dict[str, Any]]:
    rows = []
    for event_name, raw_value in sorted(calendar.items()):
        values = (
            list(raw_value)
            if isinstance(raw_value, (list, tuple, pd.Series))
            else [raw_value]
        )
        for index, value in enumerate(values):
            typed = _typed_value(value)
            rows.append(
                {
                    "symbol": symbol,
                    "event_name": str(event_name),
                    "event_index": index,
                    "value_text": typed["value_text"],
                    "value_number": typed["value_number"],
                    "value_date": typed["value_date"],
                    "extracted_at": extracted_at,
                }
            )
    return rows


def _analyst_target_rows(
    symbol: str,
    targets: Mapping[str, Any],
    extracted_at: str,
) -> list[dict[str, Any]]:
    return [
        {
            "symbol": symbol,
            "target_name": str(name),
            "target_value": _number(value),
            "extracted_at": extracted_at,
        }
        for name, value in sorted(targets.items())
        if _number(value) is not None
    ]


def _financial_fact_rows(
    symbol: str,
    frame: pd.DataFrame,
    statement_type: str,
    frequency: str,
    extracted_at: str,
) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    rows = []
    for metric, series in frame.iterrows():
        for period, value in series.items():
            numeric_value = _number(value)
            if numeric_value is None:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "statement_type": statement_type,
                    "frequency": frequency,
                    "period_end": _date_text(period) or str(period),
                    "metric": str(metric),
                    "value": numeric_value,
                    "extracted_at": extracted_at,
                }
            )
    return rows


def _option_rows(
    symbol: str,
    ticker: Any,
    expiration_limit: int,
    extracted_at: str,
) -> list[dict[str, Any]]:
    if expiration_limit == 0:
        return []
    try:
        expirations = list(ticker.options or [])[:expiration_limit]
    except Exception as exc:
        LOGGER.warning("Option expirations unavailable for %s: %s", symbol, exc)
        return []

    rows = []
    for expiration in expirations:
        try:
            chain = ticker.option_chain(expiration)
        except Exception as exc:
            LOGGER.warning(
                "Option chain %s unavailable for %s: %s",
                expiration,
                symbol,
                exc,
            )
            continue
        for option_type, frame in (("call", chain.calls), ("put", chain.puts)):
            if not isinstance(frame, pd.DataFrame) or frame.empty:
                continue
            for record in frame.to_dict("records"):
                rows.append(
                    {
                        "symbol": symbol,
                        "expiration_date": str(expiration),
                        "option_type": option_type,
                        "contract_symbol": _text(record.get("contractSymbol")) or "",
                        "last_trade_date": _date_text(record.get("lastTradeDate")),
                        "strike": _number(record.get("strike")),
                        "last_price": _number(record.get("lastPrice")),
                        "bid": _number(record.get("bid")),
                        "ask": _number(record.get("ask")),
                        "price_change": _number(record.get("change")),
                        "percent_change": _number(record.get("percentChange")),
                        "volume": _integer(record.get("volume")),
                        "open_interest": _integer(record.get("openInterest")),
                        "implied_volatility": _number(
                            record.get("impliedVolatility")
                        ),
                        "in_the_money": _boolean(record.get("inTheMoney")),
                        "contract_size": _text(record.get("contractSize")),
                        "currency": _text(record.get("currency")),
                        "extracted_at": extracted_at,
                    }
                )
    return [row for row in rows if row["contract_symbol"]]


def _extract_yfinance_fund_data(
    symbol: str,
    ticker: Any,
) -> dict[str, Any]:
    try:
        funds = ticker.funds_data
        return {
            "description": _safe_property(funds, "description"),
            "fund_overview": _mapping_or_empty(
                _safe_property(funds, "fund_overview")
            ),
            "fund_operations": _frame_or_empty(
                _safe_property(funds, "fund_operations")
            ),
            "asset_classes": _mapping_or_empty(
                _safe_property(funds, "asset_classes")
            ),
            "top_holdings": _frame_or_empty(
                _safe_property(funds, "top_holdings")
            ),
            "equity_holdings": _frame_or_empty(
                _safe_property(funds, "equity_holdings")
            ),
            "bond_holdings": _frame_or_empty(
                _safe_property(funds, "bond_holdings")
            ),
            "bond_ratings": _mapping_or_empty(
                _safe_property(funds, "bond_ratings")
            ),
            "sector_weightings": _mapping_or_empty(
                _safe_property(funds, "sector_weightings")
            ),
        }
    except Exception as exc:
        LOGGER.info("No yfinance fund data for %s: %s", symbol, exc)
        return {}


def _extract_yahooquery_fund_data(
    symbol: str,
    yahooquery_factory: Any,
) -> dict[str, Any]:
    try:
        ticker = yahooquery_factory(symbol)
        return {
            "profile": _symbol_module(ticker.fund_profile, symbol),
            "holding_info": _symbol_module(ticker.fund_holding_info, symbol),
            "sector_weightings": ticker.fund_sector_weightings,
            "performance": _symbol_module(ticker.fund_performance, symbol),
        }
    except Exception as exc:
        LOGGER.info("No yahooquery fund data for %s: %s", symbol, exc)
        return {}


def _fund_profile_row(
    symbol: str,
    yfinance_fund: Mapping[str, Any],
    yahooquery_fund: Mapping[str, Any],
    extracted_at: str,
) -> dict[str, Any] | None:
    yf_overview = _mapping_or_empty(yfinance_fund.get("fund_overview"))
    profile = _mapping_or_empty(yahooquery_fund.get("profile"))
    if not yf_overview and not profile and not yfinance_fund.get("description"):
        return None
    fees = _mapping_or_empty(profile.get("feesExpensesInvestment"))
    management = _mapping_or_empty(profile.get("managementInfo"))
    return {
        "symbol": symbol,
        "family": _text(profile.get("family") or yf_overview.get("family")),
        "category_name": _text(
            profile.get("categoryName") or yf_overview.get("categoryName")
        ),
        "legal_type": _text(
            profile.get("legalType") or yf_overview.get("legalType")
        ),
        "description": _text(yfinance_fund.get("description")),
        "manager_name": _text(management.get("managerName")),
        "manager_bio": _text(management.get("managerBio")),
        "annual_expense_ratio": _number(
            fees.get("annualReportExpenseRatio")
        ),
        "annual_holdings_turnover": _number(
            fees.get("annualHoldingsTurnover")
        ),
        "total_net_assets": _number(fees.get("totalNetAssets")),
        "extracted_at": extracted_at,
    }


def _fund_asset_allocation_rows(
    symbol: str,
    yfinance_fund: Mapping[str, Any],
    yahooquery_fund: Mapping[str, Any],
    extracted_at: str,
) -> list[dict[str, Any]]:
    allocation = dict(
        _mapping_or_empty(yahooquery_fund.get("holding_info"))
    )
    allocation.pop("holdings", None)
    allocation.pop("equityHoldings", None)
    allocation.pop("bondHoldings", None)
    allocation.pop("bondRatings", None)
    allocation.pop("maxAge", None)
    if not allocation:
        allocation = dict(
            _mapping_or_empty(yfinance_fund.get("asset_classes"))
        )
    return [
        {
            "symbol": symbol,
            "asset_class": str(name),
            "weight": numeric,
            "extracted_at": extracted_at,
        }
        for name, value in sorted(allocation.items())
        if (numeric := _number(value)) is not None
    ]


def _fund_holding_rows(
    symbol: str,
    yfinance_fund: Mapping[str, Any],
    yahooquery_fund: Mapping[str, Any],
    extracted_at: str,
) -> list[dict[str, Any]]:
    holding_info = _mapping_or_empty(yahooquery_fund.get("holding_info"))
    holdings = holding_info.get("holdings")
    records: list[Mapping[str, Any]] = (
        holdings if isinstance(holdings, list) else []
    )
    if not records:
        frame = _frame_or_empty(yfinance_fund.get("top_holdings"))
        if not frame.empty:
            records = [
                {
                    "symbol": index,
                    "holdingName": row.get("Name"),
                    "holdingPercent": row.get("Holding Percent"),
                }
                for index, row in frame.iterrows()
            ]
    return [
        {
            "symbol": symbol,
            "holding_rank": rank,
            "holding_symbol": _text(record.get("symbol")),
            "holding_name": _text(
                record.get("holdingName") or record.get("Name")
            ),
            "weight": _number(
                record.get("holdingPercent") or record.get("Holding Percent")
            ),
            "extracted_at": extracted_at,
        }
        for rank, record in enumerate(records, start=1)
    ]


def _fund_sector_rows(
    symbol: str,
    yfinance_fund: Mapping[str, Any],
    yahooquery_fund: Mapping[str, Any],
    extracted_at: str,
) -> list[dict[str, Any]]:
    sectors: dict[str, Any] = {}
    frame = yahooquery_fund.get("sector_weightings")
    if isinstance(frame, pd.DataFrame) and not frame.empty:
        column = symbol if symbol in frame.columns else frame.columns[0]
        sectors = {
            str(index): value
            for index, value in frame[column].items()
            if str(index).strip()
        }
    if not sectors:
        sectors = dict(
            _mapping_or_empty(yfinance_fund.get("sector_weightings"))
        )
    return [
        {
            "symbol": symbol,
            "sector": str(name),
            "weight": numeric,
            "extracted_at": extracted_at,
        }
        for name, value in sorted(sectors.items())
        if (numeric := _number(value)) is not None
    ]


def _fund_metric_rows(
    symbol: str,
    yfinance_fund: Mapping[str, Any],
    yahooquery_fund: Mapping[str, Any],
    extracted_at: str,
) -> list[dict[str, Any]]:
    holding_info = _mapping_or_empty(yahooquery_fund.get("holding_info"))
    groups: dict[str, Any] = {
        "equity_holdings": holding_info.get("equityHoldings"),
        "bond_holdings": holding_info.get("bondHoldings"),
        "bond_ratings": holding_info.get("bondRatings"),
        "fund_operations": yfinance_fund.get("fund_operations"),
    }
    if not groups["equity_holdings"]:
        groups["equity_holdings"] = yfinance_fund.get("equity_holdings")
    if not groups["bond_holdings"]:
        groups["bond_holdings"] = yfinance_fund.get("bond_holdings")
    if not groups["bond_ratings"]:
        groups["bond_ratings"] = yfinance_fund.get("bond_ratings")

    rows = []
    for group_name, group in groups.items():
        for metric, value in _flatten_values(group):
            typed = _typed_value(value)
            rows.append(
                {
                    "symbol": symbol,
                    "metric_group": group_name,
                    "metric": metric,
                    "value_text": typed["value_text"],
                    "value_number": typed["value_number"],
                    "extracted_at": extracted_at,
                }
            )
    return _deduplicate(rows, ("symbol", "metric_group", "metric"))


def _fund_performance_rows(
    symbol: str,
    yahooquery_fund: Mapping[str, Any],
    extracted_at: str,
) -> list[dict[str, Any]]:
    performance = _mapping_or_empty(yahooquery_fund.get("performance"))
    rows = []
    paired_sections = (
        ("performance_overview", "performanceOverview", "performanceOverviewCat"),
        ("trailing_return", "trailingReturns", "trailingReturnsCat"),
    )
    for performance_type, value_key, category_key in paired_sections:
        values = _mapping_or_empty(performance.get(value_key))
        category = _mapping_or_empty(performance.get(category_key))
        as_of_date = _date_text(values.get("asOfDate")) or ""
        for period, value in values.items():
            if period == "asOfDate":
                continue
            numeric = _number(value)
            if numeric is None:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "performance_type": performance_type,
                    "period": str(period),
                    "as_of_date": as_of_date,
                    "value": numeric,
                    "category_value": _number(category.get(period)),
                    "extracted_at": extracted_at,
                }
            )

    annual = _mapping_or_empty(performance.get("annualTotalReturns"))
    annual_values = annual.get("returns")
    annual_category = annual.get("returnsCat")
    category_by_year = {
        str(item.get("year")): _number(
            item.get("annualValue") or item.get("annualValueCat")
        )
        for item in annual_category
        if isinstance(item, Mapping)
    } if isinstance(annual_category, list) else {}
    if isinstance(annual_values, list):
        for item in annual_values:
            if not isinstance(item, Mapping):
                continue
            year = str(item.get("year") or "")
            numeric = _number(item.get("annualValue"))
            if not year or numeric is None:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "performance_type": "annual_return",
                    "period": year,
                    "as_of_date": "",
                    "value": numeric,
                    "category_value": category_by_year.get(year),
                    "extracted_at": extracted_at,
                }
            )
    return _deduplicate(
        rows,
        ("symbol", "performance_type", "period", "as_of_date"),
    )


def _symbol_module(value: Any, symbol: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    result = value.get(symbol)
    if result is None:
        result = value.get(symbol.upper())
    return dict(result) if isinstance(result, Mapping) else {}


def _safe_property(instance: Any, name: str) -> Any:
    try:
        return getattr(instance, name)
    except Exception:
        return None


def _mapping_or_empty(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _frame_or_empty(value: Any) -> pd.DataFrame:
    return value if isinstance(value, pd.DataFrame) else pd.DataFrame()


def _flatten_values(value: Any) -> list[tuple[str, Any]]:
    if isinstance(value, Mapping):
        rows = []
        for key, nested in value.items():
            if isinstance(nested, Mapping):
                rows.extend(
                    (f"{key}.{child_key}", child_value)
                    for child_key, child_value in nested.items()
                )
            else:
                rows.append((str(key), nested))
        return rows
    if isinstance(value, pd.DataFrame):
        rows = []
        for index, series in value.iterrows():
            for column, cell in series.items():
                rows.append((f"{index}.{column}", cell))
        return rows
    if isinstance(value, list):
        rows = []
        for index, item in enumerate(value):
            if isinstance(item, Mapping):
                rows.extend(
                    (f"{index}.{key}", nested)
                    for key, nested in item.items()
                )
        return rows
    return []


def _typed_value(value: Any) -> dict[str, Any]:
    if isinstance(value, bool):
        return {
            "value_text": None,
            "value_number": None,
            "value_boolean": int(value),
            "value_date": None,
        }
    date_value = _date_text(value)
    if date_value is not None and isinstance(
        value,
        (date, datetime, pd.Timestamp),
    ):
        return {
            "value_text": None,
            "value_number": None,
            "value_boolean": None,
            "value_date": date_value,
        }
    numeric = _number(value)
    if numeric is not None:
        return {
            "value_text": None,
            "value_number": numeric,
            "value_boolean": None,
            "value_date": None,
        }
    return {
        "value_text": _text(value),
        "value_number": None,
        "value_boolean": None,
        "value_date": None,
    }


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (date, datetime, pd.Timestamp)):
        return value.isoformat()
    if pd.isna(value) if not isinstance(value, (list, tuple, dict)) else False:
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except (ValueError, AttributeError):
            pass
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    if isinstance(value, (Mapping, list, tuple, set)):
        return json.dumps(_json_safe(value), sort_keys=True)
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return str(value)


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if math.isfinite(numeric) else None


def _integer(value: Any) -> int | None:
    numeric = _number(value)
    return int(numeric) if numeric is not None else None


def _boolean(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "yes", "1"}:
            return 1
        if normalized in {"false", "no", "0"}:
            return 0
    return int(bool(value))


def _date_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return None


def _deduplicate(
    rows: Sequence[dict[str, Any]],
    keys: Sequence[str],
) -> list[dict[str, Any]]:
    unique: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        unique[tuple(row[key] for key in keys)] = row
    return list(unique.values())


def run(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    connection: sqlite3.Connection | None = None
    try:
        reference_data_dir, _ = load_config()
        symbols = args.symbols or load_reference_symbols(
            "reference_tickers.csv",
            reference_data_dir,
        )
        connection = connect_database(args.sqlite_db)
        with connection:
            ensure_schema(connection)
    except (ValueError, FileNotFoundError, sqlite3.Error) as exc:
        LOGGER.error("Configuration/database loading error: %s", exc)
        if connection is not None:
            connection.close()
        return 1

    succeeded = 0
    failed = 0
    try:
        for symbol in symbols:
            LOGGER.info("Extracting normalized Yahoo data for %s", symbol)
            try:
                extracted = extract_symbol(
                    symbol,
                    options_expirations=args.options_expirations,
                )
                with connection:
                    counts = load_symbol_snapshot(connection, extracted)
                LOGGER.info(
                    "Loaded %s: %s",
                    symbol,
                    ", ".join(
                        f"{name}={count}"
                        for name, count in counts.items()
                        if count
                    )
                    or "profile only",
                )
                succeeded += 1
            except Exception as exc:
                failed += 1
                message = f"Security reference extraction failed for {symbol}: {exc}"
                LOGGER.exception(message)
                log_import_error(connection, message, PIPELINE_NAME)
    finally:
        connection.close()

    LOGGER.info(
        "Security reference pipeline complete: %d succeeded, %d failed.",
        succeeded,
        failed,
    )
    return 1 if failed and not succeeded else 0


if __name__ == "__main__":
    raise SystemExit(run())
