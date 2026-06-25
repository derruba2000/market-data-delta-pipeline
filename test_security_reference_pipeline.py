from __future__ import annotations

import sqlite3

from security_reference_pipeline import ensure_schema, load_symbol_snapshot


def test_load_symbol_snapshot_replaces_normalized_child_rows() -> None:
    connection = sqlite3.connect(":memory:")
    ensure_schema(connection)
    first = _snapshot("MSFT", "2026-06-24T10:00:00+00:00", 500.0)
    second = _snapshot("MSFT", "2026-06-25T10:00:00+00:00", 510.0)
    second["calendar"] = []

    with connection:
        load_symbol_snapshot(connection, first)
        load_symbol_snapshot(connection, second)

    snapshot = connection.execute(
        """
        SELECT symbol, long_name, extracted_at
        FROM yahoo_security_snapshots
        """
    ).fetchone()
    target = connection.execute(
        """
        SELECT target_value
        FROM yahoo_analyst_targets
        WHERE symbol = 'MSFT' AND target_name = 'mean'
        """
    ).fetchone()
    calendar_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM yahoo_calendar_events
        WHERE symbol = 'MSFT'
        """
    ).fetchone()

    assert snapshot == ("MSFT", "Microsoft Corporation", "2026-06-25T10:00:00+00:00")
    assert target == (510.0,)
    assert calendar_count == (0,)


def _snapshot(symbol: str, extracted_at: str, target: float) -> dict:
    return {
        "symbol": symbol,
        "extracted_at": extracted_at,
        "snapshot": {
            "symbol": symbol,
            "quote_type": "EQUITY",
            "short_name": "Microsoft",
            "long_name": "Microsoft Corporation",
            "currency": "USD",
            "exchange": "NMS",
            "market": "us_market",
            "timezone": "America/New_York",
            "website": "https://www.microsoft.com",
            "industry": "Software - Infrastructure",
            "sector": "Technology",
            "category": None,
            "fund_family": None,
            "legal_type": None,
            "business_summary": "Software company",
            "raw_info_json": "{}",
            "extracted_at": extracted_at,
        },
        "info": [
            {
                "symbol": symbol,
                "attribute": "marketCap",
                "value_text": None,
                "value_number": 1.0,
                "value_boolean": None,
                "value_date": None,
                "extracted_at": extracted_at,
            }
        ],
        "calendar": [
            {
                "symbol": symbol,
                "event_name": "Earnings Date",
                "event_index": 0,
                "value_text": None,
                "value_number": None,
                "value_date": "2026-07-20",
                "extracted_at": extracted_at,
            }
        ],
        "analyst_targets": [
            {
                "symbol": symbol,
                "target_name": "mean",
                "target_value": target,
                "extracted_at": extracted_at,
            }
        ],
        "financial_facts": [],
        "options": [],
        "fund_profile": None,
        "fund_asset_allocation": [],
        "fund_holdings": [],
        "fund_sector_weightings": [],
        "fund_metrics": [],
        "fund_performance": [],
    }
