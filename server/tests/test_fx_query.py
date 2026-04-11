"""
test_fx_query.py — Unit tests for database/exchange/fx.py DB helpers.

Covers:
  - get_gbp_rate: GBP passthrough, exact hit, tolerance fallback, no result
  - convert_to_gbp: GBP passthrough, division by rate, zero/None rate → None
  - insert_fx_json: correct pairs inserted, non-GBP source pairs skipped
"""

import sqlite3
import pytest
from datetime import date
from unittest.mock import patch

from database.exchange.fx import get_gbp_rate, convert_to_gbp, insert_fx_json


# ---------------------------------------------------------------------------
# Shared DB fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE fx_rates (
            id              INTEGER PRIMARY KEY,
            date            TEXT NOT NULL,
            source_currency TEXT NOT NULL,
            target_currency TEXT NOT NULL,
            rate            REAL NOT NULL,
            timestamp       TEXT NOT NULL,
            created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            UNIQUE(date, source_currency, target_currency)
        );
    """)
    return conn


@pytest.fixture
def patch_conn(db):
    with patch("database.exchange.fx.get_conn", return_value=db), \
         patch("database.exchange.table.get_conn", return_value=db):
        yield db


def _insert_rate(db, currency, on_date, rate, source="GBP"):
    db.execute(
        "INSERT INTO fx_rates (date, source_currency, target_currency, rate, timestamp) VALUES (?,?,?,?,?)",
        (on_date, source, currency, rate, "2024-01-01T00:00:00Z"),
    )
    db.commit()


# ---------------------------------------------------------------------------
# get_gbp_rate
# ---------------------------------------------------------------------------

class TestGetGbpRate:

    def test_gbp_returns_1_without_db_hit(self):
        # No DB fixture needed — GBP short-circuits before any query
        result = get_gbp_rate("GBP", date(2024, 6, 15))
        assert result == pytest.approx(1.0)

    def test_exact_date_match(self, patch_conn):
        _insert_rate(patch_conn, "USD", "2024-06-15", 1.25)
        result = get_gbp_rate("USD", date(2024, 6, 15))
        assert result == pytest.approx(1.25)

    def test_tolerance_fallback_earlier_date(self, patch_conn):
        # Rate available 3 days before target
        _insert_rate(patch_conn, "EUR", "2024-06-12", 1.17)
        result = get_gbp_rate("EUR", date(2024, 6, 15), tolerance_days=7)
        assert result == pytest.approx(1.17)

    def test_tolerance_fallback_later_date(self, patch_conn):
        # Rate available 2 days after target
        _insert_rate(patch_conn, "AUD", "2024-06-17", 1.90)
        result = get_gbp_rate("AUD", date(2024, 6, 15), tolerance_days=7)
        assert result == pytest.approx(1.90)

    def test_no_rate_within_tolerance_returns_none(self, patch_conn):
        # Rate exists but outside tolerance window
        _insert_rate(patch_conn, "JPY", "2024-01-01", 180.0)
        result = get_gbp_rate("JPY", date(2024, 6, 15), tolerance_days=3)
        assert result is None

    def test_no_rate_at_all_returns_none(self, patch_conn):
        result = get_gbp_rate("NZD", date(2024, 6, 15))
        assert result is None

    def test_exact_match_preferred_over_tolerance(self, patch_conn):
        # Both exact and nearby — exact should win (queried first)
        _insert_rate(patch_conn, "CAD", "2024-06-15", 1.70)
        _insert_rate(patch_conn, "CAD", "2024-06-14", 1.65)
        result = get_gbp_rate("CAD", date(2024, 6, 15))
        assert result == pytest.approx(1.70)


# ---------------------------------------------------------------------------
# convert_to_gbp
# ---------------------------------------------------------------------------

class TestConvertToGbp:

    def test_gbp_returns_rounded_amount(self):
        result = convert_to_gbp(10.123456789, "GBP", date(2024, 6, 15))
        assert result == pytest.approx(10.123457, rel=1e-5)

    def test_gbp_rounds_to_6dp(self):
        result = convert_to_gbp(1.0, "GBP", date(2024, 6, 15))
        assert result == pytest.approx(1.0)

    def test_converts_using_rate(self, patch_conn):
        _insert_rate(patch_conn, "USD", "2024-06-15", 1.25)
        result = convert_to_gbp(50.0, "USD", date(2024, 6, 15))
        assert result == pytest.approx(40.0, rel=1e-5)  # 50 / 1.25

    def test_no_rate_returns_none(self, patch_conn):
        result = convert_to_gbp(100.0, "NZD", date(2024, 6, 15))
        assert result is None

    def test_zero_rate_returns_none(self, patch_conn):
        # Protect against division by zero
        _insert_rate(patch_conn, "ZZZ", "2024-06-15", 0.0)
        result = convert_to_gbp(100.0, "ZZZ", date(2024, 6, 15))
        assert result is None

    def test_result_rounded_to_6dp(self, patch_conn):
        _insert_rate(patch_conn, "EUR", "2024-06-15", 1.17)
        result = convert_to_gbp(100.0, "EUR", date(2024, 6, 15))
        expected = round(100.0 / 1.17, 6)
        assert result == pytest.approx(expected, rel=1e-5)


# ---------------------------------------------------------------------------
# insert_fx_json
# ---------------------------------------------------------------------------

class TestInsertFxJson:

    def test_inserts_gbp_pair(self, patch_conn):
        quotes = {"2024-06-15": {"GBPUSD": 1.25, "GBPEUR": 1.17}}
        insert_fx_json(quotes)

        rows = patch_conn.execute(
            "SELECT target_currency, rate FROM fx_rates ORDER BY target_currency"
        ).fetchall()
        assert len(rows) == 2
        targets = {r["target_currency"]: r["rate"] for r in rows}
        assert targets["USD"] == pytest.approx(1.25)
        assert targets["EUR"] == pytest.approx(1.17)

    def test_skips_non_gbp_source(self, patch_conn, caplog):
        import logging
        quotes = {"2024-06-15": {"USDEUR": 0.93, "GBPUSD": 1.25}}
        with caplog.at_level(logging.WARNING):
            insert_fx_json(quotes)

        rows = patch_conn.execute("SELECT COUNT(*) FROM fx_rates").fetchone()[0]
        assert rows == 1  # only GBPUSD inserted

    def test_idempotent_on_duplicate(self, patch_conn):
        quotes = {"2024-06-15": {"GBPUSD": 1.25}}
        insert_fx_json(quotes)
        insert_fx_json(quotes)  # second call — INSERT OR IGNORE
        count = patch_conn.execute("SELECT COUNT(*) FROM fx_rates").fetchone()[0]
        assert count == 1

    def test_multiple_dates(self, patch_conn):
        quotes = {
            "2024-06-14": {"GBPUSD": 1.24},
            "2024-06-15": {"GBPUSD": 1.25},
        }
        insert_fx_json(quotes)
        count = patch_conn.execute("SELECT COUNT(*) FROM fx_rates").fetchone()[0]
        assert count == 2
