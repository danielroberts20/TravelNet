"""
test_ingest_util.py — Unit tests for database/transaction/ingest/util.py pure helpers.

Covers safe_float() (pure function only; DB helper deferred to Phase 2):
  - Valid float string → float
  - Integer string → float
  - Blank/whitespace-only string → None
  - Non-numeric string → None
  - None input → None (AttributeError on .strip() caught → None)
"""

import pytest

from database.transaction.ingest.util import safe_float


class TestSafeFloat:

    def test_valid_positive_float(self):
        assert safe_float("3.14") == pytest.approx(3.14)

    def test_valid_negative_float(self):
        assert safe_float("-2.5") == pytest.approx(-2.5)

    def test_integer_string_returns_float(self):
        result = safe_float("42")
        assert result == pytest.approx(42.0)
        assert isinstance(result, float)

    def test_zero(self):
        assert safe_float("0") == pytest.approx(0.0)

    def test_blank_string_returns_none(self):
        assert safe_float("") is None

    def test_whitespace_only_returns_none(self):
        assert safe_float("   ") is None

    def test_non_numeric_returns_none(self):
        assert safe_float("abc") is None

    def test_none_returns_none(self):
        # AttributeError on None.strip() is caught → None
        assert safe_float(None) is None

    def test_mixed_string_returns_none(self):
        assert safe_float("12.3abc") is None

    def test_scientific_notation(self):
        # float() handles "1e3" → 1000.0
        assert safe_float("1e3") == pytest.approx(1000.0)
