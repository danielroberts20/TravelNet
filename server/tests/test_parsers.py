"""
test_parsers.py — Unit tests for models/parsers.py pure helpers.

Covers:
  - parse_int: None/empty → None, valid string → int, invalid raises
  - parse_float: None/empty → None, valid string → float, invalid raises
  - parse_bool_yes_no: case-insensitive yes/no; anything else → False (no raise)
  - parse_string: None/empty → None, valid string returned verbatim
  - parse_cellular_states: None/empty → None, valid JSON → list, malformed → None
"""

import json
import pytest

from models.parsers import (
    parse_int,
    parse_float,
    parse_bool_yes_no,
    parse_string,
    parse_cellular_states,
)


# ---------------------------------------------------------------------------
# parse_int
# ---------------------------------------------------------------------------

class TestParseInt:

    def test_none_returns_none(self):
        assert parse_int(None) is None

    def test_empty_string_returns_none(self):
        assert parse_int("") is None

    def test_valid_integer_string(self):
        assert parse_int("42") == 42

    def test_negative_integer_string(self):
        assert parse_int("-7") == -7

    def test_zero(self):
        assert parse_int("0") == 0

    def test_invalid_string_raises(self):
        with pytest.raises(ValueError):
            parse_int("abc")

    def test_float_string_raises(self):
        with pytest.raises(ValueError):
            parse_int("3.14")


# ---------------------------------------------------------------------------
# parse_float
# ---------------------------------------------------------------------------

class TestParseFloat:

    def test_none_returns_none(self):
        assert parse_float(None) is None

    def test_empty_string_returns_none(self):
        assert parse_float("") is None

    def test_valid_float_string(self):
        assert parse_float("3.14") == pytest.approx(3.14)

    def test_integer_string_returns_float(self):
        result = parse_float("10")
        assert result == pytest.approx(10.0)
        assert isinstance(result, float)

    def test_negative_float(self):
        assert parse_float("-0.5") == pytest.approx(-0.5)

    def test_invalid_string_raises(self):
        with pytest.raises(ValueError):
            parse_float("not-a-number")


# ---------------------------------------------------------------------------
# parse_bool_yes_no
# ---------------------------------------------------------------------------

class TestParseBoolYesNo:

    def test_yes_returns_true(self):
        assert parse_bool_yes_no("Yes") is True

    def test_yes_lowercase_returns_true(self):
        assert parse_bool_yes_no("yes") is True

    def test_yes_uppercase_returns_true(self):
        assert parse_bool_yes_no("YES") is True

    def test_no_returns_false(self):
        assert parse_bool_yes_no("No") is False

    def test_no_lowercase_returns_false(self):
        assert parse_bool_yes_no("no") is False

    def test_empty_string_returns_false(self):
        assert parse_bool_yes_no("") is False

    def test_arbitrary_string_returns_false(self):
        # Contract: anything that isn't "yes" (case-insensitive) → False, no exception
        assert parse_bool_yes_no("true") is False
        assert parse_bool_yes_no("1") is False
        assert parse_bool_yes_no("maybe") is False


# ---------------------------------------------------------------------------
# parse_string
# ---------------------------------------------------------------------------

class TestParseString:

    def test_none_returns_none(self):
        assert parse_string(None) is None

    def test_empty_string_returns_none(self):
        assert parse_string("") is None

    def test_valid_string_returned_verbatim(self):
        assert parse_string("hello") == "hello"

    def test_whitespace_string_returned_verbatim(self):
        # Only exact "" or None triggers None — whitespace is a valid string
        assert parse_string("  ") == "  "

    def test_numeric_string_returned_verbatim(self):
        assert parse_string("123") == "123"


# ---------------------------------------------------------------------------
# parse_cellular_states
# ---------------------------------------------------------------------------

class TestParseCellularStates:

    def test_none_returns_none(self):
        assert parse_cellular_states(None) is None

    def test_empty_string_returns_none(self):
        assert parse_cellular_states("") is None

    def test_valid_json_returns_list(self):
        data = json.dumps([
            {"provider_name": "EE", "radio": "LTE", "code": "234", "is_roaming": False}
        ])
        result = parse_cellular_states(data)
        assert result is not None
        assert len(result) == 1
        assert result[0].provider_name == "EE"
        assert result[0].radio == "LTE"
        assert result[0].code == "234"
        assert result[0].is_roaming is False

    def test_multiple_entries(self):
        data = json.dumps([
            {"provider_name": "EE", "radio": "LTE", "code": "234", "is_roaming": False},
            {"provider_name": "O2", "radio": "5G", "code": "235", "is_roaming": True},
        ])
        result = parse_cellular_states(data)
        assert result is not None
        assert len(result) == 2

    def test_malformed_json_returns_none(self, caplog):
        result = parse_cellular_states("{not valid json")
        assert result is None

    def test_missing_required_key_returns_none(self, caplog):
        # provider_name key missing — from_json will raise KeyError → caught → None
        data = json.dumps([{"radio": "LTE", "code": "234", "is_roaming": False}])
        result = parse_cellular_states(data)
        assert result is None
