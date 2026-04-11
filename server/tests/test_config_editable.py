"""
test_config_editable.py — Unit tests for config/editable.py helper functions.

Covers:
  - _infer_type: all supported primitive types
  - _infer_list_type: empty list, typed lists
  - _format_value: scalars and lists (short and long)
"""

from datetime import datetime

import pytest

from config.editable import _format_value, _infer_list_type, _infer_type


# ---------------------------------------------------------------------------
# _infer_type
# ---------------------------------------------------------------------------

class TestInferType:

    def test_bool(self):
        # bool must be checked before int (bool is a subclass of int)
        assert _infer_type(True) == "bool"
        assert _infer_type(False) == "bool"

    def test_int(self):
        assert _infer_type(42) == "int"
        assert _infer_type(0) == "int"
        assert _infer_type(-1) == "int"

    def test_float(self):
        assert _infer_type(3.14) == "float"
        assert _infer_type(0.0) == "float"

    def test_str(self):
        assert _infer_type("hello") == "str"
        assert _infer_type("") == "str"

    def test_list_delegates_to_infer_list_type(self):
        assert _infer_type([1, 2, 3]) == "list[int]"
        assert _infer_type(["a", "b"]) == "list[str]"

    def test_datetime(self):
        assert _infer_type(datetime(2026, 1, 1)) == "datetime"

    def test_dict(self):
        assert _infer_type({"key": "val"}) == "dict[str,str]"

    def test_unknown_type_falls_back_to_str(self):
        class Exotic:
            pass
        assert _infer_type(Exotic()) == "str"


# ---------------------------------------------------------------------------
# _infer_list_type
# ---------------------------------------------------------------------------

class TestInferListType:

    def test_empty_list_defaults_to_list_str(self):
        assert _infer_list_type([]) == "list[str]"

    def test_bool_list(self):
        assert _infer_list_type([True, False]) == "list[bool]"

    def test_int_list(self):
        assert _infer_list_type([1, 2, 3]) == "list[int]"

    def test_float_list(self):
        assert _infer_list_type([1.0, 2.5]) == "list[float]"

    def test_str_list(self):
        assert _infer_list_type(["a", "b", "c"]) == "list[str]"

    def test_mixed_list_uses_first_element_type(self):
        # Type is inferred from the first element only
        assert _infer_list_type([1, "two", 3.0]) == "list[int]"


# ---------------------------------------------------------------------------
# _format_value
# ---------------------------------------------------------------------------

class TestFormatValue:

    def test_scalar_int(self):
        assert _format_value(42) == "42"

    def test_scalar_str(self):
        assert _format_value("hello") == repr("hello")

    def test_short_list_shown_in_full(self):
        val = [1, 2, 3]
        result = _format_value(val)
        assert result == repr(val)

    def test_exactly_three_elements_shown_in_full(self):
        val = ["a", "b", "c"]
        result = _format_value(val)
        assert result == repr(val)

    def test_long_list_truncated(self):
        val = [1, 2, 3, 4, 5]
        result = _format_value(val)
        assert "…" in result
        assert "+2 more" in result
        # First 3 elements should appear
        assert "1" in result
        assert "2" in result
        assert "3" in result
        # Elements 4 and 5 should not appear as literals
        assert "4" not in result
        assert "5" not in result

    def test_four_element_list_truncation_count(self):
        val = [10, 20, 30, 40]
        result = _format_value(val)
        assert "+1 more" in result
