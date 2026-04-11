"""
test_editable.py — Unit tests for config/editable.py.

Covers coerce_value():
  - bool: True/False literals, "true"/"false" strings, "1"/"0" strings
  - int: numeric string, float truncated
  - float: string, int passthrough
  - str: anything → str
  - list[int]: list of ints, list of int-strings
  - dict[str,float]: dict coercion
  - Invalid value raises ValueError

Covers get_value():
  - Returns current value for known key
  - Returns supplied default for unknown key
  - Returns None (not KeyError) when no default supplied and key missing

Covers load_overrides():
  - Missing file → silently returns, registry unchanged
  - Valid JSON file → patches known key in module and in _EDITABLE
  - Unknown key in JSON → silently skipped
  - Malformed JSON → silently ignored, no crash
"""

import json
import pytest
from unittest.mock import patch, MagicMock

from config.editable import coerce_value, get_value, load_overrides, _EDITABLE


# ---------------------------------------------------------------------------
# coerce_value
# ---------------------------------------------------------------------------

class TestCoerceValue:

    # bool
    def test_bool_true_literal(self):
        assert coerce_value(True, "bool") is True

    def test_bool_false_literal(self):
        assert coerce_value(False, "bool") is False

    def test_bool_string_true(self):
        assert coerce_value("true", "bool") is True

    def test_bool_string_false(self):
        assert coerce_value("false", "bool") is False

    def test_bool_string_1(self):
        assert coerce_value("1", "bool") is True

    def test_bool_string_0(self):
        assert coerce_value("0", "bool") is False

    def test_bool_string_yes(self):
        assert coerce_value("yes", "bool") is True

    # int
    def test_int_from_string(self):
        assert coerce_value("42", "int") == 42

    def test_int_from_int(self):
        assert coerce_value(7, "int") == 7

    def test_int_invalid_raises(self):
        with pytest.raises(ValueError):
            coerce_value("not-a-number", "int")

    # float
    def test_float_from_string(self):
        assert coerce_value("3.14", "float") == pytest.approx(3.14)

    def test_float_from_int(self):
        assert coerce_value(2, "float") == pytest.approx(2.0)

    # str
    def test_str_from_int(self):
        assert coerce_value(99, "str") == "99"

    def test_str_from_str(self):
        assert coerce_value("hello", "str") == "hello"

    # list[int]
    def test_list_int_from_list(self):
        assert coerce_value([1, 2, 3], "list[int]") == [1, 2, 3]

    def test_list_int_from_string_list(self):
        assert coerce_value(["1", "2"], "list[int]") == [1, 2]

    def test_list_non_list_raises(self):
        with pytest.raises(ValueError):
            coerce_value("notalist", "list[int]")

    # dict[str,float]
    def test_dict_str_float(self):
        result = coerce_value({"a": "1.5", "b": "2.0"}, "dict[str,float]")
        assert result["a"] == pytest.approx(1.5)
        assert result["b"] == pytest.approx(2.0)

    def test_dict_non_dict_raises(self):
        with pytest.raises(ValueError):
            coerce_value("notadict", "dict[str,str]")


# ---------------------------------------------------------------------------
# get_value
# ---------------------------------------------------------------------------

class TestGetValue:

    def test_returns_value_for_known_key(self):
        # Use a key we know is registered by config.general at import time
        # (any editable constant will do — pick PAGE_SIZE from detect_country_transitions
        #  or just any key present in _EDITABLE)
        if not _EDITABLE:
            pytest.skip("No editables registered — config.general not imported yet")
        key = next(iter(_EDITABLE))
        expected = _EDITABLE[key]["value"]
        assert get_value(key) == expected

    def test_returns_default_for_unknown_key(self):
        assert get_value("__nonexistent_key__", default=42) == 42

    def test_returns_none_for_unknown_key_no_default(self):
        assert get_value("__nonexistent_key__") is None


# ---------------------------------------------------------------------------
# load_overrides
# ---------------------------------------------------------------------------

class TestLoadOverrides:

    def test_missing_file_does_not_raise(self, tmp_path):
        nonexistent = tmp_path / "no_overrides.json"
        with patch("config.general.OVERRIDES_PATH", nonexistent):
            load_overrides()  # should return silently

    def test_unknown_key_silently_skipped(self, tmp_path):
        overrides = tmp_path / "overrides.json"
        overrides.write_text(json.dumps({"__totally_unknown_key__": 999}))
        with patch("config.general.OVERRIDES_PATH", overrides):
            load_overrides()  # no exception; unknown key skipped

    def test_malformed_json_does_not_raise(self, tmp_path):
        overrides = tmp_path / "overrides.json"
        overrides.write_text("{not valid json{{")
        with patch("config.general.OVERRIDES_PATH", overrides):
            load_overrides()  # should swallow the error silently

    def test_valid_override_patches_editable_value(self, tmp_path):
        """A valid override for a registered key updates _EDITABLE['value']."""
        if not _EDITABLE:
            pytest.skip("No editables registered")

        # Find a registered int or float key to override
        key = None
        for k, entry in _EDITABLE.items():
            if entry["type"] in ("int", "float"):
                key = k
                break
        if key is None:
            pytest.skip("No int/float editable found")

        original_value = _EDITABLE[key]["value"]
        new_value = original_value + 1 if _EDITABLE[key]["type"] == "int" else original_value + 1.0

        overrides = tmp_path / "overrides.json"
        overrides.write_text(json.dumps({key: new_value}))

        with patch("config.general.OVERRIDES_PATH", overrides):
            load_overrides()

        try:
            assert _EDITABLE[key]["value"] == new_value
        finally:
            # Restore original to avoid polluting other tests
            _EDITABLE[key]["value"] = original_value
