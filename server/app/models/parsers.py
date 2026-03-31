import json
import logging

logger = logging.getLogger(__name__)


def parse_int(value: str | None) -> int | None:
    """Parse a string to int, returning None for empty/None values."""
    if value in ("", None):
        return None
    return int(value)

def parse_float(value: str | None) -> float | None:
    """Parse a string to float, returning None for empty/None values."""
    if value in ("", None):
        return None
    return float(value)

def parse_bool_yes_no(value: str) -> bool:
    """Parse a 'Yes'/'No' string to bool."""
    return value.lower() == "yes"

def parse_string(value: str | None) -> str | None:
    """Return the string unchanged, or None for empty/None values."""
    if value in ("", None):
        return None
    return value

def parse_cellular_states(states: str | None):
    """Parse a JSON-encoded list of cellular state dicts into CellularState objects."""
    from models.telemetry import CellularState

    if states in ("", None):
        return None
    try:
        return [
            CellularState.from_json(**i)
            for i in json.loads(states)
        ]
    except Exception as e:
        logger.warning(f"Bad cellular data\t Cellular entry: {states}\tException: {e}")
        return None
