from datetime import datetime
import importlib
import inspect
import json

_EDITABLE: dict = {}

def _infer_type(value) -> str:
    """Return a type string for the given value, used to guide API coercion."""
    if isinstance(value, bool):     return "bool"
    if isinstance(value, int):      return "int"
    if isinstance(value, float):    return "float"
    if isinstance(value, str):      return "str"
    if isinstance(value, list):     return _infer_list_type(value)
    if isinstance(value, datetime): return "datetime"
    if isinstance(value, dict):     return "dict"
    return "str"

def _infer_list_type(value: list) -> str:
    """Return a typed list string (e.g. 'list[int]') based on the first element."""
    if not value:
        return "list[str]"  # default for empty list
    first = value[0]
    if isinstance(first, bool):  return "list[bool]"
    if isinstance(first, int):   return "list[int]"
    if isinstance(first, float): return "list[float]"
    if isinstance(first, str):   return "list[str]"
    return "list[str]"

def editable(key: str, description: str = "", group: str = "general"):
    """Decorator-factory that registers a config constant as runtime-editable.

    Usage::

        MY_SETTING = editable("MY_SETTING", "What this controls")(42)

    The decorated value is stored in _EDITABLE and can be overridden at
    runtime via the /metadata/config API (persisted to config_overrides.json).
    The constant in the originating module is patched in-place on load_overrides().
    """
    frame = inspect.stack()[1]
    module_name = frame[0].f_globals["__name__"]
    def register(value):
        _EDITABLE[key] = {
            "value":       value,
            "default":     value,  # store original for reset support
            "description": description,
            "group":       group,
            "type":        _infer_type(value),
            "module":      module_name,
        }
        return value

    return register

def get_editable() -> dict:
    """Return the full registry of editable config entries."""
    return _EDITABLE


def get_value(key: str, default=None):
    """Return the current (possibly overridden) value for key, or default."""
    entry = _EDITABLE.get(key)
    return entry["value"] if entry else default

def load_overrides() -> None:
    """Read config_overrides.json and patch module-level constants in-place.

    Called once at startup (after all modules have registered their editables).
    Silently skips unknown keys and logs any parse failures.
    """
    import config.general
    import config.logging

    if not config.general.OVERRIDES_PATH.exists():
        return
    try:
        with open(config.general.OVERRIDES_PATH) as f:
            overrides = json.load(f)
        for key, value in overrides.items():
            if key not in _EDITABLE:
                continue
            _EDITABLE[key]["value"] = value
            # Patch the constant in whatever module registered it
            module_name = _EDITABLE[key]["module"]
            module = importlib.import_module(module_name)
            setattr(module, key, value)
    except Exception as e:
        print(f"[config] Failed to load overrides: {e}")

def _format_value(value) -> str:
    """Return a concise repr for a config value, truncating long lists."""
    if isinstance(value, list):
        if len(value) <= 3:
            return repr(value)
        return f"[{', '.join(repr(x) for x in value[:3])}, … +{len(value)-3} more]"
    return repr(value)

def log_config_summary() -> None:
    """Emit an INFO log line for every registered editable, marking overrides."""
    import logging
    logger = logging.getLogger("config.editable")
    if not _EDITABLE:
        return
    logger.info("Config summary:")
    for key, entry in _EDITABLE.items():
        current = entry["value"]
        default = entry["default"]
        status  = "DEFAULT" if current == default else "OVERRIDDEN"
        logger.info(f"  {key} [{status}] = {_format_value(current)}")
    logger.info("Config summary complete.")