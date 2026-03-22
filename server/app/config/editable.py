from datetime import datetime
import importlib
import inspect
import json

_EDITABLE: dict = {}

def _infer_type(value) -> str:
    if isinstance(value, bool):     return "bool"
    if isinstance(value, int):      return "int"
    if isinstance(value, float):    return "float"
    if isinstance(value, str):      return "str"
    if isinstance(value, list):     return _infer_list_type(value)
    if isinstance(value, datetime): return "datetime"
    return "str"

def _infer_list_type(value: list) -> str:
    if not value:
        return "list[str]"  # default for empty list
    first = value[0]
    if isinstance(first, bool):  return "list[bool]"
    if isinstance(first, int):   return "list[int]"
    if isinstance(first, float): return "list[float]"
    if isinstance(first, str):   return "list[str]"
    return "list[str]"

def editable(key: str, description: str = "", group: str = "general"):
    frame = inspect.stack()[1]
    module_name = frame[0].f_globals["__name__"]
    def register(value):
        _EDITABLE[key] = {
            "value":       value,
            "default":     value,  # store original
            "description": description,
            "group":       group,
            "type":        _infer_type(value),
            "module":      module_name,
        }
        return value
    
    return register

def get_editable() -> dict:
    return _EDITABLE


def get_value(key: str, default=None):
    entry = _EDITABLE.get(key)
    return entry["value"] if entry else default

def load_overrides() -> None:
    import config.general
    import config.logging
    
    """Read config_overrides.json and patch module-level constants."""
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
    if isinstance(value, list):
        if len(value) <= 3:
            return repr(value)
        return f"[{', '.join(repr(x) for x in value[:3])}, … +{len(value)-3} more]"
    return repr(value)

def log_config_summary() -> None:
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