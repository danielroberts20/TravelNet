def parse_int(value: str | None) -> int | None:
    if value in ("", None):
        return None
    return int(value)

def parse_float(value: str | None) -> float | None:
    if value in ("", None):
        return None
    return float(value)

def parse_bool_yes_no(value: str) -> bool:
    return value.lower() == "yes"