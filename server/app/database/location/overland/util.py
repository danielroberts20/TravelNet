from datetime import datetime, timezone


def _normalise_ts(ts: str) -> str:
    """
    Parse any ISO 8601 timestamp (with or without offset) and
    return a UTC string in the format SQLite sorts correctly:
    'YYYY-MM-DD HH:MM:SS'
    """
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            # Treat naive timestamps as UTC (shouldn't happen with Overland)
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        # Return as-is if parsing fails — don't silently drop the row
        return ts