import json

from database.connection import get_conn


def get_unique_metrics() -> list[str]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT metric FROM health_data ORDER BY metric"
        ).fetchall()
        return [row[0] for row in rows]

def get_metric_entries(metric: str) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT timestamp, value_json FROM health_data WHERE metric = ? ORDER BY timestamp",
            (metric,),
        ).fetchall()
        return [
            {"timestamp": row[0], **json.loads(row[1])}
            for row in rows
        ]
