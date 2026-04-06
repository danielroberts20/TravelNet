from database.connection import get_conn


def get_unique_metrics() -> list[str]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT metric FROM health_quantity ORDER BY metric"
        ).fetchall()
        return [row[0] for row in rows]


def get_metric_entries(metric: str) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT timestamp, value, unit, source FROM health_quantity WHERE metric = ? ORDER BY timestamp",
            (metric,),
        ).fetchall()
        return [dict(row) for row in rows]
